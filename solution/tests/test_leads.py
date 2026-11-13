"""Integration tests for transactional Assignment A lead ingestion."""

from __future__ import annotations

import csv
import hashlib
import pytest

from solution.au_pipeline.config import Settings
from solution.au_pipeline.db import database, initialize_database
from solution.au_pipeline.leads import LeadIngestionError, _ingest_lead_files
from solution.au_pipeline.schema import load_schemas
from solution.au_pipeline.security import decrypt_value


TEST_FERNET_KEY = b"MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
TEST_HMAC_SECRET = b"test-only-assignment-a-secret-32-bytes"


def _settings(tmp_path):
    return Settings(tmp_path / "leads.sqlite3", TEST_FERNET_KEY, TEST_HMAC_SECRET)


def _write_csv(path, headers, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        writer.writerows(rows)


def _read_quality(settings, drop_id):
    with database(settings.database_path) as connection:
        return dict(connection.execute(
            "SELECT * FROM drop_quality WHERE drop_id=?", (drop_id,)
        ).fetchone())


def _run_lead_stage(drop, settings):
    """Test harness for a stage invoked by an outer drop transaction."""
    initialize_database(settings.database_path)
    drop_id = drop.name
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT INTO ingested_drops (drop_id, source_path, content_sha256, status, ingested_at) "
            "VALUES (?, ?, ?, 'processing', 'test-time')",
            (drop_id, str(drop), hashlib.sha256(drop_id.encode()).hexdigest()),
        )
        connection.execute("INSERT INTO drop_quality (drop_id) VALUES (?)", (drop_id,))
        return _ingest_lead_files(connection, drop, drop_id, settings, load_schemas())


def _claim_for_test(settings, drop_id, source_path, *, status="processing", quality=True):
    """Create committed outer-pipeline metadata for guard tests."""
    initialize_database(settings.database_path)
    with database(settings.database_path) as connection:
        connection.execute(
            "INSERT INTO ingested_drops (drop_id, source_path, content_sha256, status, ingested_at) "
            "VALUES (?, ?, ?, ?, 'test-time')",
            (drop_id, str(source_path), hashlib.sha256(drop_id.encode()).hexdigest(), status),
        )
        if quality:
            connection.execute("INSERT INTO drop_quality (drop_id) VALUES (?)", (drop_id,))


def _assert_no_stage_writes(settings, drop_id, *, quality_exists=True):
    with database(settings.database_path) as connection:
        assert connection.execute("SELECT count(*) FROM leads").fetchone()[0] == 0
        quality = connection.execute(
            "SELECT * FROM drop_quality WHERE drop_id=?", (drop_id,)
        ).fetchone()
        if quality_exists:
            assert quality is not None
            assert all(quality[name] == 0 for name in (
                "lead_rows_total", "lead_rows_rejected", "duplicate_lead_rows_merged", "field_warnings"
            ))
        else:
            assert quality is None


def test_motor_schema_valid_rejected_blank_rows_and_optional_warnings(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_1"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["Insured Name", "Mobile No", "Policy No", "Expiry Date", "Premium", "Language", "notes"],
        [
            ["Asha Rao", "+91 98765 43210", "POL-1", "01-Dec-2026", "₹12,345.67", "HI", "ignored"],
            ["", "", "", "", "", "", ""],
            ["   ", "9876543210", "POL-2", "31/02/2026", "N/A", "Hindi", "reject first"],
            ["Bad Phone", "5123456789", "POL-3", "", "", "", "reject phone"],
            ["Meera Das", "8765432109", "POL-4", "soon", "twelve", "Hindi", "warnings"],
        ],
    )

    result = _run_lead_stage(drop, settings)
    assert (result.lead_rows_total, result.lead_rows_rejected) == (4, 2)
    assert result.field_warnings == 3
    quality = _read_quality(settings, "drop_1")
    assert quality["lead_rows_total"] == 4
    assert quality["lead_rows_rejected"] == 2
    assert quality["field_warnings"] == 3

    with database(settings.database_path) as connection:
        leads = connection.execute("SELECT * FROM leads ORDER BY reference_value").fetchall()
        rejections = connection.execute(
            "SELECT workflow_id, batch_id, row_number, reason FROM lead_rejections "
            "ORDER BY row_number"
        ).fetchall()
    assert len(leads) == 2
    assert [(row["row_number"], row["reason"]) for row in rejections] == [
        (2, "missing_required:customer_name"),
        (3, "invalid_phone"),
    ]
    assert "Asha Rao" not in repr([tuple(row) for row in rejections])
    assert "+91 98765 43210" not in repr([tuple(row) for row in rejections])
    first = next(row for row in leads if row["reference_value"] == "POL-1")
    assert first["batch_id"] == "batch_001"
    assert '"policy_expiry": "2026-12-01"' in first["attributes_json"]
    assert '"premium_amount": "12345.67"' in first["attributes_json"]
    assert '"preferred_language": "hi"' in first["attributes_json"]
    warning_lead = next(row for row in leads if row["reference_value"] == "POL-4")
    assert '"policy_expiry": null' in warning_lead["attributes_json"]
    assert '"premium_amount": null' in warning_lead["attributes_json"]
    assert '"preferred_language": "en"' in warning_lead["attributes_json"]


def test_policy_feedback_schema_aliases_and_values_ingest(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_feedback"
    _write_csv(
        drop / "leads" / "policy_feedback" / "batch_010.csv",
        ["Name", "Contact", "Order ID", "Item", "Purchase Date", "Language"],
        [["Vikram Iyer", "(080) 7654-3210", "ORD-3", "Tablet", "03/10/2026", "Ta"]],
    )
    result = _run_lead_stage(drop, settings)
    assert result.lead_rows_total == 1
    with database(settings.database_path) as connection:
        row = connection.execute("SELECT * FROM leads").fetchone()
    assert row["reference_value"] == "ORD-3"
    assert row["batch_id"] == "batch_010"
    assert '"order_date": "2026-10-03"' in row["attributes_json"]
    assert '"preferred_language": "ta"' in row["attributes_json"]
    assert '"product": "Tablet"' in row["attributes_json"]


def test_duplicates_merge_in_filename_order_and_preserve_first_batch(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_merge"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_002.csv",
        ["Name", "Phone", "Policy", "Expiry", "Premium", "Language"],
        [["First Name", "9876543210", "POL-FIRST", "2026-12-01", "100.00", "en"]],
    )
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["Name", "Phone", "Policy", "Expiry", "Premium", "Language"],
        [["Later Name", "+91 98765 43210", "POL-LAST", "", "", "hi"]],
    )
    # Filename order means batch_001 is processed first, even though written second.
    result = _run_lead_stage(drop, settings)
    assert result.duplicate_lead_rows_merged == 1
    with database(settings.database_path) as connection:
        row = connection.execute("SELECT * FROM leads").fetchone()
    assert row["batch_id"] == "batch_001"
    assert row["reference_value"] == "POL-FIRST"
    assert decrypt_value(row["customer_name_cipher"], TEST_FERNET_KEY) == "First Name"
    assert '"policy_expiry": "2026-12-01"' in row["attributes_json"]
    assert '"premium_amount": "100.00"' in row["attributes_json"]
    assert '"preferred_language": "en"' in row["attributes_json"]


def test_cross_drop_duplicate_updates_non_null_fields_and_defaults(tmp_path):
    settings = _settings(tmp_path)
    headers = ["Name", "Phone", "Policy", "Expiry", "Premium", "Language"]
    drop1 = tmp_path / "drop_1"
    _write_csv(
        drop1 / "leads" / "motor_renewal" / "batch_001.csv",
        headers,
        [["Initial Name", "9876543210", "POL-1", "", "", ""]],
    )
    first_result = _run_lead_stage(drop1, settings)
    assert first_result.duplicate_lead_rows_merged == 0
    with database(settings.database_path) as connection:
        first = connection.execute("SELECT * FROM leads").fetchone()
    assert '"preferred_language": "en"' in first["attributes_json"]

    drop2 = tmp_path / "drop_2"
    _write_csv(
        drop2 / "leads" / "motor_renewal" / "batch_003.csv",
        headers,
        [
            ["Updated Name", "+91 98765 43210", "POL-2", "2026-12-01", "", "Ta"],
            ["Final Name", "09876543210", "POL-3", "soon", "", "Hindi"],
        ],
    )
    second_result = _run_lead_stage(drop2, settings)
    assert second_result.duplicate_lead_rows_merged == 2
    assert second_result.field_warnings == 2
    with database(settings.database_path) as connection:
        row = connection.execute("SELECT * FROM leads").fetchone()
    assert row["batch_id"] == "batch_001"
    assert row["reference_value"] == "POL-3"
    assert decrypt_value(row["customer_name_cipher"], TEST_FERNET_KEY) == "Final Name"
    attrs = row["attributes_json"]
    assert '"preferred_language": "ta"' in attrs  # invalid later enum did not overwrite
    assert '"policy_expiry": "2026-12-01"' in attrs  # invalid later date did not overwrite


def test_database_rows_do_not_contain_plaintext_name_or_phone(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_pii"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["customer_name", "phone", "policy_number"],
        [["Private Person", "+91 98765 43210", "POL-PII"]],
    )
    _run_lead_stage(drop, settings)
    with database(settings.database_path) as connection:
        row = connection.execute("SELECT * FROM leads").fetchone()
        assert decrypt_value(row["phone_cipher"], TEST_FERNET_KEY) == "+919876543210"
        assert decrypt_value(row["customer_name_cipher"], TEST_FERNET_KEY) == "Private Person"
        stored = b" ".join(
            value if isinstance(value, bytes) else str(value).encode("utf-8")
            for value in row
        )
        assert b"Private Person" not in stored
        assert b"+919876543210" not in stored
        assert b"98765 43210" not in stored


def test_lead_stage_does_not_claim_commit_or_complete_the_drop(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_stage"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["Name", "Phone", "Policy"],
        [["Stage Name", "9876543210", "POL-1"]],
    )
    initialize_database(settings.database_path)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT INTO ingested_drops (drop_id, source_path, content_sha256, status, ingested_at) "
            "VALUES ('drop_stage', ?, ?, 'processing', 'test-time')",
            (str(drop), hashlib.sha256(b"drop_stage").hexdigest()),
        )
        connection.execute("INSERT INTO drop_quality (drop_id) VALUES ('drop_stage')")
        stats = _ingest_lead_files(
            connection, drop, "drop_stage", settings, load_schemas()
        )
        assert stats.lead_rows_total == 1
        assert connection.execute(
            "SELECT status FROM ingested_drops WHERE drop_id='drop_stage'"
        ).fetchone()[0] == "processing"
        # A second connection cannot see the caller's uncommitted writes.
        with database(settings.database_path) as reader:
            assert reader.execute("SELECT count(*) FROM leads").fetchone()[0] == 0
            assert reader.execute("SELECT count(*) FROM ingested_drops").fetchone()[0] == 0

    with database(settings.database_path) as connection:
        assert connection.execute(
            "SELECT status FROM ingested_drops WHERE drop_id='drop_stage'"
        ).fetchone()[0] == "processing"
        assert connection.execute("SELECT count(*) FROM leads").fetchone()[0] == 1
        connection.execute(
            "UPDATE ingested_drops SET status='complete' WHERE drop_id='drop_stage'"
        )
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(LeadIngestionError, match="processing drop"):
            _ingest_lead_files(connection, drop, "drop_stage", settings, load_schemas())


def test_stage_requires_active_outer_transaction(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_no_transaction"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["Name", "Phone", "Policy"],
        [["Name", "9876543210", "POL-1"]],
    )
    _claim_for_test(settings, drop.name, drop)
    with database(settings.database_path) as connection:
        with pytest.raises(LeadIngestionError, match="outer database transaction"):
            _ingest_lead_files(connection, drop, drop.name, settings, load_schemas())
    _assert_no_stage_writes(settings, drop.name)


def test_stage_rejects_missing_drop_id_without_writes(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_missing_id"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["Name", "Phone", "Policy"],
        [["Name", "9876543210", "POL-1"]],
    )
    _claim_for_test(settings, "some_other_drop", drop)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(LeadIngestionError, match="existing processing drop"):
            _ingest_lead_files(connection, drop, "missing_drop", settings, load_schemas())
    _assert_no_stage_writes(settings, "some_other_drop")


def test_stage_rejects_non_processing_drop_without_writes(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_complete"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["Name", "Phone", "Policy"],
        [["Name", "9876543210", "POL-1"]],
    )
    _claim_for_test(settings, drop.name, drop, status="complete")
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(LeadIngestionError, match="existing processing drop"):
            _ingest_lead_files(connection, drop, drop.name, settings, load_schemas())
    _assert_no_stage_writes(settings, drop.name)


def test_stage_rejects_missing_quality_row_without_writes(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_missing_quality"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["Name", "Phone", "Policy"],
        [["Name", "9876543210", "POL-1"]],
    )
    _claim_for_test(settings, drop.name, drop, quality=False)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(LeadIngestionError, match="drop quality row"):
            _ingest_lead_files(connection, drop, drop.name, settings, load_schemas())
    _assert_no_stage_writes(settings, drop.name, quality_exists=False)


def test_stage_rejects_path_mismatched_with_claim_without_writes(tmp_path):
    settings = _settings(tmp_path)
    claimed = tmp_path / "claimed_drop"
    supplied = tmp_path / "other_drop"
    _write_csv(
        supplied / "leads" / "motor_renewal" / "batch_001.csv",
        ["Name", "Phone", "Policy"],
        [["Name", "9876543210", "POL-1"]],
    )
    _claim_for_test(settings, "claimed_drop", claimed)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(LeadIngestionError, match="does not match"):
            _ingest_lead_files(
                connection, supplied, "claimed_drop", settings, load_schemas()
            )
    _assert_no_stage_writes(settings, "claimed_drop")


def test_failed_csv_rolls_back_drop_leads_and_quality(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_rollback"
    wf_dir = drop / "leads" / "motor_renewal"
    _write_csv(
        wf_dir / "batch_001.csv",
        ["Name", "Phone", "Policy"],
        [["Should Roll Back", "9876543210", "POL-1"]],
    )
    (wf_dir / "batch_002.csv").write_bytes(b"Name,Phone,Policy\n\xff,9876543210,POL-2\n")
    with pytest.raises(LeadIngestionError, match="could not read CSV"):
        _run_lead_stage(drop, settings)
    with database(settings.database_path) as connection:
        assert connection.execute("SELECT count(*) FROM ingested_drops").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM leads").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM drop_quality").fetchone()[0] == 0


def test_required_validation_uses_schema_order_and_blank_rows_do_not_advance_row_numbers(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_order"
    _write_csv(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["Name", "Phone", "Policy"],
        [
            ["   ", "", " "],
            ["   ", "bad", ""],
            ["Name", "bad", ""],
            ["Name", "9876543210", ""],
            ["Name", "bad", "POL-2"],
        ],
    )
    result = _run_lead_stage(drop, settings)
    assert result.lead_rows_total == 4
    assert result.lead_rows_rejected == 4
    with database(settings.database_path) as connection:
        rows = connection.execute(
            "SELECT row_number, reason FROM lead_rejections ORDER BY row_number"
        ).fetchall()
    assert [(row["row_number"], row["reason"]) for row in rows] == [
        (1, "missing_required:customer_name"),
        (2, "invalid_phone"),
        (3, "missing_required:policy_number"),
        (4, "invalid_phone"),
    ]
