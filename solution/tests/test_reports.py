"""Synthetic integration tests for the Assignment A reporting engine."""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from solution.au_pipeline.config import Settings
from solution.au_pipeline.db import database, initialize_database
from solution.au_pipeline.normalize import normalize_phone
from solution.au_pipeline.reports import generate_reports
from solution.au_pipeline.schema import load_schemas
from solution.au_pipeline.security import encrypt_value, phone_digest


TEST_FERNET_KEY = b"MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
TEST_HMAC_SECRET = b"test-only-assignment-a-secret-32-bytes"
QUALITY_FIELDS = {
    "lead_rows_total", "lead_rows_rejected", "rejections_by_reason",
    "duplicate_lead_rows_merged", "field_warnings", "events_total",
    "malformed_events", "duplicate_events_dropped", "orphan_events",
    "unknown_disposition_events",
}


def _settings(tmp_path):
    return Settings(tmp_path / "reports.sqlite3", TEST_FERNET_KEY, TEST_HMAC_SECRET)


def _initialize(settings):
    initialize_database(settings.database_path)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT INTO ingested_drops "
            "(drop_id, source_path, content_sha256, status, ingested_at) "
            "VALUES ('drop_1', 'synthetic/drop_1', ?, 'complete', '2026-11-01T00:00:00+00:00')",
            ("a" * 64,),
        )
        connection.execute("INSERT INTO drop_quality (drop_id) VALUES ('drop_1')")


def _seed_lead(connection, workflow, reference, batch, phone):
    canonical = normalize_phone(phone)
    cursor = connection.execute(
        "INSERT INTO leads (workflow_id, phone_hmac, phone_cipher, customer_name_cipher, "
        "reference_value, attributes_json, batch_id, created_drop_id, updated_drop_id, "
        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, '{}', ?, 'drop_1', 'drop_1', ?, ?)",
        (
            workflow, phone_digest(canonical, TEST_HMAC_SECRET),
            encrypt_value(canonical, TEST_FERNET_KEY),
            encrypt_value("Private Synthetic Name", TEST_FERNET_KEY),
            reference, batch, "2026-11-01T00:00:00+00:00", "2026-11-01T00:00:00+00:00",
        ),
    )
    return cursor.lastrowid


def _seed_event(
    connection,
    event_id,
    workflow,
    event_type,
    call_id,
    lead_id,
    *,
    occurred="2026-10-05T20:00:00+00:00",
    timestamp_status="valid",
    code=None,
    state=None,
):
    is_disposition = event_type.startswith("disposition.")
    code_cipher = encrypt_value(code or "", TEST_FERNET_KEY) if is_disposition else None
    stored_time = occurred if timestamp_status == "valid" else None
    connection.execute(
        "INSERT INTO events (event_id, workflow_id, event_type, call_id, occurred_at_utc, "
        "timestamp_status, disposition_code_cipher, payload_json, source_drop_id, "
        "source_file, source_line, inserted_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'drop_1', 'events.jsonl', 1, '2026-11-01T00:00:00+00:00')",
        (event_id, workflow, event_type, call_id, stored_time, timestamp_status, code_cipher),
    )
    if lead_id is not None:
        connection.execute(
            "INSERT INTO event_lead_links "
            "(event_id, workflow_id, lead_id, link_method, linked_at) "
            "VALUES (?, ?, ?, 'phone', '2026-11-01T00:00:00+00:00')",
            (event_id, workflow, lead_id),
        )
    if is_disposition:
        connection.execute(
            "INSERT INTO event_disposition_state "
            "(event_id, state, catalogue_workflow_id, classified_at) VALUES (?, ?, ?, ?)",
            (
                event_id, state or "known",
                workflow, "2026-11-01T00:00:00+00:00",
            ),
        )


def _read_csv(path):
    with path.open("r", encoding="utf-8", newline="") as stream:
        return list(csv.reader(stream))


def test_empty_database_generates_exact_empty_reports(tmp_path):
    settings = _settings(tmp_path)
    initialize_database(settings.database_path)
    output = tmp_path / "reports"

    generate_reports(output, settings, load_schemas())

    assert sorted(path.name for path in output.iterdir()) == [
        "batch_summary.csv", "daily_overview.csv", "data_quality.json",
        "lead_report.csv", "rejections.csv",
    ]
    assert _read_csv(output / "batch_summary.csv") == [[
        "workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
        "in_progress_telephony_failed", "in_progress_dialed", "completed",
    ]]
    assert _read_csv(output / "daily_overview.csv") == [[
        "workflow_id", "date", "total_calls", "connected_calls",
        "connectivity_rate", "success_leads",
    ]]
    assert _read_csv(output / "lead_report.csv") == [[
        "workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
        "total_calls", "connected_calls", "last_call_at", "current_disposition",
        "ever_success",
    ]]
    assert _read_csv(output / "rejections.csv") == [[
        "workflow_id", "batch_id", "row_number", "reason",
    ]]
    quality = json.loads((output / "data_quality.json").read_text(encoding="utf-8"))
    assert set(quality) == QUALITY_FIELDS
    assert quality["rejections_by_reason"] == {}
    assert all(value == 0 for key, value in quality.items() if key != "rejections_by_reason")


def test_buckets_effective_dispositions_daily_metrics_and_ordering(tmp_path):
    settings = _settings(tmp_path)
    _initialize(settings)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        lead_none = _seed_lead(connection, "motor_renewal", "M-0", "batch_001", "9876543210")
        lead_failed = _seed_lead(connection, "motor_renewal", "M-1", "batch_002", "9876543211")
        lead_dialed = _seed_lead(connection, "motor_renewal", "M-2", "batch_003", "9876543212")
        lead_terminal = _seed_lead(connection, "motor_renewal", "M-3", "batch_004", "9876543213")
        lead_override = _seed_lead(connection, "motor_renewal", "M-4", "batch_005", "9876543214")
        _seed_lead(connection, "policy_feedback", "P-1", "batch_001", "9876543215")

        _seed_event(connection, "a_init_fail", "motor_renewal", "call.initiated", "call_fail", lead_failed)
        _seed_event(connection, "b_fail", "motor_renewal", "call.telephony_failed", "call_fail", lead_failed)
        _seed_event(connection, "c_init_dial", "motor_renewal", "call.initiated", "call_dial", lead_dialed)
        _seed_event(connection, "d_connected", "motor_renewal", "call.connected", "call_dial", lead_dialed)
        _seed_event(connection, "e_init_terminal", "motor_renewal", "call.initiated", "call_terminal", lead_terminal)
        _seed_event(connection, "f_fail_terminal", "motor_renewal", "call.telephony_failed", "call_terminal", lead_terminal)
        _seed_event(connection, "g_terminal", "motor_renewal", "disposition.set", "call_terminal", lead_terminal, code="not_interested")

        _seed_event(connection, "h_init_success", "motor_renewal", "call.initiated", "call_success", lead_override, occurred="2026-10-05T19:00:00+00:00")
        _seed_event(connection, "i_success_set", "motor_renewal", "disposition.set", "call_success", lead_override, occurred="2026-10-05T19:05:00+00:00", code="callback_requested")
        _seed_event(connection, "j_success_override", "motor_renewal", "disposition.override", "call_success", lead_override, occurred="2026-10-05T19:10:00+00:00", code="renewed")
        _seed_event(connection, "k_init_tie", "motor_renewal", "call.initiated", "call_tie", lead_override, occurred="2026-10-05T20:00:00+00:00")
        _seed_event(connection, "l_connected_tie", "motor_renewal", "call.connected", "call_tie", lead_override, occurred="2026-10-05T20:01:00+00:00")
        _seed_event(connection, "m_set_tie", "motor_renewal", "disposition.set", "call_tie", lead_override, code="renewed")
        _seed_event(connection, "evt_override_a", "motor_renewal", "disposition.override", "call_tie", lead_override, code="callback_requested")
        _seed_event(connection, "evt_override_z", "motor_renewal", "disposition.override", "call_tie", lead_override, code="not_interested")

    output = tmp_path / "reports"
    generate_reports(output, settings, load_schemas())

    batch = _read_csv(output / "batch_summary.csv")
    assert batch[0] == [
        "workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
        "in_progress_telephony_failed", "in_progress_dialed", "completed",
    ]
    motor = {row[1]: row[3:] for row in batch[1:] if row[0] == "motor_renewal"}
    assert motor == {
        "batch_001": ["1", "0", "0", "0"],
        "batch_002": ["0", "1", "0", "0"],
        "batch_003": ["0", "0", "1", "0"],
        "batch_004": ["0", "0", "0", "1"],
        "batch_005": ["0", "0", "0", "1"],
    }
    daily = _read_csv(output / "daily_overview.csv")
    assert daily[1] == ["motor_renewal", "2026-10-06", "5", "2", "0.4000", "1"]

    lead_rows = _read_csv(output / "lead_report.csv")
    assert lead_rows[0] == [
        "workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
        "total_calls", "connected_calls", "last_call_at", "current_disposition",
        "ever_success",
    ]
    by_ref = {row[1]: row for row in lead_rows[1:]}
    assert by_ref["M-0"][3:7] == ["+91******3210", "yet_to_reach_out", "0", "0"]
    assert by_ref["M-3"][4] == "completed"
    assert by_ref["M-4"][5:10] == [
        "2", "1", "2026-10-06T01:30:00+05:30", "not_interested", "true"
    ]
    assert [row[0] for row in lead_rows[1:]] == sorted(row[0] for row in lead_rows[1:])


def test_invalid_timestamps_and_unknown_types_do_not_affect_call_metrics(tmp_path):
    settings = _settings(tmp_path)
    _initialize(settings)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        lead_id = _seed_lead(connection, "motor_renewal", "M-1", "batch_001", "9876543210")
        _seed_event(connection, "evt_init", "motor_renewal", "call.initiated", "c1", lead_id)
        _seed_event(
            connection, "evt_bad_connected", "motor_renewal", "call.connected", "c1", lead_id,
            occurred=None, timestamp_status="invalid",
        )
        _seed_event(connection, "evt_unknown_type", "motor_renewal", "call.mystery", "c1", lead_id)
        _seed_event(
            connection, "evt_bad_disposition", "motor_renewal", "disposition.set", "c1", lead_id,
            occurred=None, timestamp_status="invalid", code="renewed", state="invalid_timestamp",
        )
        _seed_event(
            connection, "evt_unknown_disposition", "motor_renewal", "disposition.override", "c1", lead_id,
            code="not_in_catalogue", state="unknown",
        )

    output = tmp_path / "reports"
    generate_reports(output, settings, load_schemas())

    lead = _read_csv(output / "lead_report.csv")[1]
    assert lead[4:10] == [
        "in_progress_dialed", "1", "0", "2026-10-06T01:30:00+05:30", "", "false"
    ]
    assert _read_csv(output / "daily_overview.csv")[1][2:6] == ["1", "0", "0.0000", "0"]
    quality = json.loads((output / "data_quality.json").read_text(encoding="utf-8"))
    assert quality["unknown_disposition_events"] == 1


def test_rejections_pii_safety_stable_output_and_database_read_only(tmp_path, caplog):
    settings = _settings(tmp_path)
    _initialize(settings)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        lead_id = _seed_lead(connection, "motor_renewal", "M-1", "batch_001", "9876543210")
        connection.execute(
            "INSERT INTO lead_rejections (drop_id, workflow_id, batch_id, row_number, reason) "
            "VALUES ('drop_1', 'motor_renewal', 'batch_001', 7, 'invalid_phone')"
        )
        _seed_event(connection, "evt_1", "motor_renewal", "call.initiated", "c1", lead_id)

    with database(settings.database_path) as connection:
        before = {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
            for table in ("ingested_drops", "leads", "events", "event_lead_links", "event_disposition_state", "lead_rejections", "drop_quality")
        }

    output = tmp_path / "reports"
    generate_reports(output, settings, load_schemas())
    first = {path.name: path.read_bytes() for path in output.iterdir()}
    generate_reports(output, settings, load_schemas())
    second = {path.name: path.read_bytes() for path in output.iterdir()}

    assert first == second
    rejection_text = (output / "rejections.csv").read_text(encoding="utf-8")
    assert rejection_text == "workflow_id,batch_id,row_number,reason\nmotor_renewal,batch_001,7,invalid_phone\n"
    all_reports = b"".join(first.values()).decode("utf-8")
    assert "Private Synthetic Name" not in all_reports
    assert "9876543210" not in all_reports
    assert "+91******3210" in all_reports
    assert not caplog.records

    with database(settings.database_path) as connection:
        after = {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
            for table in before
        }
    assert after == before


def test_data_quality_has_exact_fields_and_aggregated_counters(tmp_path):
    settings = _settings(tmp_path)
    _initialize(settings)
    with database(settings.database_path) as connection:
        connection.execute(
            "UPDATE drop_quality SET lead_rows_total=3, lead_rows_rejected=1, "
            "duplicate_lead_rows_merged=2, field_warnings=4, events_total=6, "
            "malformed_events=1, duplicate_events_dropped=2 WHERE drop_id='drop_1'"
        )
        connection.execute(
            "INSERT INTO lead_rejections (drop_id, workflow_id, batch_id, row_number, reason) "
            "VALUES ('drop_1', 'motor_renewal', 'batch_001', 1, 'invalid_phone')"
        )

    output = tmp_path / "reports"
    generate_reports(output, settings, load_schemas())
    quality = json.loads((output / "data_quality.json").read_text(encoding="utf-8"))

    assert set(quality) == QUALITY_FIELDS
    assert quality == {
        "lead_rows_total": 3,
        "lead_rows_rejected": 1,
        "rejections_by_reason": {"invalid_phone": 1},
        "duplicate_lead_rows_merged": 2,
        "field_warnings": 4,
        "events_total": 6,
        "malformed_events": 1,
        "duplicate_events_dropped": 2,
        "orphan_events": 0,
        "unknown_disposition_events": 0,
    }


def test_winning_disposition_is_selected_before_catalogue_lookup(tmp_path):
    settings = _settings(tmp_path)
    _initialize(settings)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        set_then_unknown_override = _seed_lead(
            connection, "motor_renewal", "M-unknown-override", "batch_001", "9876543210"
        )
        unknown_set_then_override = _seed_lead(
            connection, "motor_renewal", "M-known-override", "batch_001", "9876543211"
        )
        earlier_known_override_then_unknown = _seed_lead(
            connection, "motor_renewal", "M-latest-unknown", "batch_001", "9876543212"
        )

        _seed_event(connection, "a_init", "motor_renewal", "call.initiated", "call_a", set_then_unknown_override)
        _seed_event(connection, "a_set", "motor_renewal", "disposition.set", "call_a", set_then_unknown_override, occurred="2026-10-05T20:01:00+00:00", code="renewed")
        _seed_event(connection, "a_override_unknown", "motor_renewal", "disposition.override", "call_a", set_then_unknown_override, occurred="2026-10-05T20:02:00+00:00", code="unknown_code", state="unknown")

        _seed_event(connection, "b_init", "motor_renewal", "call.initiated", "call_b", unknown_set_then_override)
        _seed_event(connection, "b_set_unknown", "motor_renewal", "disposition.set", "call_b", unknown_set_then_override, occurred="2026-10-05T20:01:00+00:00", code="unknown_code", state="unknown")
        _seed_event(connection, "b_override_known", "motor_renewal", "disposition.override", "call_b", unknown_set_then_override, occurred="2026-10-05T20:02:00+00:00", code="renewed")

        _seed_event(connection, "c_init", "motor_renewal", "call.initiated", "call_c", earlier_known_override_then_unknown)
        _seed_event(connection, "c_set", "motor_renewal", "disposition.set", "call_c", earlier_known_override_then_unknown, occurred="2026-10-05T20:01:00+00:00", code="renewed")
        _seed_event(connection, "c_override_known", "motor_renewal", "disposition.override", "call_c", earlier_known_override_then_unknown, occurred="2026-10-05T20:02:00+00:00", code="callback_requested")
        _seed_event(connection, "c_override_unknown_latest", "motor_renewal", "disposition.override", "call_c", earlier_known_override_then_unknown, occurred="2026-10-05T20:03:00+00:00", code="unknown_code", state="unknown")

    output = tmp_path / "reports"
    generate_reports(output, settings, load_schemas())
    rows = {row[1]: row for row in _read_csv(output / "lead_report.csv")[1:]}

    assert rows["M-unknown-override"][4] == "in_progress_dialed"
    assert rows["M-unknown-override"][8:] == ["", "false"]
    assert rows["M-known-override"][4] == "completed"
    assert rows["M-known-override"][8:] == ["renewed", "true"]
    assert rows["M-latest-unknown"][4] == "in_progress_dialed"
    assert rows["M-latest-unknown"][8:] == ["", "false"]


def test_rejections_with_shared_sort_fields_are_stable_across_drops(tmp_path):
    settings = _settings(tmp_path)
    _initialize(settings)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        for drop_id in ("drop_3", "drop_2"):
            connection.execute(
                "INSERT INTO ingested_drops "
                "(drop_id, source_path, content_sha256, status, ingested_at) "
                "VALUES (?, ?, ?, 'complete', '2026-11-01T00:00:00+00:00')",
                (drop_id, "synthetic/" + drop_id, drop_id[5] * 64),
            )
            connection.execute("INSERT INTO drop_quality (drop_id) VALUES (?)", (drop_id,))
        for drop_id, reason in (
            ("drop_3", "invalid_phone"),
            ("drop_2", "missing_required:customer_name"),
            ("drop_1", "invalid_phone"),
        ):
            connection.execute(
                "INSERT INTO lead_rejections "
                "(drop_id, workflow_id, batch_id, row_number, reason) "
                "VALUES (?, 'motor_renewal', 'batch_001', 4, ?)",
                (drop_id, reason),
            )

    output = tmp_path / "reports"
    generate_reports(output, settings, load_schemas())
    first = (output / "rejections.csv").read_bytes()
    generate_reports(output, settings, load_schemas())
    second = (output / "rejections.csv").read_bytes()

    assert first == second
    assert first.decode("utf-8").splitlines() == [
        "workflow_id,batch_id,row_number,reason",
        "motor_renewal,batch_001,4,invalid_phone",
        "motor_renewal,batch_001,4,invalid_phone",
        "motor_renewal,batch_001,4,missing_required:customer_name",
    ]
