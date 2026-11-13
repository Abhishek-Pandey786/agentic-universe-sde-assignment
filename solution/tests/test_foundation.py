"""Foundation tests for secrets and the SQLite schema."""

from __future__ import annotations

import sqlite3

import pytest

from solution.au_pipeline.config import Settings
from solution.au_pipeline.db import database, initialize_database
from solution.au_pipeline.security import decrypt_value, encrypt_value, phone_digest

TEST_FERNET_KEY = b"MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
WRONG_FERNET_KEY = b"MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTE="


def valid_env(tmp_path):
    return {
        "AU_DATABASE_PATH": str(tmp_path / "assignment.sqlite3"),
        "AU_FERNET_KEY": TEST_FERNET_KEY.decode("ascii"),
        "AU_HMAC_SECRET": "h" * 48,
    }


def test_settings_requires_secrets_and_validates_keys(tmp_path):
    values = valid_env(tmp_path)
    settings = Settings.from_env(values)
    assert settings.database_path == tmp_path / "assignment.sqlite3"
    assert settings.fernet_key == values["AU_FERNET_KEY"].encode("ascii")
    assert settings.hmac_secret == values["AU_HMAC_SECRET"].encode("utf-8")
    representation = repr(settings)
    assert values["AU_FERNET_KEY"] not in representation
    assert values["AU_HMAC_SECRET"] not in representation

    with pytest.raises(ValueError, match="AU_FERNET_KEY is required"):
        Settings.from_env({"AU_HMAC_SECRET": "h" * 48})
    with pytest.raises(ValueError, match="valid Fernet key"):
        Settings.from_env({**values, "AU_FERNET_KEY": "not-a-key"})
    with pytest.raises(ValueError, match="at least 32"):
        Settings.from_env({**values, "AU_HMAC_SECRET": "short"})


def test_encryption_round_trip_and_wrong_key_rejection():
    token = encrypt_value("+919876543210", TEST_FERNET_KEY)
    assert isinstance(token, bytes)
    assert token != b"+919876543210"
    assert decrypt_value(token, TEST_FERNET_KEY) == "+919876543210"
    with pytest.raises(ValueError, match="could not decrypt"):
        decrypt_value(token, WRONG_FERNET_KEY)


def test_database_context_rolls_back_when_exception_escapes(tmp_path):
    path = tmp_path / "rollback.sqlite3"
    initialize_database(path)
    with pytest.raises(RuntimeError, match="force rollback"):
        with database(path) as connection:
            _insert_drop(connection)
            raise RuntimeError("force rollback")
    with database(path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM ingested_drops WHERE drop_id='drop_1'"
        ).fetchone()[0] == 0


def test_database_initialization_rolls_back_partial_ddl(tmp_path, monkeypatch):
    import solution.au_pipeline.db as db_module

    path = tmp_path / "ddl-rollback.sqlite3"
    monkeypatch.setattr(
        db_module,
        "_SCHEMA",
        "CREATE TABLE should_rollback (id INTEGER);\nTHIS IS INVALID SQL;",
    )
    with pytest.raises(sqlite3.OperationalError):
        initialize_database(path)
    with database(path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='should_rollback'"
        ).fetchone()[0] == 0

def test_phone_digest_is_deterministic_and_keyed():
    phone = "+919876543210"
    secret = b"s" * 32
    assert phone_digest(phone, secret) == phone_digest(phone, secret)
    assert phone_digest(phone, secret) != phone_digest(phone, b"t" * 32)
    assert len(phone_digest(phone, secret)) == 64
    with pytest.raises(ValueError):
        phone_digest("", secret)
    with pytest.raises(ValueError):
        phone_digest(phone, b"short")

def test_database_initialization_is_repeatable_and_enables_foreign_keys(tmp_path):
    path = tmp_path / "nested" / "pipeline.sqlite3"
    initialize_database(path)
    initialize_database(path)
    with database(path) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        names = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        assert {
            "ingested_drops", "leads", "events", "event_lead_links",
            "event_disposition_state", "lead_rejections", "drop_quality",
        } <= names
        indexes = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        assert {
            "idx_leads_workflow_batch", "idx_leads_workflow_reference",
            "idx_events_workflow_call", "idx_events_workflow_phone",
            "idx_events_occurred_at", "idx_event_lead_links_lead",
            "idx_disposition_state_state", "idx_rejections_report_order",
        } <= indexes

def _insert_drop(connection, drop_id="drop_1"):
    connection.execute(
        "INSERT INTO ingested_drops VALUES (?, ?, ?, ?, ?)",
        (drop_id, "/tmp/drop", "a" * 64, "complete", "2026-11-13T00:00:00Z"),
    )

def test_schema_unique_and_check_constraints(tmp_path):
    initialize_database(tmp_path / "test.sqlite3")
    with database(tmp_path / "test.sqlite3") as connection:
        _insert_drop(connection)
        lead_values = ("motor_renewal", "b" * 64, b"enc-phone", b"enc-name",
                       "POL-1", "{}", "batch_001", "drop_1", "drop_1", "now", "now")
        connection.execute(
            "INSERT INTO leads (workflow_id, phone_hmac, phone_cipher, customer_name_cipher, "
            "reference_value, attributes_json, batch_id, created_drop_id, updated_drop_id, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            lead_values,
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO leads (workflow_id, phone_hmac, phone_cipher, customer_name_cipher, "
                "reference_value, attributes_json, batch_id, created_drop_id, updated_drop_id, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                lead_values,
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO drop_quality (drop_id, lead_rows_total, lead_rows_rejected) "
                "VALUES ('drop_1', 0, 1)"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO events (event_id, workflow_id, event_type, call_id, occurred_at_utc, "
                "timestamp_status, payload_json, source_drop_id, source_file, source_line, inserted_at) "
                "VALUES ('evt_bad', 'motor_renewal', 'unknown.type', 'call1', NULL, 'valid', '{}', "
                "'drop_1', 'events.jsonl', 1, 'now')"
            )
        connection.execute(
            "INSERT INTO events (event_id, workflow_id, event_type, call_id, occurred_at_utc, "
            "timestamp_status, payload_json, source_drop_id, source_file, source_line, inserted_at) "
            "VALUES ('evt_unique', 'motor_renewal', 'call.initiated', 'call_1', 'now', 'valid', '{}', "
            "'drop_1', 'events.jsonl', 1, 'now')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO events (event_id, workflow_id, event_type, call_id, occurred_at_utc, "
                "timestamp_status, payload_json, source_drop_id, source_file, source_line, inserted_at) "
                "VALUES ('evt_unique', 'motor_renewal', 'call.initiated', 'call_2', 'now', 'valid', '{}', "
                "'drop_1', 'events.jsonl', 2, 'now')"
            )
        rejection = ("drop_1", "motor_renewal", "batch_001", 1, "invalid_phone")
        connection.execute(
            "INSERT INTO lead_rejections (drop_id, workflow_id, batch_id, row_number, reason) "
            "VALUES (?, ?, ?, ?, ?)", rejection
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO lead_rejections (drop_id, workflow_id, batch_id, row_number, reason) "
                "VALUES (?, ?, ?, ?, ?)", rejection
            )

def test_foreign_keys_enforce_drop_and_matching_workflow(tmp_path):
    path = tmp_path / "fk.sqlite3"
    initialize_database(path)
    with database(path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO drop_quality (drop_id) VALUES ('missing')"
            )
        _insert_drop(connection)
        connection.execute(
            "INSERT INTO leads (workflow_id, phone_hmac, phone_cipher, customer_name_cipher, "
            "reference_value, attributes_json, batch_id, created_drop_id, updated_drop_id, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("motor_renewal", "b" * 64, b"p", b"n", "POL-1", "{}", "batch_001",
             "drop_1", "drop_1", "now", "now"),
        )
        connection.execute(
            "INSERT INTO events (event_id, workflow_id, event_type, call_id, occurred_at_utc, "
            "timestamp_status, payload_json, source_drop_id, source_file, source_line, inserted_at) "
            "VALUES ('evt_1', 'motor_renewal', 'call.initiated', 'call_1', 'now', 'valid', '{}', "
            "'drop_1', 'events.jsonl', 1, 'now')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO event_lead_links VALUES ('evt_1', 'policy_feedback', 1, 'phone', 'now')"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO event_lead_links VALUES ('not-an-event', 'motor_renewal', 1, 'phone', 'now')"
            )

def test_events_are_append_only(tmp_path):
    path = tmp_path / "append.sqlite3"
    initialize_database(path)
    with database(path) as connection:
        _insert_drop(connection)
        connection.execute(
            "INSERT INTO events (event_id, workflow_id, event_type, call_id, occurred_at_utc, "
            "timestamp_status, payload_json, source_drop_id, source_file, source_line, inserted_at) "
            "VALUES ('evt_1', 'motor_renewal', 'call.initiated', 'call_1', 'now', 'valid', '{}', "
            "'drop_1', 'events.jsonl', 1, 'now')"
        )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE events SET payload_json='{}' WHERE event_id='evt_1'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM events WHERE event_id='evt_1'")
