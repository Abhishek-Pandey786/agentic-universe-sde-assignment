"""Integration tests for the transactional Assignment A event stage."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

import pytest

from solution.au_pipeline import db as db_module
from solution.au_pipeline.config import Settings
from solution.au_pipeline.db import database, initialize_database
from solution.au_pipeline.events import EventIngestionError, _ingest_event_files
from solution.au_pipeline.normalize import normalize_phone
from solution.au_pipeline.schema import load_schemas
from solution.au_pipeline.security import decrypt_value, encrypt_value, phone_digest


TEST_FERNET_KEY = b"MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
TEST_HMAC_SECRET = b"test-only-assignment-a-secret-32-bytes"


def _settings(tmp_path):
    return Settings(tmp_path / "events.sqlite3", TEST_FERNET_KEY, TEST_HMAC_SECRET)


def _event(event_id="evt_1", *, event_type="call.initiated", phone="9876543210",
           workflow_id="motor_renewal", call_id="call_1", occurred_at="2026-10-05T13:20:21Z",
           payload=None, **extra):
    value = {
        "event_id": event_id,
        "event_type": event_type,
        "workflow_id": workflow_id,
        "call_id": call_id,
        "occurred_at": occurred_at,
        "payload": payload or {},
    }
    if phone is not None:
        value["phone"] = phone
    value.update(extra)
    return value


def _write_jsonl(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _claim_drop(settings, drop_path, drop_id=None, *, status="processing", quality=True):
    initialize_database(settings.database_path)
    drop_id = drop_id or drop_path.name
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT INTO ingested_drops (drop_id, source_path, content_sha256, status, ingested_at) "
            "VALUES (?, ?, ?, ?, 'test-time')",
            (drop_id, str(drop_path.resolve()), ("a" * 64), status),
        )
        if quality:
            connection.execute("INSERT INTO drop_quality (drop_id) VALUES (?)", (drop_id,))
    return drop_id


def _seed_lead(connection, *, workflow_id="motor_renewal", phone="9876543210", drop_id="drop_1"):
    canonical = normalize_phone(phone)
    connection.execute(
        "INSERT INTO leads (workflow_id, phone_hmac, phone_cipher, customer_name_cipher, "
        "reference_value, attributes_json, batch_id, created_drop_id, updated_drop_id, "
        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            workflow_id, phone_digest(canonical, TEST_HMAC_SECRET),
            encrypt_value(canonical, TEST_FERNET_KEY),
            encrypt_value("Test Customer", TEST_FERNET_KEY), "REF-1", "{}", "batch_001",
            drop_id, drop_id, "now", "now",
        ),
    )


def _run_event_stage(drop_path, settings, *, drop_id=None, schemas=None):
    schemas = schemas or load_schemas()
    drop_id = drop_id or drop_path.name
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        return _ingest_event_files(connection, drop_path, drop_id, settings, schemas)


def _assert_event_counters(settings, drop_id, *, total=0, malformed=0, duplicates=0):
    with database(settings.database_path) as connection:
        row = connection.execute(
            "SELECT events_total, malformed_events, duplicate_events_dropped "
            "FROM drop_quality WHERE drop_id=?", (drop_id,)
        ).fetchone()
    assert tuple(row) == (total, malformed, duplicates)


def test_valid_phone_event_is_linked_with_encrypted_phone_and_payload(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_1"
    _claim_drop(settings, drop)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _seed_lead(connection)
    raw = _event(
        phone="+91 98765 43210",
        payload={"reason": "callback", "customer_name": "Private Customer"},
    )
    _write_jsonl(drop / "events" / "events_0001.jsonl", [json.dumps(raw)])

    result = _run_event_stage(drop, settings)
    assert result.events_total == 1
    with database(settings.database_path) as connection:
        row = connection.execute("SELECT * FROM events WHERE event_id='evt_1'").fetchone()
        link = connection.execute(
            "SELECT lead_id, link_method FROM event_lead_links WHERE event_id='evt_1'"
        ).fetchone()
    assert row["phone_hmac"] == phone_digest("+919876543210", TEST_HMAC_SECRET)
    assert decrypt_value(row["phone_cipher"], TEST_FERNET_KEY) == "+919876543210"
    assert link["link_method"] == "phone"
    payload_envelope = json.loads(row["payload_json"])
    assert set(payload_envelope) == {"_encrypted_event"}
    raw_round_trip = json.loads(
        decrypt_value(payload_envelope["_encrypted_event"].encode("ascii"), TEST_FERNET_KEY)
    )
    assert raw_round_trip["phone"] == "+91 98765 43210"
    stored = repr(tuple(row)).encode("utf-8")
    assert b"Private Customer" not in stored
    assert b"+91 98765 43210" not in stored
    assert b"9876543210" not in stored
    _assert_event_counters(settings, "drop_1", total=1)


def test_malformed_json_and_missing_or_blank_event_ids_are_counted(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_malformed"
    _claim_drop(settings, drop)
    _write_jsonl(
        drop / "events" / "events_0001.jsonl",
        ["{broken", "{}", '{"event_id":"   "}', json.dumps(_event("evt_ok", phone=None))],
    )
    result = _run_event_stage(drop, settings)
    assert (result.events_total, result.malformed_events) == (4, 3)
    with database(settings.database_path) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1
    _assert_event_counters(settings, "drop_malformed", total=4, malformed=3)


def test_whitespace_only_jsonl_lines_are_excluded_from_event_counters(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_whitespace"
    _claim_drop(settings, drop)
    _write_jsonl(
        drop / "events" / "events_0001.jsonl",
        ["   \t", "", json.dumps(_event("evt_after_blank", phone=None))],
    )
    result = _run_event_stage(drop, settings)
    assert result.events_total == 1
    assert result.malformed_events == 0
    _assert_event_counters(settings, "drop_whitespace", total=1, malformed=0)


def test_duplicate_event_ids_within_and_across_drops_are_dropped(tmp_path):
    settings = _settings(tmp_path)
    drop1 = tmp_path / "drop_1"
    _claim_drop(settings, drop1)
    _write_jsonl(
        drop1 / "events" / "events_0001.jsonl",
        [json.dumps(_event("evt_repeat", payload={"marker": "first"})),
         json.dumps(_event("evt_repeat", payload={"marker": "second"}))],
    )
    result1 = _run_event_stage(drop1, settings)
    assert (result1.events_total, result1.duplicate_events_dropped) == (2, 1)

    drop2 = tmp_path / "drop_2"
    _claim_drop(settings, drop2)
    _write_jsonl(drop2 / "events" / "events_1001.jsonl", [json.dumps(_event("evt_repeat"))])
    result2 = _run_event_stage(drop2, settings)
    assert (result2.events_total, result2.duplicate_events_dropped) == (1, 1)
    with database(settings.database_path) as connection:
        row = connection.execute("SELECT source_drop_id FROM events WHERE event_id='evt_repeat'").fetchone()
        assert row["source_drop_id"] == "drop_1"
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 1
    _assert_event_counters(settings, "drop_1", total=2, duplicates=1)
    _assert_event_counters(settings, "drop_2", total=1, duplicates=1)


def test_unknown_event_type_is_retained_and_unknown_disposition_is_classified(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_unknown"
    _claim_drop(settings, drop)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _seed_lead(connection, drop_id="drop_unknown")
    _write_jsonl(
        drop / "events" / "events_0001.jsonl",
        [
            json.dumps(_event("evt_future", event_type="call.future_type")),
            json.dumps(_event("evt_unknown_code", event_type="disposition.set",
                              payload={"disposition_code": "not_in_catalogue"})),
            json.dumps(_event("evt_known_code", event_type="disposition.set",
                              payload={"disposition_code": "renewed"})),
        ],
    )
    result = _run_event_stage(drop, settings)
    assert result.events_total == 3
    assert result.malformed_events == 0
    assert result.unknown_disposition_events == 1
    with database(settings.database_path) as connection:
        future = connection.execute("SELECT event_type FROM events WHERE event_id='evt_future'").fetchone()
        states = dict(connection.execute(
            "SELECT event_id, state FROM event_disposition_state"
        ).fetchall())
    assert future["event_type"] == "call.future_type"
    assert states == {"evt_unknown_code": "unknown", "evt_known_code": "known"}


def test_phone_linking_is_workflow_scoped_and_orphans_are_retained(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_scope"
    _claim_drop(settings, drop)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _seed_lead(connection, workflow_id="motor_renewal", drop_id="drop_scope")
    _write_jsonl(
        drop / "events" / "events_0001.jsonl",
        [
            json.dumps(_event("evt_motor", workflow_id="motor_renewal")),
            json.dumps(_event("evt_feedback", workflow_id="policy_feedback")),
            json.dumps(_event("evt_orphan", phone="+91 91234 56789")),
        ],
    )
    result = _run_event_stage(drop, settings)
    assert result.events_total == 3
    with database(settings.database_path) as connection:
        links = {row[0] for row in connection.execute("SELECT event_id FROM event_lead_links")}
        event_ids = {row[0] for row in connection.execute("SELECT event_id FROM events")}
    assert links == {"evt_motor"}
    assert event_ids == {"evt_motor", "evt_feedback", "evt_orphan"}


def test_override_before_supporting_event_resolves_after_file_is_read(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_override"
    _claim_drop(settings, drop)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _seed_lead(connection, drop_id="drop_override")
    override = _event(
        "evt_override", event_type="disposition.override", phone=None,
        payload={"disposition_code": "renewed", "reason": "qa_review"},
    )
    supporting = _event("evt_call", event_type="call.initiated", phone="9876543210")
    _write_jsonl(
        drop / "events" / "events_0001.jsonl",
        [json.dumps(override), json.dumps(supporting)],
    )
    result = _run_event_stage(drop, settings)
    assert result.unknown_disposition_events == 0
    with database(settings.database_path) as connection:
        link = connection.execute(
            "SELECT lead_id, link_method FROM event_lead_links WHERE event_id='evt_override'"
        ).fetchone()
        state = connection.execute(
            "SELECT state FROM event_disposition_state WHERE event_id='evt_override'"
        ).fetchone()[0]
    assert link["link_method"] == "call_id"
    assert state == "known"


def test_prior_drop_override_retains_call_id_for_future_orchestrator(tmp_path):
    settings = _settings(tmp_path)
    drop1 = tmp_path / "drop_override_first"
    _claim_drop(settings, drop1)
    override = _event(
        "evt_prior_override", event_type="disposition.override", phone=None,
        call_id="call_arrives_later",
        payload={"disposition_code": "renewed", "reason": "qa_review"},
    )
    _write_jsonl(drop1 / "events" / "events_0001.jsonl", [json.dumps(override)])
    _run_event_stage(drop1, settings)

    with database(settings.database_path) as connection:
        prior = connection.execute(
            "SELECT e.workflow_id, e.call_id, e.disposition_code_cipher, s.state "
            "FROM events AS e JOIN event_disposition_state AS s ON s.event_id=e.event_id "
            "WHERE e.event_id='evt_prior_override'"
        ).fetchone()
        assert prior["call_id"] == "call_arrives_later"
        assert prior["disposition_code_cipher"] is not None
        assert prior["state"] == "pending_link"
        assert connection.execute(
            "SELECT count(*) FROM event_lead_links WHERE event_id='evt_prior_override'"
        ).fetchone()[0] == 0

    drop2 = tmp_path / "drop_call_later"
    _claim_drop(settings, drop2)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        _seed_lead(connection, drop_id=drop2.name)
    supporting = _event(
        "evt_later_call", event_type="call.initiated", phone="9876543210",
        call_id="call_arrives_later",
    )
    _write_jsonl(drop2 / "events" / "events_1001.jsonl", [json.dumps(supporting)])
    _run_event_stage(drop2, settings)

    with database(settings.database_path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM event_lead_links WHERE event_id='evt_prior_override'"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM event_lead_links WHERE event_id='evt_later_call'"
        ).fetchone()[0] == 1
        candidates = connection.execute(
            "SELECT DISTINCT l.lead_id FROM events AS e "
            "JOIN event_lead_links AS l ON l.event_id=e.event_id "
            "WHERE e.workflow_id=? AND e.call_id=? AND e.event_id<>? "
            "AND e.event_type<>'disposition.override'",
            (prior["workflow_id"], prior["call_id"], "evt_prior_override"),
        ).fetchall()
        state = connection.execute(
            "SELECT state FROM event_disposition_state WHERE event_id='evt_prior_override'"
        ).fetchone()[0]
    assert len(candidates) == 1
    assert state == "pending_link"


def test_timestamp_forms_and_invalid_timestamp_are_retained(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_times"
    _claim_drop(settings, drop)
    timestamps = {
        "evt_utc": "2026-10-05T13:20:21Z",
        "evt_offset": "2026-10-05T18:50:21+05:30",
        "evt_naive": "2026-10-05 18:50:21",
        "evt_bad_time": "2026-02-30T10:00:00Z",
    }
    _write_jsonl(
        drop / "events" / "events_0001.jsonl",
        [json.dumps(_event(event_id, phone=None, occurred_at=value))
         for event_id, value in timestamps.items()],
    )
    _run_event_stage(drop, settings)
    with database(settings.database_path) as connection:
        rows = {row["event_id"]: row for row in connection.execute("SELECT * FROM events")}
    assert rows["evt_utc"]["occurred_at_utc"] == "2026-10-05T13:20:21+00:00"
    assert rows["evt_offset"]["occurred_at_utc"] == "2026-10-05T13:20:21+00:00"
    assert rows["evt_naive"]["occurred_at_utc"] == "2026-10-05T13:20:21+00:00"
    assert rows["evt_bad_time"]["timestamp_status"] == "invalid"
    assert rows["evt_bad_time"]["occurred_at_utc"] is None


def test_file_and_line_processing_order_is_deterministic(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_order"
    _claim_drop(settings, drop)
    _write_jsonl(
        drop / "events" / "events_0002.jsonl",
        [json.dumps(_event("evt_duplicate", payload={"which": "later_file"})),
         json.dumps(_event("evt_last"))],
    )
    _write_jsonl(
        drop / "events" / "events_0001.jsonl",
        [json.dumps(_event("evt_duplicate", payload={"which": "first_file"})),
         json.dumps(_event("evt_middle"))],
    )
    result = _run_event_stage(drop, settings)
    assert result.duplicate_events_dropped == 1
    with database(settings.database_path) as connection:
        rows = connection.execute(
            "SELECT event_id, source_file, source_line FROM events "
            "ORDER BY source_file, source_line"
        ).fetchall()
        first = connection.execute(
            "SELECT source_file FROM events WHERE event_id='evt_duplicate'"
        ).fetchone()[0]
    assert [row["event_id"] for row in rows] == ["evt_duplicate", "evt_middle", "evt_last"]
    assert first == "events/events_0001.jsonl"


def test_event_stage_guards_reject_invalid_outer_state_without_writes(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_guard"
    _write_jsonl(drop / "events" / "events_0001.jsonl", [json.dumps(_event())])
    _claim_drop(settings, drop)

    with database(settings.database_path) as connection:
        with pytest.raises(EventIngestionError, match="outer database transaction"):
            _ingest_event_files(connection, drop, "drop_guard", settings, load_schemas())
    _assert_event_counters(settings, "drop_guard")

    cases = [
        ("missing_id", drop, False, "existing processing drop"),
    ]
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(EventIngestionError, match=cases[0][3]):
            _ingest_event_files(connection, cases[0][1], cases[0][0], settings, load_schemas())
    _assert_event_counters(settings, "drop_guard")

    other_path = tmp_path / "other_path"
    other_path.mkdir()
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(EventIngestionError, match="path does not match"):
            _ingest_event_files(connection, other_path, "drop_guard", settings, load_schemas())
    _assert_event_counters(settings, "drop_guard")

    complete = tmp_path / "drop_complete"
    complete.mkdir()
    _claim_drop(settings, complete, status="complete")
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(EventIngestionError, match="existing processing drop"):
            _ingest_event_files(connection, complete, "drop_complete", settings, load_schemas())
    _assert_event_counters(settings, "drop_complete")

    no_quality = tmp_path / "drop_no_quality"
    no_quality.mkdir()
    _claim_drop(settings, no_quality, quality=False)
    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(EventIngestionError, match="drop quality row"):
            _ingest_event_files(connection, no_quality, "drop_no_quality", settings, load_schemas())
    with database(settings.database_path) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM drop_quality WHERE drop_id='drop_no_quality'"
        ).fetchone()[0] == 0


def test_event_stage_leaves_commit_and_rollback_to_outer_transaction(tmp_path):
    settings = _settings(tmp_path)
    drop = tmp_path / "drop_transaction"
    _claim_drop(settings, drop)
    _write_jsonl(drop / "events" / "events_0001.jsonl", [json.dumps(_event())])
    with pytest.raises(RuntimeError, match="outer rollback"):
        with database(settings.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            result = _ingest_event_files(connection, drop, drop.name, settings, load_schemas())
            assert result.events_total == 1
            with database(settings.database_path) as reader:
                assert reader.execute("SELECT count(*) FROM events").fetchone()[0] == 0
                assert reader.execute(
                    "SELECT events_total FROM drop_quality WHERE drop_id=?", (drop.name,)
                ).fetchone()[0] == 0
            raise RuntimeError("outer rollback")
    with database(settings.database_path) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 0
        assert connection.execute(
            "SELECT events_total FROM drop_quality WHERE drop_id=?", (drop.name,)
        ).fetchone()[0] == 0


def test_event_type_migration_preserves_rows_links_indexes_uniqueness_and_triggers(tmp_path):
    settings = _settings(tmp_path)
    path = settings.database_path
    legacy_schema = db_module._SCHEMA.replace(
        "event_type TEXT NOT NULL CHECK(length(trim(event_type)) > 0),",
        "event_type TEXT NOT NULL CHECK(event_type IN ('call.initiated', 'call.telephony_failed', "
        "'call.connected', 'call.ended', 'disposition.set', 'disposition.override')),",
        1,
    )
    assert "event_type IN (" in legacy_schema
    with database(path) as connection:
        connection.executescript("BEGIN IMMEDIATE;\n" + legacy_schema)
        connection.execute(
            "INSERT INTO ingested_drops VALUES ('legacy_drop', ?, ?, 'complete', 'now')",
            (str(tmp_path), "a" * 64),
        )
        canonical = "+919876543210"
        connection.execute(
            "INSERT INTO leads (workflow_id, phone_hmac, phone_cipher, customer_name_cipher, "
            "reference_value, attributes_json, batch_id, created_drop_id, updated_drop_id, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("motor_renewal", phone_digest(canonical, TEST_HMAC_SECRET),
             encrypt_value(canonical, TEST_FERNET_KEY), encrypt_value("Name", TEST_FERNET_KEY),
             "REF", "{}", "batch_001", "legacy_drop", "legacy_drop", "now", "now"),
        )
        connection.execute(
            "INSERT INTO events (event_id, workflow_id, event_type, call_id, occurred_at_utc, "
            "timestamp_status, phone_hmac, phone_cipher, disposition_code_cipher, payload_json, "
            "source_drop_id, source_file, source_line, inserted_at) "
            "VALUES ('evt_legacy', 'motor_renewal', 'call.initiated', 'call_legacy', 'now', "
            "'valid', ?, ?, NULL, '{}', 'legacy_drop', 'events.jsonl', 1, 'now')",
            (phone_digest(canonical, TEST_HMAC_SECRET), encrypt_value(canonical, TEST_FERNET_KEY)),
        )
        lead_id = connection.execute("SELECT lead_id FROM leads").fetchone()[0]
        connection.execute(
            "INSERT INTO event_lead_links VALUES ('evt_legacy', 'motor_renewal', ?, 'phone', 'now')",
            (lead_id,),
        )

    initialize_database(path)
    with database(path) as connection:
        event = connection.execute("SELECT * FROM events WHERE event_id='evt_legacy'").fetchone()
        link = connection.execute("SELECT * FROM event_lead_links WHERE event_id='evt_legacy'").fetchone()
        indexes = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        assert event["event_type"] == "call.initiated"
        assert event["workflow_id"] == "motor_renewal"
        assert event["call_id"] == "call_legacy"
        assert event["occurred_at_utc"] == "now"
        assert event["timestamp_status"] == "valid"
        assert event["phone_hmac"] == phone_digest("+919876543210", TEST_HMAC_SECRET)
        assert decrypt_value(event["phone_cipher"], TEST_FERNET_KEY) == "+919876543210"
        assert event["disposition_code_cipher"] is None
        assert event["payload_json"] == "{}"
        assert event["source_drop_id"] == "legacy_drop"
        assert event["source_file"] == "events.jsonl"
        assert event["source_line"] == 1
        assert event["inserted_at"] == "now"
        assert link["lead_id"] == lead_id
        assert {"idx_events_workflow_call", "idx_events_workflow_phone", "idx_events_occurred_at"} <= indexes
        connection.execute(
            "INSERT INTO events (event_id, workflow_id, event_type, timestamp_status, "
            "payload_json, source_drop_id, source_file, source_line, inserted_at) "
            "VALUES ('evt_unknown_type', 'motor_renewal', 'call.future', 'invalid', '{}', "
            "'legacy_drop', 'events.jsonl', 2, 'now')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO events (event_id, workflow_id, event_type, timestamp_status, "
                "payload_json, source_drop_id, source_file, source_line, inserted_at) "
                "VALUES ('evt_unknown_type', 'motor_renewal', 'call.initiated', 'invalid', '{}', "
                "'legacy_drop', 'events.jsonl', 3, 'now')"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE events SET event_type='changed' WHERE event_id='evt_legacy'")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM events WHERE event_id='evt_legacy'")
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_event_type_migration_rolls_back_when_foreign_key_check_fails(tmp_path):
    settings = _settings(tmp_path)
    path = settings.database_path
    legacy_schema = db_module._SCHEMA.replace(
        "event_type TEXT NOT NULL CHECK(length(trim(event_type)) > 0),",
        "event_type TEXT NOT NULL CHECK(event_type IN ('call.initiated', 'call.telephony_failed', "
        "'call.connected', 'call.ended', 'disposition.set', 'disposition.override')),",
        1,
    )
    with database(path) as connection:
        connection.executescript("BEGIN IMMEDIATE;\n" + legacy_schema)
        connection.commit()
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(
            "INSERT INTO ingested_drops VALUES ('legacy_drop', ?, ?, 'complete', 'now')",
            (str(tmp_path), "a" * 64),
        )
        connection.execute(
            "INSERT INTO events (event_id, workflow_id, event_type, call_id, occurred_at_utc, "
            "timestamp_status, payload_json, source_drop_id, source_file, source_line, inserted_at) "
            "VALUES ('evt_preserved', 'motor_renewal', 'call.initiated', 'call_1', 'now', "
            "'valid', '{}', 'legacy_drop', 'events.jsonl', 1, 'now')"
        )
        connection.execute(
            "INSERT INTO event_lead_links VALUES ('dangling', 'motor_renewal', 123, 'phone', 'now')"
        )

    with pytest.raises(sqlite3.IntegrityError, match="foreign keys"):
        initialize_database(path)
    with database(path) as connection:
        ddl = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='events'"
        ).fetchone()[0]
        assert "event_type IN (" in ddl
        assert connection.execute(
            "SELECT event_id FROM events WHERE event_id='evt_preserved'"
        ).fetchone()[0] == "evt_preserved"
        assert connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='events_rebuilt'"
        ).fetchone()[0] == 0
