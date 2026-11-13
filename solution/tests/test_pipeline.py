"""Integration tests for the atomic shared drop orchestrator."""

from __future__ import annotations

import csv
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from solution.au_pipeline import pipeline
from solution.au_pipeline.config import Settings
from solution.au_pipeline.db import database
from solution.au_pipeline.pipeline import DropConflictError, DropIngestionError, ingest_drop
from solution.au_pipeline.schema import load_schemas


TEST_FERNET_KEY = b"MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
TEST_HMAC_SECRET = b"test-only-assignment-a-secret-32-bytes"


def _settings(tmp_path):
    return Settings(tmp_path / "pipeline.sqlite3", TEST_FERNET_KEY, TEST_HMAC_SECRET)


def _drop(tmp_path, name):
    root = tmp_path / name
    (root / "leads").mkdir(parents=True)
    (root / "events").mkdir()
    return root


def _lead(drop, phone="9876543210", *, ref="POL-1", name="Synthetic Customer"):
    path = drop / "leads" / "motor_renewal" / "batch_001.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["customer_name", "phone", "policy_number"])
        writer.writerow([name, phone, ref])
    return path


def _events(drop, *rows, file="events_0001.jsonl"):
    path = drop / "events" / file
    path.write_text("".join(
        row if isinstance(row, str) else json.dumps(row, separators=(",", ":")) + "\n"
        for row in rows
    ), encoding="utf-8")
    return path


def _event(event_id, kind="call.initiated", *, phone="9876543210", call_id="call_1",
           code=None, workflow="motor_renewal"):
    result = {
        "event_id": event_id,
        "event_type": kind,
        "workflow_id": workflow,
        "call_id": call_id,
        "occurred_at": "2026-10-05T13:20:21Z",
        "payload": {} if code is None else {"disposition_code": code},
    }
    if phone is not None:
        result["phone"] = phone
    return result


def _rows(settings, sql, params=()):
    with database(settings.database_path) as connection:
        return connection.execute(sql, params).fetchall()


def test_full_drop_completes_after_both_stages_and_relinking(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)
    _events(drop, _event("evt_call"), _event("evt_disp", "disposition.set", code="renewed"))

    result = ingest_drop(drop, settings, load_schemas())

    assert result.drop_id == "drop_1"
    assert result.already_ingested is False
    assert result.quality["lead_rows_total"] == 1
    assert result.quality["events_total"] == 2
    assert result.quality["orphan_events"] == 0
    assert result.quality["unknown_disposition_events"] == 0
    drop_row = _rows(settings, "SELECT status FROM ingested_drops WHERE drop_id='drop_1'")[0]
    assert drop_row["status"] == "complete"
    assert _rows(settings, "SELECT count(*) AS n FROM event_lead_links")[0]["n"] == 2


def test_identical_completed_drop_is_a_noop(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)
    _events(drop, _event("evt_1"))

    first = ingest_drop(drop, settings, load_schemas())
    second = ingest_drop(drop, settings, load_schemas())

    assert first.already_ingested is False
    assert second.already_ingested is True
    assert _rows(settings, "SELECT count(*) AS n FROM leads")[0]["n"] == 1
    assert _rows(settings, "SELECT count(*) AS n FROM events")[0]["n"] == 1
    assert _rows(settings, "SELECT count(*) AS n FROM ingested_drops")[0]["n"] == 1


def test_changed_content_for_same_drop_id_fails_without_changes(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)
    _events(drop)
    ingest_drop(drop, settings, load_schemas())
    _events(drop, _event("evt_new"))

    with pytest.raises(DropConflictError, match="different content"):
        ingest_drop(drop, settings, load_schemas())
    assert _rows(settings, "SELECT count(*) AS n FROM leads")[0]["n"] == 1
    assert _rows(settings, "SELECT count(*) AS n FROM events")[0]["n"] == 0
    assert _rows(settings, "SELECT status FROM ingested_drops")[0]["status"] == "complete"


@pytest.mark.parametrize("stage_name", ["lead", "event"])
def test_stage_failure_rolls_back_entire_drop(tmp_path, monkeypatch, stage_name):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)
    _events(drop, _event("evt_1"))

    target = "_ingest_lead_files" if stage_name == "lead" else "_ingest_event_files"

    def fail(*args, **kwargs):
        raise RuntimeError("synthetic stage failure")

    monkeypatch.setattr(pipeline, target, fail)
    with pytest.raises(RuntimeError, match="synthetic stage failure"):
        ingest_drop(drop, settings, load_schemas())

    assert _rows(settings, "SELECT count(*) AS n FROM ingested_drops")[0]["n"] == 0
    assert _rows(settings, "SELECT count(*) AS n FROM drop_quality")[0]["n"] == 0
    assert _rows(settings, "SELECT count(*) AS n FROM leads")[0]["n"] == 0
    assert _rows(settings, "SELECT count(*) AS n FROM events")[0]["n"] == 0


def test_orphan_phone_event_links_after_lead_arrives_in_later_drop(tmp_path):
    settings = _settings(tmp_path)
    schemas = load_schemas()
    first = _drop(tmp_path, "drop_1")
    _events(first, _event("evt_orphan"))
    first_result = ingest_drop(first, settings, schemas)
    assert first_result.quality["orphan_events"] == 1

    second = _drop(tmp_path, "drop_2")
    _lead(second)
    second_result = ingest_drop(second, settings, schemas)

    assert second_result.quality["orphan_events"] == 0
    assert _rows(settings, "SELECT link_method FROM event_lead_links")[0]["link_method"] == "phone"


def test_overrides_resolve_across_drops_by_call_id(tmp_path):
    settings = _settings(tmp_path)
    schemas = load_schemas()
    first = _drop(tmp_path, "drop_1")
    _events(
        first,
        _event("evt_call", "call.initiated", phone="9876543210"),
        _event("evt_override", "disposition.override", phone=None, code="renewed"),
    )
    assert ingest_drop(first, settings, schemas).quality["orphan_events"] == 2

    second = _drop(tmp_path, "drop_2")
    _lead(second)
    result = ingest_drop(second, settings, schemas)

    links = _rows(
        settings,
        "SELECT event_id, link_method FROM event_lead_links ORDER BY event_id",
    )
    assert [(row["event_id"], row["link_method"]) for row in links] == [
        ("evt_call", "phone"), ("evt_override", "call_id")
    ]
    assert result.quality["orphan_events"] == 0
    assert result.quality["unknown_disposition_events"] == 0


def test_override_remains_pending_until_call_event_links(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _events(
        drop,
        _event("evt_call", "call.initiated", phone=None),
        _event("evt_override", "disposition.override", phone=None, code="renewed"),
    )
    result = ingest_drop(drop, settings, load_schemas())

    assert result.quality["orphan_events"] == 2
    states = _rows(settings, "SELECT state FROM event_disposition_state ORDER BY event_id")
    assert [row["state"] for row in states] == ["pending_link"]


def test_unknown_disposition_count_starts_only_after_linking(tmp_path):
    settings = _settings(tmp_path)
    schemas = load_schemas()
    first = _drop(tmp_path, "drop_1")
    _events(first, _event("evt_unknown", "disposition.set", code="not_in_catalogue"))
    result1 = ingest_drop(first, settings, schemas)
    assert result1.quality["unknown_disposition_events"] == 0
    assert result1.quality["orphan_events"] == 1

    second = _drop(tmp_path, "drop_2")
    _lead(second)
    result2 = ingest_drop(second, settings, schemas)
    assert result2.quality["unknown_disposition_events"] == 1
    assert result2.quality["orphan_events"] == 0


def test_final_orphan_and_unknown_counts_are_mutually_exclusive(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)
    _events(
        drop,
        _event("evt_unknown_linked", "disposition.set", code="no_such_code"),
        _event("evt_orphan", "call.initiated", phone="6789012345", call_id="other"),
    )

    result = ingest_drop(drop, settings, load_schemas())

    assert result.quality["orphan_events"] == 1
    assert result.quality["unknown_disposition_events"] == 1


def test_relinking_is_deterministic_and_safe_to_repeat(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)
    _events(drop, _event("evt_1", "disposition.set", code="renewed"))
    ingest_drop(drop, settings, load_schemas())

    with database(settings.database_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        before = tuple(connection.execute(
            "SELECT state, catalogue_workflow_id, classified_at "
            "FROM event_disposition_state WHERE event_id='evt_1'"
        ).fetchone())
        link_count = connection.execute("SELECT count(*) FROM event_lead_links").fetchone()[0]
        pipeline._relink_and_classify(connection, load_schemas(), settings)
        pipeline._relink_and_classify(connection, load_schemas(), settings)
        after = tuple(connection.execute(
            "SELECT state, catalogue_workflow_id, classified_at "
            "FROM event_disposition_state WHERE event_id='evt_1'"
        ).fetchone())
        assert after == before
        assert connection.execute("SELECT count(*) FROM event_lead_links").fetchone()[0] == link_count


def test_input_change_during_staging_is_detected_and_rolled_back(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)
    event_file = _events(drop, _event("evt_1"))
    original = pipeline._ingest_event_files

    def mutate_after_stage(*args, **kwargs):
        result = original(*args, **kwargs)
        event_file.write_text(json.dumps(_event("evt_changed")) + "\n", encoding="utf-8")
        return result

    monkeypatch.setattr(pipeline, "_ingest_event_files", mutate_after_stage)
    with pytest.raises(DropIngestionError, match="changed during ingestion"):
        ingest_drop(drop, settings, load_schemas())
    assert _rows(settings, "SELECT count(*) AS n FROM ingested_drops")[0]["n"] == 0
    assert _rows(settings, "SELECT count(*) AS n FROM leads")[0]["n"] == 0
    assert _rows(settings, "SELECT count(*) AS n FROM events")[0]["n"] == 0


def test_malformed_drop_structure_is_rejected_before_claim(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    (drop / "events" / "unexpected.txt").write_text("x", encoding="utf-8")

    with pytest.raises(DropIngestionError, match="only JSONL"):
        ingest_drop(drop, settings, load_schemas())
    assert not settings.database_path.exists()


def test_concurrent_same_drop_attempts_produce_one_ingestion(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)
    _events(drop, _event("evt_1"))

    def run():
        return ingest_drop(drop, settings, load_schemas())

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _n: run(), range(2)))

    assert sorted(result.already_ingested for result in results) == [False, True]
    assert _rows(settings, "SELECT count(*) AS n FROM leads")[0]["n"] == 1
    assert _rows(settings, "SELECT count(*) AS n FROM events")[0]["n"] == 1


def test_quality_snapshot_groups_rejections_by_reason(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    csv_path = drop / "leads" / "motor_renewal" / "batch_001.csv"
    csv_path.parent.mkdir(parents=True)
    with csv_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["customer_name", "phone", "policy_number"])
        writer.writerow(["Synthetic Customer", "123", "POL-1"])
        writer.writerow(["", "9876543210", "POL-2"])
        writer.writerow(["Synthetic Customer", "9876543210", ""])

    result = ingest_drop(drop, settings, load_schemas())

    assert result.quality["rejections_by_reason"] == {
        "invalid_phone": 1,
        "missing_required:customer_name": 1,
        "missing_required:policy_number": 1,
    }
    assert result.quality["lead_rows_rejected"] == 3


def test_quality_snapshot_returns_empty_rejection_mapping(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    result = ingest_drop(drop, settings, load_schemas())

    assert result.quality["rejections_by_reason"] == {}


def test_repeated_quality_snapshot_is_identical(tmp_path):
    settings = _settings(tmp_path)
    drop = _drop(tmp_path, "drop_1")
    csv_path = drop / "leads" / "motor_renewal" / "batch_001.csv"
    csv_path.parent.mkdir(parents=True)
    with csv_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["customer_name", "phone", "policy_number"])
        writer.writerow(["", "9876543210", "POL-1"])

    first = ingest_drop(drop, settings, load_schemas())
    second = ingest_drop(drop, settings, load_schemas())

    assert second.already_ingested is True
    assert second.quality == first.quality
