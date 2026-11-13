"""Atomic drop orchestration for Assignment A."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings
from .db import database, initialize_database
from .events import EventIngestionError, _ingest_event_files
from .leads import LeadIngestionError, _ingest_lead_files
from .schema import AssignmentSchemas
from .security import decrypt_value


class DropIngestionError(RuntimeError):
    """Raised when a drop cannot be safely validated or ingested."""


class DropConflictError(DropIngestionError):
    """Raised when an already-used drop ID is presented with different content."""


@dataclass(frozen=True, slots=True)
class DropIngestionResult:
    """Outcome of a drop ingestion and a final database-wide quality snapshot."""

    drop_id: str
    already_ingested: bool
    quality: dict[str, int | dict[str, int]]


def _validate_drop(path: Path, schemas: AssignmentSchemas) -> None:
    """Validate the complete input tree before any drop data is persisted."""
    if not path.is_dir():
        raise DropIngestionError("drop path must be an existing directory")
    if not path.name.strip():
        raise DropIngestionError("drop directory must have a non-empty name")

    try:
        children = {child.name: child for child in path.iterdir()}
    except OSError as exc:
        raise DropIngestionError("could not inspect drop directory") from exc
    if set(children) != {"leads", "events"}:
        raise DropIngestionError("drop must contain exactly leads/ and events/ directories")
    if any(not item.is_dir() or item.is_symlink() for item in children.values()):
        raise DropIngestionError("drop leads/ and events/ entries must be real directories")

    leads_root = children["leads"]
    try:
        for workflow_dir in leads_root.iterdir():
            if workflow_dir.is_symlink() or not workflow_dir.is_dir():
                raise DropIngestionError("lead input must use workflow subdirectories")
            if workflow_dir.name not in schemas.workflows:
                raise DropIngestionError("lead input contains an unknown workflow directory")
            for item in workflow_dir.iterdir():
                if item.is_symlink() or not item.is_file() or item.suffix != ".csv":
                    raise DropIngestionError("lead workflow directories may contain only CSV files")
        events_root = children["events"]
        for item in events_root.iterdir():
            if item.is_symlink() or not item.is_file() or item.suffix != ".jsonl":
                raise DropIngestionError("events/ may contain only JSONL files")
    except OSError as exc:
        raise DropIngestionError("could not inspect drop contents") from exc


def _content_sha256(path: Path) -> str:
    """Hash every file using sorted relative paths and length-delimited content."""
    digest = hashlib.sha256()
    try:
        files = sorted(
            (item for item in path.rglob("*") if item.is_file()),
            key=lambda item: item.relative_to(path).as_posix(),
        )
        for item in files:
            relative = item.relative_to(path).as_posix().encode("utf-8")
            digest.update(len(relative).to_bytes(8, "big"))
            digest.update(relative)
            size = item.stat().st_size
            digest.update(size.to_bytes(8, "big"))
            with item.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
    except OSError as exc:
        raise DropIngestionError("could not read complete drop contents") from exc
    return digest.hexdigest()


def _normalized_path(path: str | Path) -> str:
    """Normalize an absolute path consistently on Windows and Linux."""
    resolved = Path(path).expanduser().resolve()
    return os.path.normcase(os.path.normpath(str(resolved)))


def _relink_and_classify(
    connection: sqlite3.Connection,
    schemas: AssignmentSchemas,
    settings: Settings,
) -> None:
    """Deterministically relink all retained events, then classify dispositions."""
    now = datetime.now(timezone.utc).isoformat()

    # Overrides are always associated through call_id, even if a nonconforming
    # source line also included a phone.
    connection.execute(
        "DELETE FROM event_lead_links WHERE link_method='phone' "
        "AND event_id IN (SELECT event_id FROM events WHERE event_type='disposition.override')"
    )

    unresolved_phone_events = connection.execute(
        "SELECT e.event_id, e.workflow_id, e.phone_hmac FROM events AS e "
        "LEFT JOIN event_lead_links AS l ON l.event_id=e.event_id "
        "WHERE l.event_id IS NULL AND e.phone_hmac IS NOT NULL "
        "AND e.event_type<>'disposition.override' "
        "ORDER BY (SELECT ingested_at FROM ingested_drops d WHERE d.drop_id=e.source_drop_id), "
        "e.source_drop_id, e.source_file, e.source_line, e.event_id"
    ).fetchall()
    for event in unresolved_phone_events:
        lead = connection.execute(
            "SELECT lead_id FROM leads WHERE workflow_id=? AND phone_hmac=?",
            (event["workflow_id"], event["phone_hmac"]),
        ).fetchone()
        if lead is not None:
            connection.execute(
                "INSERT INTO event_lead_links "
                "(event_id, workflow_id, lead_id, link_method, linked_at) "
                "VALUES (?, ?, ?, 'phone', ?)",
                (event["event_id"], event["workflow_id"], lead["lead_id"], now),
            )

    unresolved_overrides = connection.execute(
        "SELECT e.event_id, e.workflow_id, e.call_id FROM events AS e "
        "LEFT JOIN event_lead_links AS l ON l.event_id=e.event_id "
        "WHERE e.event_type='disposition.override' AND l.event_id IS NULL "
        "AND e.call_id IS NOT NULL "
        "ORDER BY (SELECT ingested_at FROM ingested_drops d WHERE d.drop_id=e.source_drop_id), "
        "e.source_drop_id, e.source_file, e.source_line, e.event_id"
    ).fetchall()
    for override in unresolved_overrides:
        if not override["call_id"]:
            continue
        linked_leads = connection.execute(
            "SELECT DISTINCT l.lead_id FROM events AS call_event "
            "JOIN event_lead_links AS l ON l.event_id=call_event.event_id "
            "WHERE call_event.workflow_id=? AND call_event.call_id=? "
            "AND call_event.event_id<>? AND call_event.event_type<>'disposition.override' "
            "ORDER BY l.lead_id",
            (override["workflow_id"], override["call_id"], override["event_id"]),
        ).fetchall()
        if len(linked_leads) == 1:
            connection.execute(
                "INSERT INTO event_lead_links "
                "(event_id, workflow_id, lead_id, link_method, linked_at) "
                "VALUES (?, ?, ?, 'call_id', ?)",
                (override["event_id"], override["workflow_id"], linked_leads[0]["lead_id"], now),
            )

    # Ensure every disposition has mutable state, including any retained legacy
    # event rows, then resolve state only for linked events.
    connection.execute(
        "INSERT OR IGNORE INTO event_disposition_state "
        "(event_id, state, catalogue_workflow_id, classified_at) "
        "SELECT event_id, 'pending_link', NULL, ? FROM events "
        "WHERE event_type LIKE 'disposition.%'",
        (now,),
    )
    dispositions = connection.execute(
        "SELECT e.event_id, e.workflow_id, e.timestamp_status, "
        "e.disposition_code_cipher, s.state, s.catalogue_workflow_id "
        "FROM events AS e JOIN event_lead_links AS l ON l.event_id=e.event_id "
        "JOIN event_disposition_state AS s ON s.event_id=e.event_id "
        "WHERE e.event_type LIKE 'disposition.%' "
        "ORDER BY e.source_drop_id, e.source_file, e.source_line, e.event_id"
    ).fetchall()
    for event in dispositions:
        code = decrypt_value(event["disposition_code_cipher"], settings.fernet_key)
        is_unknown = (event["workflow_id"], code) not in schemas.dispositions
        target_state = (
            "unknown" if is_unknown
            else "invalid_timestamp" if event["timestamp_status"] == "invalid"
            else "known"
        )
        if event["state"] != target_state or event["catalogue_workflow_id"] != event["workflow_id"]:
            connection.execute(
                "UPDATE event_disposition_state SET state=?, catalogue_workflow_id=?, "
                "classified_at=? WHERE event_id=?",
                (target_state, event["workflow_id"], now, event["event_id"]),
            )


def _quality_snapshot(connection: sqlite3.Connection) -> dict[str, int | dict[str, int]]:
    """Derive final cumulative quality fields without mutating per-drop counters."""
    counters = connection.execute(
        "SELECT "
        "COALESCE(SUM(lead_rows_total), 0), "
        "COALESCE(SUM(lead_rows_rejected), 0), "
        "COALESCE(SUM(duplicate_lead_rows_merged), 0), "
        "COALESCE(SUM(field_warnings), 0), "
        "COALESCE(SUM(events_total), 0), "
        "COALESCE(SUM(malformed_events), 0), "
        "COALESCE(SUM(duplicate_events_dropped), 0) "
        "FROM drop_quality"
    ).fetchone()
    orphans = connection.execute(
        "SELECT COUNT(*) FROM events AS e LEFT JOIN event_lead_links AS l "
        "ON l.event_id=e.event_id WHERE l.event_id IS NULL"
    ).fetchone()[0]
    unknown = connection.execute(
        "SELECT COUNT(*) FROM events AS e JOIN event_lead_links AS l ON l.event_id=e.event_id "
        "JOIN event_disposition_state AS s ON s.event_id=e.event_id WHERE s.state='unknown'"
    ).fetchone()[0]
    rejection_rows = connection.execute(
        "SELECT reason, COUNT(*) FROM lead_rejections GROUP BY reason ORDER BY reason"
    ).fetchall()
    return {
        "lead_rows_total": counters[0],
        "lead_rows_rejected": counters[1],
        "rejections_by_reason": {row[0]: row[1] for row in rejection_rows},
        "duplicate_lead_rows_merged": counters[2],
        "field_warnings": counters[3],
        "events_total": counters[4],
        "malformed_events": counters[5],
        "duplicate_events_dropped": counters[6],
        "orphan_events": orphans,
        "unknown_disposition_events": unknown,
    }


def ingest_drop(
    drop_path: str | Path,
    settings: Settings,
    schemas: AssignmentSchemas,
) -> DropIngestionResult:
    """Ingest one complete drop atomically; this function owns its lifecycle."""
    path = Path(drop_path).expanduser().resolve()
    _validate_drop(path, schemas)
    drop_id = path.name
    content_hash = _content_sha256(path)
    source_path = _normalized_path(path)

    # Schema initialization is idempotent and separate from the data transaction.
    initialize_database(settings.database_path)
    try:
        with database(settings.database_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT content_sha256, status FROM ingested_drops WHERE drop_id=?",
                (drop_id,),
            ).fetchone()
            if existing is not None:
                if existing["content_sha256"] != content_hash:
                    raise DropConflictError("drop ID already exists with different content")
                if existing["status"] != "complete":
                    raise DropIngestionError("drop ID exists without a completed ingestion")
                return DropIngestionResult(drop_id, True, _quality_snapshot(connection))

            connection.execute(
                "INSERT INTO ingested_drops "
                "(drop_id, source_path, content_sha256, status, ingested_at) "
                "VALUES (?, ?, ?, 'processing', ?)",
                (drop_id, source_path, content_hash, datetime.now(timezone.utc).isoformat()),
            )
            connection.execute("INSERT INTO drop_quality (drop_id) VALUES (?)", (drop_id,))

            _ingest_lead_files(connection, path, drop_id, settings, schemas)
            _ingest_event_files(connection, path, drop_id, settings, schemas)
            _relink_and_classify(connection, schemas, settings)

            _validate_drop(path, schemas)
            if _content_sha256(path) != content_hash:
                raise DropIngestionError("drop contents changed during ingestion")

            quality = _quality_snapshot(connection)
            updated = connection.execute(
                "UPDATE ingested_drops SET status='complete' WHERE drop_id=? AND status='processing'",
                (drop_id,),
            )
            if updated.rowcount != 1:
                raise DropIngestionError("could not complete drop")
        return DropIngestionResult(drop_id, False, quality)
    except (LeadIngestionError, EventIngestionError) as exc:
        # Stage exceptions are already deliberately PII-safe; don't add source
        # record content or parser details to the public error.
        raise DropIngestionError(str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        # Keep database values and input contents out of integrity diagnostics.
        raise DropIngestionError("drop could not be committed because of a database constraint") from exc
