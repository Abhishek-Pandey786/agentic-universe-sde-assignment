"""Transactional JSONL event ingestion for Assignment A."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .normalize import NormalizationError, normalize_phone, parse_timestamp
from .schema import AssignmentSchemas
from .security import decrypt_value, encrypt_value, phone_digest


class EventIngestionError(RuntimeError):
    """Raised when event input or drop-stage preconditions are invalid."""


@dataclass(frozen=True, slots=True)
class EventIngestionStats:
    """Counters produced by one event-ingestion stage."""

    events_total: int = 0
    malformed_events: int = 0
    duplicate_events_dropped: int = 0
    unknown_disposition_events: int = 0


def _discover_event_files(drop_path: Path) -> list[Path]:
    """Return direct JSONL children in deterministic filename order."""
    event_root = drop_path / "events"
    if not event_root.exists():
        return []
    if not event_root.is_dir():
        raise EventIngestionError("drop events path must be a directory")
    return sorted(
        (path for path in event_root.glob("*.jsonl") if path.is_file()),
        key=lambda path: path.name,
    )


def _strict_json(line: str) -> Any:
    """Parse standards-compliant JSON, rejecting Python's NaN extensions."""
    def invalid_constant(_value: str) -> None:
        raise ValueError("non-standard JSON constant")

    return json.loads(line, parse_constant=invalid_constant)


def _safe_event_id(value: object) -> str | None:
    """Return a nonblank string event ID, or None for malformed IDs."""
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _claimed_path_matches(claimed: str, supplied: Path) -> bool:
    """Compare resolved paths with platform-appropriate case semantics."""
    claimed_path = Path(claimed).expanduser().resolve()
    left = os.path.normcase(os.path.normpath(str(claimed_path)))
    right = os.path.normcase(os.path.normpath(str(supplied)))
    return left == right


def _encrypt_raw_event(event: dict[str, Any], key: bytes) -> str:
    """Keep the complete source object in a valid JSON envelope as ciphertext."""
    raw_json = json.dumps(event, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    token = encrypt_value(raw_json, key).decode("ascii")
    return json.dumps({"_encrypted_event": token}, separators=(",", ":"))


def _link_by_phone(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    workflow_id: str,
    phone_hmac: str | None,
    now: str,
) -> bool:
    """Link an event to an existing lead using its workflow-scoped digest."""
    if phone_hmac is None:
        return False
    lead = connection.execute(
        "SELECT lead_id FROM leads WHERE workflow_id=? AND phone_hmac=?",
        (workflow_id, phone_hmac),
    ).fetchone()
    if lead is None:
        return False
    connection.execute(
        "INSERT INTO event_lead_links (event_id, workflow_id, lead_id, link_method, linked_at) "
        "VALUES (?, ?, ?, 'phone', ?)",
        (event_id, workflow_id, lead["lead_id"], now),
    )
    return True


def _resolve_current_overrides(
    connection: sqlite3.Connection,
    drop_id: str,
    now: str,
) -> None:
    """Resolve phone-less overrides from other linked events for the same call."""
    rows = connection.execute(
        "SELECT e.event_id, e.workflow_id, e.call_id "
        "FROM events AS e LEFT JOIN event_lead_links AS l ON l.event_id=e.event_id "
        "WHERE e.source_drop_id=? AND e.event_type='disposition.override' "
        "AND l.event_id IS NULL ORDER BY e.source_file, e.source_line, e.event_id",
        (drop_id,),
    ).fetchall()
    for row in rows:
        if not row["call_id"]:
            continue
        lead_ids = connection.execute(
            "SELECT DISTINCT l.lead_id FROM events AS other "
            "JOIN event_lead_links AS l ON l.event_id=other.event_id "
            "WHERE other.workflow_id=? AND other.call_id=? AND other.event_id<>? "
            "AND other.event_type<>'disposition.override' ORDER BY l.lead_id",
            (row["workflow_id"], row["call_id"], row["event_id"]),
        ).fetchall()
        if len(lead_ids) != 1:
            continue
        connection.execute(
            "INSERT INTO event_lead_links (event_id, workflow_id, lead_id, link_method, linked_at) "
            "VALUES (?, ?, ?, 'call_id', ?)",
            (row["event_id"], row["workflow_id"], lead_ids[0]["lead_id"], now),
        )


def _classify_current_dispositions(
    connection: sqlite3.Connection,
    drop_id: str,
    schemas: AssignmentSchemas,
    fernet_key: bytes,
    now: str,
) -> int:
    """Classify currently linked dispositions for this drop; return unknown count."""
    rows = connection.execute(
        "SELECT e.event_id, e.workflow_id, e.timestamp_status, e.disposition_code_cipher "
        "FROM events AS e JOIN event_lead_links AS l ON l.event_id=e.event_id "
        "WHERE e.source_drop_id=? AND e.event_type LIKE 'disposition.%' "
        "ORDER BY e.source_file, e.source_line, e.event_id",
        (drop_id,),
    ).fetchall()
    unknown_count = 0
    for row in rows:
        code = decrypt_value(row["disposition_code_cipher"], fernet_key)
        disposition = schemas.dispositions.get((row["workflow_id"], code))
        if disposition is None:
            state = "unknown"
            unknown_count += 1
        elif row["timestamp_status"] == "invalid":
            state = "invalid_timestamp"
        else:
            state = "known"
        connection.execute(
            "UPDATE event_disposition_state SET state=?, catalogue_workflow_id=?, classified_at=? "
            "WHERE event_id=?",
            (state, row["workflow_id"], now, row["event_id"]),
        )
    return unknown_count


def _ingest_event_files(
    connection: sqlite3.Connection,
    drop_path: str | Path,
    drop_id: str,
    settings: Settings,
    schemas: AssignmentSchemas,
) -> EventIngestionStats:
    """Ingest event JSONL files inside the caller's active drop transaction.

    The caller owns the drop claim and transaction. This stage only inserts
    immutable event rows, current-drop links/classifications, and event quality
    counters; it never opens, commits, rolls back, or closes a connection.
    It does not relink pending events from earlier drops.

    A line containing only whitespace is treated as blank and excluded from
    both ``events_total`` and ``malformed_events``.
    """
    if not connection.in_transaction:
        raise EventIngestionError("event stage requires an outer database transaction")
    path = Path(drop_path).expanduser().resolve()
    if not path.is_dir():
        raise EventIngestionError("drop path must be an existing directory")
    if not drop_id:
        raise EventIngestionError("drop ID must not be empty")
    drop = connection.execute(
        "SELECT status, source_path FROM ingested_drops WHERE drop_id=?", (drop_id,)
    ).fetchone()
    if drop is None or drop["status"] != "processing":
        raise EventIngestionError("event stage requires an existing processing drop")
    if not _claimed_path_matches(drop["source_path"], path):
        raise EventIngestionError("event stage path does not match the claimed drop path")
    quality = connection.execute(
        "SELECT drop_id FROM drop_quality WHERE drop_id=?", (drop_id,)
    ).fetchone()
    if quality is None:
        raise EventIngestionError("event stage requires an existing drop quality row")

    events_total = malformed = duplicates = 0
    files = _discover_event_files(path)
    for file_path in files:
        relative_path = file_path.relative_to(path).as_posix()
        try:
            with file_path.open("r", encoding="utf-8", newline="") as stream:
                for line_number, line in enumerate(stream, start=1):
                    if not line.strip():
                        continue
                    events_total += 1
                    try:
                        parsed = _strict_json(line)
                    except (json.JSONDecodeError, ValueError):
                        malformed += 1
                        continue
                    if not isinstance(parsed, dict):
                        malformed += 1
                        continue
                    event_id = _safe_event_id(parsed.get("event_id"))
                    if event_id is None:
                        malformed += 1
                        continue
                    if connection.execute(
                        "SELECT 1 FROM events WHERE event_id=?", (event_id,)
                    ).fetchone() is not None:
                        duplicates += 1
                        continue

                    raw_workflow_id = parsed.get("workflow_id")
                    workflow_id = (
                        raw_workflow_id.strip()
                        if isinstance(raw_workflow_id, str) and raw_workflow_id.strip()
                        else "unknown"
                    )
                    raw_event_type = parsed.get("event_type")
                    event_type = (
                        raw_event_type.strip()
                        if isinstance(raw_event_type, str) and raw_event_type.strip()
                        else "unknown"
                    )
                    raw_call_id = parsed.get("call_id")
                    call_id = raw_call_id.strip() if isinstance(raw_call_id, str) and raw_call_id.strip() else None

                    try:
                        occurred = parse_timestamp(parsed.get("occurred_at"))
                        occurred_at_utc = occurred.isoformat()
                        timestamp_status = "valid"
                    except (NormalizationError, TypeError):
                        occurred_at_utc = None
                        timestamp_status = "invalid"

                    phone_hmac: str | None = None
                    phone_cipher: bytes | None = None
                    raw_phone = parsed.get("phone")
                    if isinstance(raw_phone, str) and raw_phone.strip():
                        try:
                            canonical_phone = normalize_phone(raw_phone)
                        except NormalizationError:
                            # The encrypted source envelope retains the raw
                            # value safely; an invalid number cannot be linked.
                            pass
                        else:
                            phone_hmac = phone_digest(canonical_phone, settings.hmac_secret)
                            phone_cipher = encrypt_value(canonical_phone, settings.fernet_key)

                    is_disposition = event_type.casefold().startswith("disposition.")
                    payload = parsed.get("payload")
                    raw_code = payload.get("disposition_code") if isinstance(payload, dict) else None
                    disposition_code_cipher = (
                        encrypt_value(raw_code.strip() if isinstance(raw_code, str) else "", settings.fernet_key)
                        if is_disposition else None
                    )
                    payload_json = _encrypt_raw_event(parsed, settings.fernet_key)
                    now = datetime.now(timezone.utc).isoformat()
                    connection.execute(
                        "INSERT INTO events (event_id, workflow_id, event_type, call_id, "
                        "occurred_at_utc, timestamp_status, phone_hmac, phone_cipher, "
                        "disposition_code_cipher, payload_json, source_drop_id, source_file, "
                        "source_line, inserted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            event_id, workflow_id, event_type, call_id, occurred_at_utc,
                            timestamp_status, phone_hmac, phone_cipher,
                            disposition_code_cipher, payload_json, drop_id, relative_path,
                            line_number, now,
                        ),
                    )
                    if is_disposition:
                        connection.execute(
                            "INSERT INTO event_disposition_state "
                            "(event_id, state, catalogue_workflow_id, classified_at) "
                            "VALUES (?, 'pending_link', NULL, ?)",
                            (event_id, now),
                        )
                    _link_by_phone(
                        connection,
                        event_id=event_id,
                        workflow_id=workflow_id,
                        phone_hmac=phone_hmac,
                        now=now,
                    )
        except (OSError, UnicodeError) as exc:
            # Avoid including exception text or line content, which may hold PII.
            raise EventIngestionError(f"could not read event input {relative_path!r}") from exc

    now = datetime.now(timezone.utc).isoformat()
    _resolve_current_overrides(connection, drop_id, now)
    unknown_dispositions = _classify_current_dispositions(
        connection, drop_id, schemas, settings.fernet_key, now
    )
    updated = connection.execute(
        "UPDATE drop_quality SET events_total=events_total+?, "
        "malformed_events=malformed_events+?, "
        "duplicate_events_dropped=duplicate_events_dropped+? WHERE drop_id=?",
        (events_total, malformed, duplicates, drop_id),
    )
    if updated.rowcount != 1:
        raise EventIngestionError("could not update event quality counters")
    return EventIngestionStats(events_total, malformed, duplicates, unknown_dispositions)
