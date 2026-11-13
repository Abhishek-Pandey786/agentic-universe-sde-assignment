"""Read-only generation of Assignment A CSV and JSON reports."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from .config import Settings
from .db import database
from .normalize import IST
from .schema import AssignmentSchemas, Disposition
from .security import decrypt_value


class ReportError(RuntimeError):
    """Raised when reports cannot be safely read or written."""


@dataclass(slots=True)
class _Call:
    """Report-only aggregate for one workflow and lead scoped call."""

    workflow_id: str
    lead_id: int
    call_id: str | None
    fallback_id: str
    initiated: list[tuple[datetime, str]] = field(default_factory=list)
    connected: bool = False
    telephony_failed: bool = False
    overrides: list[tuple[datetime, str, str, Disposition | None]] = field(default_factory=list)
    sets: list[tuple[datetime, str, str, Disposition | None]] = field(default_factory=list)

    @property
    def start(self) -> tuple[datetime, str] | None:
        """Use the earliest initiation for the distinct call's reporting date."""
        return min(self.initiated) if self.initiated else None

    @property
    def effective_disposition(self) -> tuple[str, Disposition] | None:
        """Select the winning event before checking whether its code is known.

        Any override takes precedence over every set. Within the selected event
        type, latest timestamp wins; event_id ascending is the stable tie order,
        so the lexicographically greatest ID breaks ties. An unknown winning
        code produces no effective disposition and never falls back.
        """
        candidates = self.overrides if self.overrides else self.sets
        if not candidates:
            return None
        _, _, code, disposition = max(candidates, key=lambda item: (item[0], item[1]))
        if disposition is None:
            return None
        return code, disposition


def _event_datetime(value: str | None) -> datetime | None:
    """Parse a stored UTC timestamp, returning None for invalid values."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def _call_bucket(calls: list[_Call]) -> str:
    """Apply the assignment's exact lead bucket precedence."""
    if not calls:
        return "yet_to_reach_out"
    if any(
        (effective := call.effective_disposition) is not None
        and effective[1].is_terminal
        for call in calls
    ):
        return "completed"
    if all(call.telephony_failed for call in calls):
        return "in_progress_telephony_failed"
    return "in_progress_dialed"


def _csv_text(rows: list[list[object]]) -> str:
    """Serialize CSV rows with stable newline behavior."""
    from io import StringIO

    stream = StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerows(rows)
    return stream.getvalue()


def _read_report_data(
    connection: sqlite3.Connection,
    schemas: AssignmentSchemas,
    settings: Settings,
) -> tuple[list[dict[str, object]], dict[tuple[int, str], list[_Call]], dict[str, object]]:
    """Read leads, linked events, calls, and final quality under one snapshot."""
    lead_rows = connection.execute(
        "SELECT lead_id, workflow_id, phone_cipher, reference_value, batch_id "
        "FROM leads ORDER BY workflow_id, reference_value, lead_id"
    ).fetchall()
    leads: list[dict[str, object]] = []
    calls_by_lead: dict[tuple[int, str], dict[tuple[int, str], _Call]] = defaultdict(dict)
    for row in lead_rows:
        workflow = row["workflow_id"]
        workflow_schema = schemas.workflows.get(workflow)
        if workflow_schema is None:
            raise ReportError("database contains a lead without a supplied workflow schema")
        reference_fields = [field for field in workflow_schema.fields if field.reference]
        if len(reference_fields) != 1:
            raise ReportError("workflow schema must define exactly one reference field")
        try:
            phone = decrypt_value(row["phone_cipher"], settings.fernet_key)
        except (TypeError, ValueError) as exc:
            raise ReportError("could not safely read reporting data") from exc
        if not phone.startswith("+91") or len(phone) != 13 or not phone[-4:].isdigit():
            raise ReportError("database contains a lead with an invalid encrypted phone")
        leads.append({
            "lead_id": row["lead_id"],
            "workflow_id": workflow,
            "external_ref": row["reference_value"],
            "batch_id": row["batch_id"],
            "phone_masked": "+91******" + phone[-4:],
        })

    event_rows = connection.execute(
        "SELECT e.event_id, e.workflow_id, e.event_type, e.call_id, "
        "e.occurred_at_utc, e.timestamp_status, e.disposition_code_cipher, "
        "l.lead_id "
        "FROM events AS e JOIN event_lead_links AS l "
        "ON l.event_id=e.event_id AND l.workflow_id=e.workflow_id "
        "ORDER BY e.workflow_id, l.lead_id, e.call_id, e.occurred_at_utc, e.event_id"
    ).fetchall()

    supported = {
        "call.initiated", "call.connected", "call.telephony_failed",
        "call.ended", "disposition.set", "disposition.override",
    }
    for event in event_rows:
        event_type = event["event_type"]
        if event_type not in supported or event["timestamp_status"] != "valid":
            continue
        occurred = _event_datetime(event["occurred_at_utc"])
        if occurred is None:
            continue
        call_id = event["call_id"]
        lead_id = event["lead_id"]
        if call_id is None or not str(call_id).strip():
            if event_type != "call.initiated":
                continue
            fallback_id = event["event_id"]
            key_id = "\x00event:" + fallback_id
            key_call_id: str | None = None
        else:
            key_call_id = str(call_id)
            fallback_id = ""
            key_id = key_call_id
        group_key = (lead_id, key_id)
        workflow_key = (lead_id, event["workflow_id"])
        call = calls_by_lead[workflow_key].get(group_key)
        if call is None:
            call = _Call(event["workflow_id"], lead_id, key_call_id, fallback_id)
            calls_by_lead[workflow_key][group_key] = call

        if event_type == "call.initiated":
            call.initiated.append((occurred, event["event_id"]))
        elif event_type == "call.connected":
            call.connected = True
        elif event_type == "call.telephony_failed":
            call.telephony_failed = True
        elif event_type in {"disposition.set", "disposition.override"}:
            try:
                code = decrypt_value(event["disposition_code_cipher"], settings.fernet_key)
            except (TypeError, ValueError) as exc:
                raise ReportError("could not safely read reporting data") from exc
            disposition = schemas.dispositions.get((event["workflow_id"], code))
            candidate = (occurred, event["event_id"], code, disposition)
            if event_type == "disposition.override":
                call.overrides.append(candidate)
            else:
                call.sets.append(candidate)

    report_calls: dict[tuple[int, str], list[_Call]] = {}
    for (lead_id, workflow), grouped in calls_by_lead.items():
        calls = [call for call in grouped.values() if call.start is not None]
        report_calls[(lead_id, workflow)] = calls

    quality_row = connection.execute(
        "SELECT "
        "COALESCE(SUM(lead_rows_total), 0) AS lead_rows_total, "
        "COALESCE(SUM(lead_rows_rejected), 0) AS lead_rows_rejected, "
        "COALESCE(SUM(duplicate_lead_rows_merged), 0) AS duplicate_lead_rows_merged, "
        "COALESCE(SUM(field_warnings), 0) AS field_warnings, "
        "COALESCE(SUM(events_total), 0) AS events_total, "
        "COALESCE(SUM(malformed_events), 0) AS malformed_events, "
        "COALESCE(SUM(duplicate_events_dropped), 0) AS duplicate_events_dropped "
        "FROM drop_quality"
    ).fetchone()
    rejection_rows = connection.execute(
        "SELECT reason, COUNT(*) AS count FROM lead_rejections "
        "GROUP BY reason ORDER BY reason"
    ).fetchall()
    orphan_count = connection.execute(
        "SELECT COUNT(*) FROM events AS e LEFT JOIN event_lead_links AS l "
        "ON l.event_id=e.event_id WHERE l.event_id IS NULL"
    ).fetchone()[0]
    unknown_count = connection.execute(
        "SELECT COUNT(*) FROM events AS e JOIN event_lead_links AS l ON l.event_id=e.event_id "
        "JOIN event_disposition_state AS s ON s.event_id=e.event_id "
        "WHERE s.state='unknown'"
    ).fetchone()[0]
    quality: dict[str, object] = {
        "lead_rows_total": quality_row["lead_rows_total"],
        "lead_rows_rejected": quality_row["lead_rows_rejected"],
        "rejections_by_reason": {row["reason"]: row["count"] for row in rejection_rows},
        "duplicate_lead_rows_merged": quality_row["duplicate_lead_rows_merged"],
        "field_warnings": quality_row["field_warnings"],
        "events_total": quality_row["events_total"],
        "malformed_events": quality_row["malformed_events"],
        "duplicate_events_dropped": quality_row["duplicate_events_dropped"],
        "orphan_events": orphan_count,
        "unknown_disposition_events": unknown_count,
    }
    return leads, report_calls, quality


def _render_reports(
    leads: list[dict[str, object]],
    calls_by_lead: dict[tuple[int, str], list[_Call]],
    quality: dict[str, object],
    rejections: list[sqlite3.Row],
) -> dict[str, str]:
    """Render all report contents without touching the database."""
    bucket_counts: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {
            "yet_to_reach_out": 0,
            "in_progress_telephony_failed": 0,
            "in_progress_dialed": 0,
            "completed": 0,
        }
    )
    bucket_by_lead: dict[int, str] = {}
    for lead in leads:
        lead_id = int(lead["lead_id"])
        workflow = str(lead["workflow_id"])
        calls = calls_by_lead.get((lead_id, workflow), [])
        bucket = _call_bucket(calls)
        bucket_by_lead[lead_id] = bucket
        key = (workflow, str(lead["batch_id"]))
        bucket_counts[key][bucket] += 1

    batch_rows: list[list[object]] = [[
        "workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
        "in_progress_telephony_failed", "in_progress_dialed", "completed",
    ]]
    for (workflow, batch), counts in sorted(bucket_counts.items()):
        batch_rows.append([
            workflow, batch, sum(counts.values()), counts["yet_to_reach_out"],
            counts["in_progress_telephony_failed"], counts["in_progress_dialed"],
            counts["completed"],
        ])

    daily: dict[tuple[str, date], dict[str, object]] = {}
    for lead in leads:
        workflow = str(lead["workflow_id"])
        lead_id = int(lead["lead_id"])
        for call in calls_by_lead.get((lead_id, workflow), []):
            start = call.start
            if start is None:
                continue
            report_date = start[0].astimezone(IST).date()
            key = (workflow, report_date)
            values = daily.setdefault(key, {"calls": set(), "connected": set(), "success_leads": set()})
            call_key = call.call_id if call.call_id is not None else call.fallback_id
            values["calls"].add((lead_id, call_key))
            if call.connected:
                values["connected"].add((lead_id, call_key))
            effective = call.effective_disposition
            if effective is not None and effective[1].is_success:
                values["success_leads"].add(lead_id)

    daily_rows: list[list[object]] = [[
        "workflow_id", "date", "total_calls", "connected_calls",
        "connectivity_rate", "success_leads",
    ]]
    for (workflow, report_date), values in sorted(daily.items()):
        total = len(values["calls"])
        connected = len(values["connected"])
        rate = (Decimal(connected) / Decimal(total)).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )
        daily_rows.append([
            workflow, report_date.isoformat(), total, connected,
            f"{rate:.4f}", len(values["success_leads"]),
        ])

    lead_rows: list[list[object]] = [[
        "workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
        "total_calls", "connected_calls", "last_call_at", "current_disposition",
        "ever_success",
    ]]
    ordered_leads = sorted(leads, key=lambda lead: (str(lead["workflow_id"]), str(lead["external_ref"])))
    for lead in ordered_leads:
        lead_id = int(lead["lead_id"])
        workflow = str(lead["workflow_id"])
        calls = sorted(
            calls_by_lead.get((lead_id, workflow), []),
            key=lambda call: (call.start[0], call.call_id or call.fallback_id, call.start[1]),
        )
        starts = [call.start for call in calls if call.start is not None]
        last_at = max(starts, key=lambda item: (item[0], item[1]))[0].astimezone(IST).isoformat() if starts else ""
        candidates = [
            (call.start, call.call_id or call.fallback_id, call.effective_disposition)
            for call in calls if call.start is not None and call.effective_disposition is not None
        ]
        current = max(candidates, key=lambda item: (item[0][0], item[1], item[0][1]))[2][0] if candidates else ""
        ever_success = any(
            call.effective_disposition is not None and call.effective_disposition[1].is_success
            for call in calls
        )
        lead_rows.append([
            workflow, lead["external_ref"], lead["batch_id"], lead["phone_masked"],
            bucket_by_lead[lead_id], len(calls), sum(call.connected for call in calls),
            last_at, current, "true" if ever_success else "false",
        ])

    rejection_rows_data: list[list[object]] = [["workflow_id", "batch_id", "row_number", "reason"]]
    for row in rejections:
        rejection_rows_data.append([
            row["workflow_id"], row["batch_id"], row["row_number"], row["reason"]
        ])

    return {
        "batch_summary.csv": _csv_text(batch_rows),
        "daily_overview.csv": _csv_text(daily_rows),
        "lead_report.csv": _csv_text(lead_rows),
        "rejections.csv": _csv_text(rejection_rows_data),
        "data_quality.json": json.dumps(quality, ensure_ascii=False, indent=2) + "\n",
    }


def generate_reports(
    output_dir: str | Path,
    settings: Settings,
    schemas: AssignmentSchemas,
) -> None:
    """Generate the five reports from one read-only SQLite snapshot."""
    database_path = Path(settings.database_path).expanduser()
    if not database_path.is_file():
        raise ReportError("report database does not exist")

    try:
        with database(database_path) as connection:
            connection.execute("BEGIN")
            leads, calls_by_lead, quality = _read_report_data(connection, schemas, settings)
            rejections = connection.execute(
                "SELECT workflow_id, batch_id, row_number, reason "
                "FROM lead_rejections "
                "ORDER BY workflow_id, batch_id, row_number, reason, drop_id"
            ).fetchall()
            report_text = _render_reports(leads, calls_by_lead, quality, rejections)
    except ReportError:
        raise
    except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
        raise ReportError("could not safely read reporting data") from exc

    destination = Path(output_dir).expanduser()
    try:
        destination.mkdir(parents=True, exist_ok=True)
        for name, contents in report_text.items():
            (destination / name).write_text(contents, encoding="utf-8", newline="")
    except OSError as exc:
        raise ReportError("could not write report files") from exc
