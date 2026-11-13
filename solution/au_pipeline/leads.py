"""Transactional CSV lead ingestion for Assignment A."""

from __future__ import annotations

import csv
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings
from .normalize import NormalizationError, normalize_date, normalize_decimal, normalize_enum, normalize_phone
from .schema import AssignmentSchemas, FieldSpec, WorkflowSchema, map_headers
from .security import encrypt_value, phone_digest


class LeadIngestionError(RuntimeError):
    """Raised when lead input or the configured schemas cannot be ingested safely."""


@dataclass(frozen=True, slots=True)
class LeadIngestionStats:
    """Lead-stage counters returned to the owning drop pipeline."""

    lead_rows_total: int = 0
    lead_rows_rejected: int = 0
    duplicate_lead_rows_merged: int = 0
    field_warnings: int = 0


def _discover_csvs(drop_path: Path, schemas: AssignmentSchemas) -> list[tuple[WorkflowSchema, Path]]:
    """Return recognized workflow CSVs in stable workflow and filename order."""
    leads_root = drop_path / "leads"
    if not leads_root.exists():
        return []
    if not leads_root.is_dir():
        raise LeadIngestionError("drop leads path must be a directory")
    discovered: list[tuple[WorkflowSchema, Path]] = []
    for workflow_dir in sorted((path for path in leads_root.iterdir() if path.is_dir()),
                               key=lambda path: path.name):
        csv_files = sorted(
            (path for path in workflow_dir.glob("*.csv") if path.is_file()),
            key=lambda path: path.name,
        )
        if not csv_files:
            continue
        schema = schemas.workflows.get(workflow_dir.name)
        if schema is None:
            raise LeadIngestionError(f"no schema for workflow directory {workflow_dir.name!r}")
        discovered.extend((schema, path) for path in csv_files)
    return discovered


def _storage_value(field: FieldSpec, value: str) -> str:
    """Normalize one non-empty source value to its database representation."""
    if field.field_type == "string":
        return value.strip()
    if field.field_type == "phone":
        return normalize_phone(value)
    if field.field_type == "date":
        return normalize_date(value).isoformat()
    if field.field_type == "decimal":
        return format(normalize_decimal(value), ".2f")
    if field.field_type == "enum":
        return normalize_enum(value, field.values)
    raise LeadIngestionError(f"unsupported field type {field.field_type!r}")


def _parse_row(
    cells: list[str],
    column_map: dict[str, int],
    schema: WorkflowSchema,
) -> tuple[dict[str, str | None] | None, str | None, int]:
    """Validate one nonblank row; return values, rejection reason, warning count."""
    values: dict[str, str | None] = {}
    warnings = 0
    for field in schema.fields:
        column = column_map.get(field.name)
        raw_value = cells[column] if column is not None and column < len(cells) else ""
        raw_value = raw_value.strip()
        if not raw_value:
            if field.required:
                return None, f"missing_required:{field.name}", 0
            values[field.name] = None
            continue
        try:
            values[field.name] = _storage_value(field, raw_value)
        except NormalizationError:
            if field.field_type == "phone":
                return None, "invalid_phone", 0
            if field.required:
                # The supplied schemas have no required non-phone fields with
                # parseable types; avoid silently accepting such future schemas.
                raise LeadIngestionError(
                    f"invalid required field {field.name!r} in workflow {schema.workflow_id!r}"
                )
            values[field.name] = None
            warnings += 1
    return values, None, warnings


def _insert_rejection(
    connection: sqlite3.Connection,
    drop_id: str,
    workflow_id: str,
    batch_id: str,
    row_number: int,
    reason: str,
) -> None:
    """Store PII-free rejection metadata."""
    connection.execute(
        "INSERT INTO lead_rejections (drop_id, workflow_id, batch_id, row_number, reason) "
        "VALUES (?, ?, ?, ?, ?)",
        (drop_id, workflow_id, batch_id, row_number, reason),
    )


def _merge_lead(
    connection: sqlite3.Connection,
    *,
    drop_id: str,
    batch_id: str,
    schema: WorkflowSchema,
    values: dict[str, str | None],
    fernet_key: bytes,
    hmac_secret: bytes,
    now: str,
) -> bool:
    """Insert or merge a lead; return whether this row merged a duplicate."""
    fields = schema.fields_by_name
    if "phone" not in fields or "customer_name" not in fields:
        raise LeadIngestionError(f"workflow {schema.workflow_id!r} requires phone and customer_name fields")
    phone = values.get("phone")
    customer_name = values.get("customer_name")
    if phone is None or customer_name is None:
        raise LeadIngestionError("accepted lead is missing a required PII field")
    unsupported_pii = [
        field.name for field in schema.fields
        if field.pii and field.name not in {"phone", "customer_name"}
    ]
    if unsupported_pii:
        raise LeadIngestionError(
            f"unsupported additional PII field(s) in workflow {schema.workflow_id!r}"
        )
    reference_fields = [field for field in schema.fields if field.reference]
    if len(reference_fields) != 1:
        raise LeadIngestionError(f"workflow {schema.workflow_id!r} must have one reference field")
    reference_value = values.get(reference_fields[0].name)
    if reference_value is None:
        raise LeadIngestionError("accepted lead is missing its reference value")

    digest = phone_digest(phone, hmac_secret)
    phone_cipher = encrypt_value(phone, fernet_key)
    customer_name_cipher = encrypt_value(customer_name, fernet_key)
    existing = connection.execute(
        "SELECT lead_id, attributes_json FROM leads WHERE workflow_id=? AND phone_hmac=?",
        (schema.workflow_id, digest),
    ).fetchone()

    non_pii_fields = [field for field in schema.fields if not field.pii]
    if existing is None:
        attributes = {field.name: values.get(field.name) for field in non_pii_fields}
        for field in non_pii_fields:
            if attributes.get(field.name) is None and field.default is not None:
                attributes[field.name] = field.default
        connection.execute(
            "INSERT INTO leads (workflow_id, phone_hmac, phone_cipher, customer_name_cipher, "
            "reference_value, attributes_json, batch_id, created_drop_id, updated_drop_id, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                schema.workflow_id, digest, phone_cipher, customer_name_cipher,
                reference_value, json.dumps(attributes, ensure_ascii=False, sort_keys=True),
                batch_id, drop_id, drop_id, now, now,
            ),
        )
        return False

    attributes = json.loads(existing["attributes_json"])
    for field in non_pii_fields:
        incoming = values.get(field.name)
        if incoming is not None:
            attributes[field.name] = incoming
        if attributes.get(field.name) is None and field.default is not None:
            attributes[field.name] = field.default
    connection.execute(
        "UPDATE leads SET customer_name_cipher=?, reference_value=?, attributes_json=?, "
        "updated_drop_id=?, updated_at=? WHERE lead_id=?",
        (
            customer_name_cipher, reference_value,
            json.dumps(attributes, ensure_ascii=False, sort_keys=True),
            drop_id, now, existing["lead_id"],
        ),
    )
    return True


def _ingest_file(
    connection: sqlite3.Connection,
    *,
    drop_id: str,
    schema: WorkflowSchema,
    path: Path,
    relative_path: str,
    settings: Settings,
) -> tuple[int, int, int, int]:
    """Ingest one CSV, returning total, rejected, duplicates, and warnings."""
    total = rejected = duplicates = warnings = 0
    batch_id = path.stem
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream, strict=True)
            try:
                headers = next(reader)
            except StopIteration:
                return 0, 0, 0, 0
            column_map = map_headers(headers, schema)
            for cells in reader:
                if all(not value.strip() for value in cells):
                    continue
                total += 1
                values, reason, row_warnings = _parse_row(cells, column_map, schema)
                if reason is not None:
                    rejected += 1
                    _insert_rejection(
                        connection, drop_id, schema.workflow_id, batch_id, total, reason
                    )
                    continue
                assert values is not None
                duplicates += int(
                    _merge_lead(
                        connection,
                        drop_id=drop_id,
                        batch_id=batch_id,
                        schema=schema,
                        values=values,
                        fernet_key=settings.fernet_key,
                        hmac_secret=settings.hmac_secret,
                        now=datetime.now(timezone.utc).isoformat(),
                    )
                )
                warnings += row_warnings
    except (OSError, UnicodeError, csv.Error) as exc:
        # Do not include parser exception text or row contents; either can
        # contain source values, including PII.
        raise LeadIngestionError(f"could not read CSV input {relative_path!r}") from exc
    return total, rejected, duplicates, warnings


def _ingest_lead_files(
    connection: sqlite3.Connection,
    drop_path: str | Path,
    drop_id: str,
    settings: Settings,
    schemas: AssignmentSchemas,
) -> LeadIngestionStats:
    """Ingest lead CSVs inside a caller-owned drop transaction.

    The caller must have claimed ``drop_id`` with status ``processing`` and
    created its ``drop_quality`` row in the same transaction. This function
    never initializes the database, claims/completes the drop, commits, or
    closes the supplied connection. It updates only lead-stage quality fields.
    """
    if not connection.in_transaction:
        raise LeadIngestionError("lead stage requires an outer database transaction")
    path = Path(drop_path).expanduser().resolve()
    if not path.is_dir():
        raise LeadIngestionError("drop path must be an existing directory")
    if not drop_id:
        raise LeadIngestionError("drop directory must have a name")
    drop = connection.execute(
        "SELECT status, source_path FROM ingested_drops WHERE drop_id=?", (drop_id,)
    ).fetchone()
    if drop is None or drop["status"] != "processing":
        raise LeadIngestionError("lead stage requires an existing processing drop")
    claimed_path = Path(drop["source_path"]).expanduser().resolve()
    normalized_claimed_path = os.path.normcase(os.path.normpath(str(claimed_path)))
    normalized_supplied_path = os.path.normcase(os.path.normpath(str(path)))
    if normalized_claimed_path != normalized_supplied_path:
        raise LeadIngestionError("lead stage path does not match the claimed drop path")
    quality = connection.execute(
        "SELECT drop_id FROM drop_quality WHERE drop_id=?", (drop_id,)
    ).fetchone()
    if quality is None:
        raise LeadIngestionError("lead stage requires an existing drop quality row")

    total = rejected = duplicates = warnings = 0
    for schema, csv_path in _discover_csvs(path, schemas):
        relative_path = csv_path.relative_to(path).as_posix()
        file_total, file_rejected, file_duplicates, file_warnings = _ingest_file(
            connection,
            drop_id=drop_id,
            schema=schema,
            path=csv_path,
            relative_path=relative_path,
            settings=settings,
        )
        total += file_total
        rejected += file_rejected
        duplicates += file_duplicates
        warnings += file_warnings
    updated = connection.execute(
        "UPDATE drop_quality SET lead_rows_total=lead_rows_total+?, "
        "lead_rows_rejected=lead_rows_rejected+?, "
        "duplicate_lead_rows_merged=duplicate_lead_rows_merged+?, "
        "field_warnings=field_warnings+? WHERE drop_id=?",
        (total, rejected, duplicates, warnings, drop_id),
    )
    if updated.rowcount != 1:
        raise LeadIngestionError("could not update lead-stage quality counters")
    return LeadIngestionStats(total, rejected, duplicates, warnings)
