"""Load and validate the supplied workflow and disposition schemas."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SUPPORTED_TYPES = frozenset({"string", "phone", "date", "decimal", "enum"})
_HEADER_SEPARATORS = re.compile(r"[_\s]+")


class SchemaError(ValueError):
    """Raised when an assignment schema or disposition catalogue is invalid."""


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """Validated metadata for one workflow lead field."""

    name: str
    field_type: str
    required: bool
    pii: bool
    aliases: tuple[str, ...] = ()
    values: tuple[str, ...] = ()
    default: str | None = None
    reference: bool = False


@dataclass(frozen=True, slots=True)
class WorkflowSchema:
    """A workflow's ordered lead fields and their normalized header map."""

    workflow_id: str
    description: str
    fields: tuple[FieldSpec, ...]

    @property
    def fields_by_name(self) -> dict[str, FieldSpec]:
        """Return fields indexed by canonical field name."""
        return {field.name: field for field in self.fields}

    @property
    def header_to_field(self) -> dict[str, str]:
        """Return normalized field names and aliases mapped to canonical names."""
        result: dict[str, str] = {}
        for field in self.fields:
            for header in (field.name, *field.aliases):
                result[normalize_header(header)] = field.name
        return result


@dataclass(frozen=True, slots=True)
class Disposition:
    """Validated disposition catalogue entry."""

    workflow_id: str
    code: str
    label: str
    is_terminal: bool
    is_success: bool


@dataclass(frozen=True, slots=True)
class AssignmentSchemas:
    """All workflow schemas and the disposition catalogue."""

    workflows: dict[str, WorkflowSchema]
    dispositions: dict[tuple[str, str], Disposition]


def normalize_header(header: str) -> str:
    """Lowercase and trim a header, collapsing underscores/whitespace."""
    if not isinstance(header, str):
        raise TypeError("header must be a string")
    return _HEADER_SEPARATORS.sub(" ", header.strip().lower())


def map_headers(headers: Iterable[str], schema: WorkflowSchema) -> dict[str, int]:
    """Map canonical fields to source column indexes.

    Unknown headers are ignored. If several recognized columns map to one
    field, the earliest recognized column in source order wins.
    """
    known = schema.header_to_field
    result: dict[str, int] = {}
    for index, header in enumerate(headers):
        field_name = known.get(normalize_header(header))
        if field_name is not None and field_name not in result:
            result[field_name] = index
    return result


def _require_string(value: object, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SchemaError(f"{location} must be a non-empty string")
    return value.strip()


def _parse_bool(value: object, location: str) -> bool:
    if isinstance(value, bool):
        return value
    raise SchemaError(f"{location} must be a JSON boolean")


def _load_workflow(path: Path) -> WorkflowSchema:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaError(f"could not read workflow schema {path.name}") from exc
    if not isinstance(raw, dict):
        raise SchemaError(f"workflow schema {path.name} must contain an object")

    workflow_id = _require_string(raw.get("workflow_id"), f"{path.name}.workflow_id")
    description = _require_string(raw.get("description"), f"{path.name}.description")
    raw_fields = raw.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise SchemaError(f"{path.name}.fields must be a non-empty array")

    fields: list[FieldSpec] = []
    names: set[str] = set()
    header_owners: dict[str, str] = {}
    reference_count = 0
    for index, item in enumerate(raw_fields):
        loc = f"{path.name}.fields[{index}]"
        if not isinstance(item, dict):
            raise SchemaError(f"{loc} must be an object")
        name = _require_string(item.get("name"), f"{loc}.name")
        field_type = _require_string(item.get("type"), f"{loc}.type")
        if name in names:
            raise SchemaError(f"duplicate field name {name!r} in {path.name}")
        names.add(name)
        if field_type not in SUPPORTED_TYPES:
            raise SchemaError(f"unsupported field type {field_type!r} in {path.name}")
        required = _parse_bool(item.get("required"), f"{loc}.required")
        pii = _parse_bool(item.get("pii", False), f"{loc}.pii")
        reference = _parse_bool(item.get("reference", False), f"{loc}.reference")
        reference_count += int(reference)

        raw_aliases = item.get("aliases", [])
        if not isinstance(raw_aliases, list) or any(
            not isinstance(alias, str) or not alias.strip() for alias in raw_aliases
        ):
            raise SchemaError(f"{loc}.aliases must be an array of non-empty strings")
        aliases = tuple(alias.strip() for alias in raw_aliases)

        raw_values = item.get("values", [])
        if field_type == "enum":
            if not isinstance(raw_values, list) or not raw_values:
                raise SchemaError(f"{loc}.values must be a non-empty array for enum fields")
            if any(not isinstance(value, str) or not value.strip() for value in raw_values):
                raise SchemaError(f"{loc}.values must contain non-empty strings")
            values = tuple(raw_values)
            if len({value.casefold() for value in values}) != len(values):
                raise SchemaError(f"{loc}.values contains duplicate enum values")
        else:
            if "values" in item and (not isinstance(raw_values, list) or raw_values):
                raise SchemaError(f"{loc}.values is only valid for enum fields")
            values = ()

        default = item.get("default")
        if default is not None and not isinstance(default, str):
            raise SchemaError(f"{loc}.default must be a string when provided")
        if default is not None and field_type == "enum" and default.casefold() not in {
            value.casefold() for value in values
        }:
            raise SchemaError(f"{loc}.default must be one of its enum values")

        field_spec = FieldSpec(
            name=name,
            field_type=field_type,
            required=required,
            pii=pii,
            aliases=aliases,
            values=values,
            default=default,
            reference=reference,
        )
        fields.append(field_spec)
        for header in (name, *aliases):
            normalized = normalize_header(header)
            owner = header_owners.get(normalized)
            if owner is not None and owner != name:
                raise SchemaError(
                    f"header {header!r} maps to both {owner!r} and {name!r} in {path.name}"
                )
            header_owners[normalized] = name

    if reference_count != 1:
        raise SchemaError(f"{path.name} must define exactly one reference field")
    return WorkflowSchema(workflow_id, description, tuple(fields))


def _load_dispositions(path: Path, workflow_ids: set[str]) -> dict[tuple[str, str], Disposition]:
    try:
        stream = path.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise SchemaError(f"could not read disposition catalogue {path.name}") from exc
    result: dict[tuple[str, str], Disposition] = {}
    with stream:
        reader = csv.DictReader(stream)
        expected = {"workflow_id", "code", "label", "is_terminal", "is_success"}
        if reader.fieldnames is None or set(reader.fieldnames) != expected:
            raise SchemaError(f"{path.name} must have columns {', '.join(sorted(expected))}")
        for row_number, row in enumerate(reader, start=2):
            if None in row or any(row.get(key) is None for key in expected):
                raise SchemaError(f"invalid disposition row {row_number}")
            workflow_id = _require_string(row["workflow_id"], f"row {row_number}.workflow_id")
            code = _require_string(row["code"], f"row {row_number}.code")
            label = _require_string(row["label"], f"row {row_number}.label")
            if workflow_id not in workflow_ids:
                raise SchemaError(f"unknown workflow {workflow_id!r} in disposition row {row_number}")
            flags: list[bool] = []
            for column in ("is_terminal", "is_success"):
                text = row[column].strip().lower()
                if text not in {"true", "false"}:
                    raise SchemaError(f"row {row_number}.{column} must be true or false")
                flags.append(text == "true")
            key = (workflow_id, code)
            if key in result:
                raise SchemaError(f"duplicate disposition {workflow_id}/{code}")
            result[key] = Disposition(workflow_id, code, label, flags[0], flags[1])
    if not result:
        raise SchemaError(f"{path.name} must contain at least one disposition")
    if workflow_ids - {workflow_id for workflow_id, _ in result}:
        missing = ", ".join(sorted(workflow_ids - {workflow_id for workflow_id, _ in result}))
        raise SchemaError(f"no dispositions defined for workflow(s): {missing}")
    return result


def load_schemas(schema_dir: str | Path | None = None) -> AssignmentSchemas:
    """Load all workflow JSON files and ``dispositions.csv`` from a directory."""
    directory = (
        Path(schema_dir)
        if schema_dir is not None
        else Path(__file__).resolve().parents[2] / "assignment-a-data" / "schemas"
    )
    if not directory.is_dir():
        raise SchemaError(f"schema directory does not exist: {directory}")
    workflow_files = sorted(directory.glob("*.json"), key=lambda path: path.name)
    if not workflow_files:
        raise SchemaError("schema directory contains no workflow JSON files")
    workflows: dict[str, WorkflowSchema] = {}
    for path in workflow_files:
        workflow = _load_workflow(path)
        if workflow.workflow_id in workflows:
            raise SchemaError(f"duplicate workflow_id {workflow.workflow_id!r}")
        workflows[workflow.workflow_id] = workflow
    dispositions = _load_dispositions(directory / "dispositions.csv", set(workflows))
    return AssignmentSchemas(workflows, dispositions)
