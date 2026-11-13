"""Tests for assignment schema loading and header mapping."""

from __future__ import annotations

import csv
import json

import pytest

from solution.au_pipeline.schema import SchemaError, load_schemas, map_headers, normalize_header


def test_loads_both_workflows_and_the_disposition_catalogue():
    loaded = load_schemas()
    assert set(loaded.workflows) == {"motor_renewal", "policy_feedback"}
    assert loaded.workflows["motor_renewal"].fields_by_name["phone"].pii is True
    assert loaded.workflows["motor_renewal"].fields_by_name["policy_number"].reference is True
    assert loaded.workflows["policy_feedback"].fields_by_name["order_id"].reference is True
    assert loaded.dispositions[("motor_renewal", "renewed")].is_success is True
    assert loaded.dispositions[("motor_renewal", "renewed")].is_terminal is True
    assert loaded.dispositions[("policy_feedback", "callback_requested")].is_success is False


def test_header_normalization_and_mapping_aliases_unknowns_and_duplicates():
    loaded = load_schemas()
    schema = loaded.workflows["motor_renewal"]
    assert normalize_header("  Insured__   Name ") == "insured name"
    mapping = map_headers(
        ["Insured Name", "ignored column", "customer_name", "Mobile No", "Language"],
        schema,
    )
    assert mapping["customer_name"] == 0  # first recognized duplicate wins
    assert "ignored" not in mapping
    assert mapping["phone"] == 3
    assert mapping["preferred_language"] == 4


def _write_minimal_schema(directory, *, field=None, disposition_flag="true"):
    directory.mkdir(parents=True, exist_ok=True)
    schema = {
        "workflow_id": "demo",
        "description": "Demo workflow",
        "fields": field if isinstance(field, list) else [field or {
            "name": "external_ref", "type": "string", "required": True,
            "reference": True, "aliases": ["ref"],
        }],
    }
    (directory / "demo.json").write_text(json.dumps(schema), encoding="utf-8")
    with (directory / "dispositions.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["workflow_id", "code", "label", "is_terminal", "is_success"])
        writer.writerow(["demo", "done", "Done", disposition_flag, "false"])


def test_schema_loader_rejects_missing_required_metadata(tmp_path):
    _write_minimal_schema(tmp_path, field={"name": "ref", "type": "string"})
    with pytest.raises(SchemaError, match="required"):
        load_schemas(tmp_path)


def test_schema_loader_rejects_invalid_disposition_definition(tmp_path):
    _write_minimal_schema(tmp_path, disposition_flag="yes")
    with pytest.raises(SchemaError, match="must be true or false"):
        load_schemas(tmp_path)


def test_schema_loader_rejects_ambiguous_aliases(tmp_path):
    _write_minimal_schema(
        tmp_path,
        field=[
            {"name": "first", "type": "string", "required": True, "reference": True,
             "aliases": ["same"]},
            {"name": "second", "type": "string", "required": False, "aliases": ["same"]},
        ],
    )
    with pytest.raises(SchemaError, match="maps to both"):
        load_schemas(tmp_path)
