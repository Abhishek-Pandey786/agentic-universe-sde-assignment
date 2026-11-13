"""CLI integration tests using synthetic drops and temporary databases."""

from __future__ import annotations

import csv
import json
import sqlite3

import pytest

from solution.au_pipeline.__main__ import main


TEST_FERNET_KEY = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
TEST_HMAC_SECRET = "test-only-assignment-a-secret-32-bytes"


def _configure(monkeypatch, tmp_path):
    monkeypatch.setenv("AU_DATABASE_PATH", str(tmp_path / "cli.sqlite3"))
    monkeypatch.setenv("AU_FERNET_KEY", TEST_FERNET_KEY)
    monkeypatch.setenv("AU_HMAC_SECRET", TEST_HMAC_SECRET)


def _drop(tmp_path, name):
    root = tmp_path / name
    (root / "leads").mkdir(parents=True)
    (root / "events").mkdir()
    return root


def _lead(drop, *, name="Synthetic Private Customer", phone="9876543210"):
    path = drop / "leads" / "motor_renewal" / "batch_001.csv"
    path.parent.mkdir(parents=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["customer_name", "phone", "policy_number"])
        writer.writerow([name, phone, "POL-CLI-1"])


@pytest.mark.parametrize(
    "argv, expected",
    [
        (["--help"], "init-db"),
        (["init-db", "--help"], "Initialize"),
        (["ingest", "--help"], "DROP_PATH"),
        (["generate-reports", "--help"], "--output-dir"),
    ],
)
def test_help_for_top_level_and_each_subcommand(argv, expected, capsys):
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 0
    assert expected in capsys.readouterr().out


def test_init_db_command(tmp_path, monkeypatch, capsys):
    _configure(monkeypatch, tmp_path)

    assert main(["init-db"]) == 0
    assert (tmp_path / "cli.sqlite3").is_file()
    assert capsys.readouterr().out.strip() == "SQLite database initialized."


def test_ingest_one_drop_and_repeat_identical_drop(tmp_path, monkeypatch, capsys):
    _configure(monkeypatch, tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop)

    assert main(["ingest", str(drop)]) == 0
    assert capsys.readouterr().out.strip() == "Drop ingested successfully."
    assert main(["ingest", str(drop)]) == 0
    assert capsys.readouterr().out.strip() == "Skipped identical completed drop."


def test_ingest_multiple_drops_preserves_supplied_order(tmp_path, monkeypatch, capsys):
    _configure(monkeypatch, tmp_path)
    second_name = _drop(tmp_path, "drop_z")
    first_name = _drop(tmp_path, "drop_a")

    assert main(["ingest", str(second_name), str(first_name)]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "Drop ingested successfully.", "Drop ingested successfully."
    ]
    connection = sqlite3.connect(tmp_path / "cli.sqlite3")
    try:
        inserted = [row[0] for row in connection.execute(
            "SELECT drop_id FROM ingested_drops ORDER BY rowid"
        )]
    finally:
        connection.close()
    assert inserted == ["drop_z", "drop_a"]


@pytest.mark.parametrize("malformed", [False, True])
def test_invalid_drop_paths_and_malformed_structures_fail_cleanly(
    tmp_path, monkeypatch, capsys, malformed
):
    _configure(monkeypatch, tmp_path)
    drop = _drop(tmp_path, "bad_drop")
    if malformed:
        (drop / "events" / "not-jsonl.txt").write_text("private value", encoding="utf-8")
        target = drop
    else:
        target = tmp_path / "missing-drop"

    assert main(["ingest", str(target)]) == 1
    output = capsys.readouterr()
    assert "Command failed" in output.err
    assert "private value" not in output.err


@pytest.mark.parametrize(
    "fernet_key,hmac_secret,secret_marker",
    [
        (None, TEST_HMAC_SECRET, None),
        ("not-a-valid-fernet-secret", TEST_HMAC_SECRET, "not-a-valid-fernet-secret"),
        (TEST_FERNET_KEY, "too-short", "too-short"),
    ],
)
def test_missing_or_invalid_configuration_is_actionable_and_secret_safe(
    tmp_path, monkeypatch, capsys, fernet_key, hmac_secret, secret_marker
):
    monkeypatch.setenv("AU_DATABASE_PATH", str(tmp_path / "cli.sqlite3"))
    monkeypatch.delenv("AU_FERNET_KEY", raising=False)
    monkeypatch.delenv("AU_HMAC_SECRET", raising=False)
    if fernet_key is not None:
        monkeypatch.setenv("AU_FERNET_KEY", fernet_key)
    if hmac_secret is not None:
        monkeypatch.setenv("AU_HMAC_SECRET", hmac_secret)

    assert main(["init-db"]) == 2
    output = capsys.readouterr()
    assert "Configuration error" in output.err
    assert "AU_FERNET_KEY" in output.err or "AU_HMAC_SECRET" in output.err
    assert secret_marker is None or secret_marker not in output.err


def test_generate_reports_command_prints_directory_and_filenames(tmp_path, monkeypatch, capsys):
    _configure(monkeypatch, tmp_path)
    assert main(["init-db"]) == 0
    capsys.readouterr()
    output_dir = tmp_path / "cli-reports"

    assert main(["generate-reports", "--output-dir", str(output_dir)]) == 0
    output = capsys.readouterr().out
    assert str(output_dir) in output
    for filename in (
        "batch_summary.csv", "daily_overview.csv", "lead_report.csv",
        "rejections.csv", "data_quality.json",
    ):
        assert filename in output
        assert (output_dir / filename).is_file()


def test_cli_output_never_contains_config_secrets_or_lead_pii(tmp_path, monkeypatch, capsys):
    _configure(monkeypatch, tmp_path)
    drop = _drop(tmp_path, "drop_1")
    _lead(drop, name="Secret Customer Name", phone="9876543210")

    assert main(["ingest", str(drop)]) == 0
    output = capsys.readouterr().out
    for value in (TEST_FERNET_KEY, TEST_HMAC_SECRET, "Secret Customer Name", "9876543210"):
        assert value not in output


def test_report_command_failure_returns_nonzero(tmp_path, monkeypatch, capsys):
    _configure(monkeypatch, tmp_path)
    missing_output = tmp_path / "reports"

    assert main(["generate-reports", "--output-dir", str(missing_output)]) == 1
    output = capsys.readouterr()
    assert "Command failed" in output.err
    assert "Secret Customer Name" not in output.err
