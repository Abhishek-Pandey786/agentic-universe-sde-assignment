"""Command-line interface for the Assignment A pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from .config import Settings
from .db import initialize_database
from .events import EventIngestionError
from .leads import LeadIngestionError
from .pipeline import DropIngestionError, ingest_drop
from .reports import ReportError, generate_reports
from .schema import SchemaError, load_schemas


REPORT_FILENAMES = (
    "batch_summary.csv",
    "daily_overview.csv",
    "lead_report.csv",
    "rejections.csv",
    "data_quality.json",
)


def _build_parser() -> argparse.ArgumentParser:
    """Build the public argparse command tree."""
    parser = argparse.ArgumentParser(
        prog="python -m solution.au_pipeline",
        description="Ingest Assignment A drops and generate reports.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    init_parser = commands.add_parser("init-db", help="initialize the SQLite database")
    init_parser.description = "Initialize the configured SQLite database."

    ingest_parser = commands.add_parser("ingest", help="ingest one or more drop directories")
    ingest_parser.description = "Ingest drops in the order supplied."
    ingest_parser.add_argument("drop_paths", nargs="+", metavar="DROP_PATH")

    reports_parser = commands.add_parser("generate-reports", help="generate the five report files")
    reports_parser.description = "Generate the Assignment A reports from the configured database."
    reports_parser.add_argument("--output-dir", required=True, metavar="PATH")
    return parser


def _configuration() -> Settings:
    """Load configuration and raise only the safe validation message."""
    return Settings.from_env()


def main(argv: Sequence[str] | None = None) -> int:
    """Run a CLI command and return its process exit status."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        settings = _configuration()
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        print(
            "Set AU_FERNET_KEY and AU_HMAC_SECRET to valid values; "
            "AU_DATABASE_PATH is optional.",
            file=sys.stderr,
        )
        return 2

    try:
        if args.command == "init-db":
            initialize_database(settings.database_path)
            print("SQLite database initialized.")
            return 0

        schemas = load_schemas()
        if args.command == "ingest":
            for drop_path in args.drop_paths:
                result = ingest_drop(drop_path, settings, schemas)
                if result.already_ingested:
                    print("Skipped identical completed drop.")
                else:
                    print("Drop ingested successfully.")
            return 0

        if args.command == "generate-reports":
            output_dir = Path(args.output_dir).expanduser()
            generate_reports(output_dir, settings, schemas)
            print(f"Reports generated in {output_dir}:")
            for filename in REPORT_FILENAMES:
                print(f"  {filename}")
            return 0
    except (DropIngestionError, LeadIngestionError, EventIngestionError, ReportError, SchemaError, OSError, ValueError) as exc:
        print(f"Command failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        # Avoid exposing unexpected exception details that might contain input data.
        print("Command failed; sensitive input details were not displayed.", file=sys.stderr)
        return 1

    parser.error("unsupported command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
