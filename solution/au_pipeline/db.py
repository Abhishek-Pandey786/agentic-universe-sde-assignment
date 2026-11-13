"""SQLite connection management and Assignment A schema initialization."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ingested_drops (
    drop_id TEXT PRIMARY KEY CHECK(length(trim(drop_id)) > 0),
    source_path TEXT NOT NULL CHECK(length(trim(source_path)) > 0),
    content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
    status TEXT NOT NULL CHECK(status IN ('processing', 'complete')),
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leads (
    lead_id INTEGER PRIMARY KEY,
    workflow_id TEXT NOT NULL CHECK(length(trim(workflow_id)) > 0),
    phone_hmac TEXT NOT NULL CHECK(length(phone_hmac) = 64),
    phone_cipher BLOB NOT NULL CHECK(typeof(phone_cipher) = 'blob'),
    customer_name_cipher BLOB NOT NULL CHECK(typeof(customer_name_cipher) = 'blob'),
    reference_value TEXT NOT NULL CHECK(length(trim(reference_value)) > 0),
    attributes_json TEXT NOT NULL DEFAULT '{}',
    batch_id TEXT NOT NULL CHECK(length(trim(batch_id)) > 0),
    created_drop_id TEXT NOT NULL REFERENCES ingested_drops(drop_id),
    updated_drop_id TEXT NOT NULL REFERENCES ingested_drops(drop_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(workflow_id, phone_hmac),
    UNIQUE(lead_id, workflow_id)
);
CREATE INDEX IF NOT EXISTS idx_leads_workflow_batch
    ON leads(workflow_id, batch_id);
CREATE INDEX IF NOT EXISTS idx_leads_workflow_reference
    ON leads(workflow_id, reference_value);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY CHECK(length(trim(event_id)) > 0),
    workflow_id TEXT NOT NULL CHECK(length(trim(workflow_id)) > 0),
    event_type TEXT NOT NULL CHECK(length(trim(event_type)) > 0),
    call_id TEXT,
    occurred_at_utc TEXT,
    timestamp_status TEXT NOT NULL CHECK(timestamp_status IN ('valid', 'invalid')),
    phone_hmac TEXT CHECK(phone_hmac IS NULL OR length(phone_hmac) = 64),
    phone_cipher BLOB CHECK(phone_cipher IS NULL OR typeof(phone_cipher) = 'blob'),
    disposition_code_cipher BLOB CHECK(disposition_code_cipher IS NULL OR typeof(disposition_code_cipher) = 'blob'),
    payload_json TEXT NOT NULL DEFAULT '{}',
    source_drop_id TEXT NOT NULL REFERENCES ingested_drops(drop_id),
    source_file TEXT NOT NULL,
    source_line INTEGER NOT NULL CHECK(source_line > 0),
    inserted_at TEXT NOT NULL,
    CHECK((timestamp_status = 'valid' AND occurred_at_utc IS NOT NULL) OR
          (timestamp_status = 'invalid' AND occurred_at_utc IS NULL)),
    CHECK((event_type LIKE 'disposition.%' AND disposition_code_cipher IS NOT NULL) OR
          (event_type NOT LIKE 'disposition.%' AND disposition_code_cipher IS NULL)),
    CHECK((phone_hmac IS NULL) = (phone_cipher IS NULL)),
    UNIQUE(event_id, workflow_id)
);
CREATE INDEX IF NOT EXISTS idx_events_workflow_call
    ON events(workflow_id, call_id);
CREATE INDEX IF NOT EXISTS idx_events_workflow_phone
    ON events(workflow_id, phone_hmac);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at
    ON events(occurred_at_utc);

CREATE TABLE IF NOT EXISTS event_lead_links (
    event_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    lead_id INTEGER NOT NULL,
    link_method TEXT NOT NULL CHECK(link_method IN ('phone', 'call_id')),
    linked_at TEXT NOT NULL,
    FOREIGN KEY(event_id, workflow_id) REFERENCES events(event_id, workflow_id),
    FOREIGN KEY(lead_id, workflow_id) REFERENCES leads(lead_id, workflow_id)
);
CREATE INDEX IF NOT EXISTS idx_event_lead_links_lead ON event_lead_links(lead_id);

CREATE TABLE IF NOT EXISTS event_disposition_state (
    event_id TEXT PRIMARY KEY REFERENCES events(event_id),
    state TEXT NOT NULL CHECK(state IN ('pending_link', 'known', 'unknown', 'invalid_timestamp')),
    catalogue_workflow_id TEXT,
    classified_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_disposition_state_state ON event_disposition_state(state);

CREATE TABLE IF NOT EXISTS lead_rejections (
    rejection_id INTEGER PRIMARY KEY,
    drop_id TEXT NOT NULL REFERENCES ingested_drops(drop_id),
    workflow_id TEXT NOT NULL CHECK(length(trim(workflow_id)) > 0),
    batch_id TEXT NOT NULL CHECK(length(trim(batch_id)) > 0),
    row_number INTEGER NOT NULL CHECK(row_number > 0),
    reason TEXT NOT NULL CHECK(length(trim(reason)) > 0),
    UNIQUE(drop_id, workflow_id, batch_id, row_number)
);
CREATE INDEX IF NOT EXISTS idx_rejections_report_order
    ON lead_rejections(workflow_id, batch_id, row_number);

CREATE TABLE IF NOT EXISTS drop_quality (
    drop_id TEXT PRIMARY KEY REFERENCES ingested_drops(drop_id),
    lead_rows_total INTEGER NOT NULL DEFAULT 0 CHECK(lead_rows_total >= 0),
    lead_rows_rejected INTEGER NOT NULL DEFAULT 0 CHECK(lead_rows_rejected >= 0),
    duplicate_lead_rows_merged INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_lead_rows_merged >= 0),
    field_warnings INTEGER NOT NULL DEFAULT 0 CHECK(field_warnings >= 0),
    events_total INTEGER NOT NULL DEFAULT 0 CHECK(events_total >= 0),
    malformed_events INTEGER NOT NULL DEFAULT 0 CHECK(malformed_events >= 0),
    duplicate_events_dropped INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_events_dropped >= 0),
    CHECK(lead_rows_rejected <= lead_rows_total),
    CHECK(malformed_events <= events_total),
    CHECK(duplicate_events_dropped <= events_total)
);

CREATE TRIGGER IF NOT EXISTS events_no_update
BEFORE UPDATE ON events BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
CREATE TRIGGER IF NOT EXISTS events_no_delete
BEFORE DELETE ON events BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
"""

_EVENT_COLUMNS = (
    "event_id, workflow_id, event_type, call_id, occurred_at_utc, timestamp_status, "
    "phone_hmac, phone_cipher, disposition_code_cipher, payload_json, source_drop_id, "
    "source_file, source_line, inserted_at"
)

_EVENTS_REBUILT_DDL = """
CREATE TABLE events_rebuilt (
    event_id TEXT PRIMARY KEY CHECK(length(trim(event_id)) > 0),
    workflow_id TEXT NOT NULL CHECK(length(trim(workflow_id)) > 0),
    event_type TEXT NOT NULL CHECK(length(trim(event_type)) > 0),
    call_id TEXT,
    occurred_at_utc TEXT,
    timestamp_status TEXT NOT NULL CHECK(timestamp_status IN ('valid', 'invalid')),
    phone_hmac TEXT CHECK(phone_hmac IS NULL OR length(phone_hmac) = 64),
    phone_cipher BLOB CHECK(phone_cipher IS NULL OR typeof(phone_cipher) = 'blob'),
    disposition_code_cipher BLOB CHECK(disposition_code_cipher IS NULL OR typeof(disposition_code_cipher) = 'blob'),
    payload_json TEXT NOT NULL DEFAULT '{}',
    source_drop_id TEXT NOT NULL REFERENCES ingested_drops(drop_id),
    source_file TEXT NOT NULL,
    source_line INTEGER NOT NULL CHECK(source_line > 0),
    inserted_at TEXT NOT NULL,
    CHECK((timestamp_status = 'valid' AND occurred_at_utc IS NOT NULL) OR
          (timestamp_status = 'invalid' AND occurred_at_utc IS NULL)),
    CHECK((event_type LIKE 'disposition.%' AND disposition_code_cipher IS NOT NULL) OR
          (event_type NOT LIKE 'disposition.%' AND disposition_code_cipher IS NULL)),
    CHECK((phone_hmac IS NULL) = (phone_cipher IS NULL)),
    UNIQUE(event_id, workflow_id)
);
"""


def _migrate_event_type_constraint(connection: sqlite3.Connection) -> None:
    """Rebuild the legacy events table to permit unknown non-empty event types."""
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='events'"
    ).fetchone()
    if row is None or "event_type IN (" not in (row["sql"] or ""):
        return

    connection.execute(_EVENTS_REBUILT_DDL)
    connection.execute(
        f"INSERT INTO events_rebuilt ({_EVENT_COLUMNS}) SELECT {_EVENT_COLUMNS} FROM events"
    )
    connection.execute("DROP TABLE events")
    connection.execute("ALTER TABLE events_rebuilt RENAME TO events")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_workflow_call ON events(workflow_id, call_id)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_workflow_phone ON events(workflow_id, phone_hmac)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON events(occurred_at_utc)")
    connection.execute(
        "CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events BEGIN "
        "SELECT RAISE(ABORT, 'events are append-only'); END"
    )
    connection.execute(
        "CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events BEGIN "
        "SELECT RAISE(ABORT, 'events are append-only'); END"
    )
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise sqlite3.IntegrityError("event table migration would violate foreign keys")


def connect(database_path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection with foreign keys and a bounded busy wait."""
    path = Path(database_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 10000")
    return connection


@contextmanager
def database(database_path: str | Path) -> Iterator[sqlite3.Connection]:
    """Yield a configured connection and always close it after use."""
    connection = connect(database_path)
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    finally:
        connection.close()


def initialize_database(database_path: str | Path) -> None:
    """Atomically create tables, indexes, and triggers if absent.

    ``executescript`` commits any pending transaction before running its input,
    so the script itself starts an explicit transaction. If a DDL statement
    fails, the connection context rolls that transaction back.
    """
    with database(database_path) as connection:
        legacy = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='events'"
        ).fetchone()
        needs_migration = legacy is not None and "event_type IN (" in (legacy["sql"] or "")
        if needs_migration:
            # SQLite cannot toggle foreign-key enforcement inside a transaction.
            # Disable it only for the atomic table swap, then validate the full
            # graph before commit and restore enforcement before returning.
            connection.execute("PRAGMA foreign_keys = OFF")
        try:
            # executescript commits prior work, then leaves this BEGIN open
            # because the script deliberately has no trailing COMMIT.
            connection.executescript("BEGIN IMMEDIATE;\n" + _SCHEMA)
            _migrate_event_type_constraint(connection)
            if needs_migration:
                violations = connection.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    raise sqlite3.IntegrityError("database schema migration would violate foreign keys")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            if needs_migration:
                connection.execute("PRAGMA foreign_keys = ON")
