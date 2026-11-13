"""Load runtime configuration without persisting secrets."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

from cryptography.fernet import Fernet, InvalidToken


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime settings for the local Assignment A pipeline."""

    database_path: Path
    fernet_key: bytes = field(repr=False)
    hmac_secret: bytes = field(repr=False)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        """Build settings from environment values, requiring both secrets.

        AU_DATABASE_PATH defaults to a local SQLite file. Secrets have no defaults
        and are never written to the database.
        """
        values = os.environ if environ is None else environ
        db_value = values.get("AU_DATABASE_PATH", "solution/assignment_a.sqlite3").strip()
        if not db_value:
            raise ValueError("AU_DATABASE_PATH must not be empty")

        fernet_text = values.get("AU_FERNET_KEY", "").strip()
        if not fernet_text:
            raise ValueError("AU_FERNET_KEY is required")
        try:
            fernet_key = fernet_text.encode("ascii", errors="strict")
            Fernet(fernet_key)
        except (UnicodeEncodeError, ValueError, TypeError, InvalidToken) as exc:
            raise ValueError("AU_FERNET_KEY must be a valid Fernet key") from exc

        hmac_text = values.get("AU_HMAC_SECRET", "").strip()
        if not hmac_text:
            raise ValueError("AU_HMAC_SECRET is required")
        hmac_secret = hmac_text.encode("utf-8")
        if len(hmac_secret) < 32:
            raise ValueError("AU_HMAC_SECRET must contain at least 32 UTF-8 bytes")

        return cls(Path(db_value).expanduser(), fernet_key, hmac_secret)
