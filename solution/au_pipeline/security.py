"""Encryption and keyed phone identity helpers."""

from __future__ import annotations

import hashlib
import hmac

from cryptography.fernet import Fernet, InvalidToken


def encrypt_value(value: str, key: bytes) -> bytes:
    """Encrypt UTF-8 text with Fernet and return its token as bytes."""
    if not isinstance(value, str):
        raise TypeError("value must be a string")
    return Fernet(key).encrypt(value.encode("utf-8"))


def decrypt_value(token: bytes, key: bytes) -> str:
    """Decrypt a Fernet token and decode UTF-8 text."""
    if not isinstance(token, bytes):
        raise TypeError("token must be bytes")
    try:
        plaintext = Fernet(key).decrypt(token)
        return plaintext.decode("utf-8")
    except (InvalidToken, UnicodeDecodeError) as exc:
        raise ValueError("could not decrypt value") from exc


def phone_digest(canonical_phone: str, secret: bytes) -> str:
    """Return a deterministic lowercase HMAC-SHA256 hex digest.

    The caller must pass a canonical phone value (normalization belongs to the
    ingestion layer). No plaintext phone or secret is returned or logged.
    """
    if not isinstance(canonical_phone, str) or not canonical_phone:
        raise ValueError("canonical_phone must be a non-empty string")
    if not isinstance(secret, bytes) or len(secret) < 32:
        raise ValueError("HMAC secret must contain at least 32 bytes")
    return hmac.new(secret, canonical_phone.encode("utf-8"), hashlib.sha256).hexdigest()
