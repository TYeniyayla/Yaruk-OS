from __future__ import annotations

import os
import secrets


def new_trace_id() -> str:
    # 16 bytes -> 32 hex chars, yeterince kisa ve benzersiz.
    return secrets.token_hex(16)


def get_or_create_trace_id(env_key: str = "YARUK_TRACE_ID") -> str:
    existing = os.getenv(env_key)
    return existing if existing else new_trace_id()

