from __future__ import annotations

from typing import Any

from yaruk.models.migrations.v1_to_v2 import migrate_v1_to_v2

CURRENT_SCHEMA = "v1"

MIGRATION_CHAIN: dict[str, tuple[str, Any]] = {
    "v1": ("v1", migrate_v1_to_v2),
}


def needs_migration(schema_version: str) -> bool:
    return schema_version != CURRENT_SCHEMA


def migrate(data: dict[str, Any], from_version: str) -> dict[str, Any]:
    version = from_version
    while version != CURRENT_SCHEMA:
        if version not in MIGRATION_CHAIN:
            raise ValueError(f"No migration path from {version} to {CURRENT_SCHEMA}")
        next_version, fn = MIGRATION_CHAIN[version]
        data = fn(data)
        version = next_version
    return data
