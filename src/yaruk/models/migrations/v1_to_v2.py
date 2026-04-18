from __future__ import annotations

from typing import Any


def migrate_v1_to_v2(data: dict[str, Any]) -> dict[str, Any]:
    if not data:
        return data

    if "schema_version" not in data:
        data["schema_version"] = "v1"

    if "pages" in data and isinstance(data["pages"], list):
        new_pages = []
        for page in data["pages"]:
            if isinstance(page, dict):
                if "blocks" in page and isinstance(page["blocks"], list):
                    new_blocks: list[dict[str, Any]] = []
                    for block in page["blocks"]:
                        if isinstance(block, dict):
                            if "reading_order" not in block:
                                block["reading_order"] = len(new_blocks)
                            if "raw_payload" not in block:
                                block["raw_payload"] = None
                            new_blocks.append(block)
                    page["blocks"] = new_blocks
                new_pages.append(page)
        data["pages"] = new_pages

    return data
