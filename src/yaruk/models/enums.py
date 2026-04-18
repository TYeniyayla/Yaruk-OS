from __future__ import annotations

from enum import StrEnum


class BlockType(StrEnum):
    PARAGRAPH = "paragraph"
    TABLE = "table"
    EQUATION = "equation"
    FIGURE = "figure"
    HEADING = "heading"
    LIST = "list"
    CODE = "code"
    CAPTION = "caption"
    FOOTER = "footer"
    HEADER = "header"
    OTHER = "other"


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"

