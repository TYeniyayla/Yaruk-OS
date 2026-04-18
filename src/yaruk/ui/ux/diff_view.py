from __future__ import annotations

import difflib
from dataclasses import dataclass
from typing import Any


@dataclass
class DiffChunk:
    type: str
    old_start: int
    old_lines: list[str]
    new_start: int
    new_lines: list[str]


class DiffViewer:
    def __init__(self) -> None:
        self._old: str = ""
        self._new: str = ""

    def set_original(self, text: str) -> None:
        self._old = text

    def set_parsed(self, text: str) -> None:
        self._new = text

    def compute_diff(self) -> list[DiffChunk]:
        old_lines = self._old.splitlines(keepends=True)
        new_lines = self._new.splitlines(keepends=True)
        diff = list(
            difflib.unified_diff(
                old_lines,
                new_lines,
                lineterm="",
                n=3,
            )
        )
        chunks: list[DiffChunk] = []
        old_start = 0
        new_start = 0
        old_buf: list[str] = []
        new_buf: list[str] = []
        in_chunk = False

        for line in diff:
            if line.startswith("---"):
                continue
            if line.startswith("+++"):
                continue
            if line.startswith("@@"):
                if in_chunk:
                    chunks.append(
                        DiffChunk(
                            type="modified",
                            old_start=old_start,
                            old_lines=list(old_buf),
                            new_start=new_start,
                            new_lines=list(new_buf),
                        )
                    )
                old_buf.clear()
                new_buf.clear()
                parts = line.split()
                if len(parts) >= 4:
                    old_start = int(parts[1].split(",")[0])
                    new_start = int(parts[2].split(",")[0])
                in_chunk = True
                continue
            if line.startswith("-"):
                old_buf.append(line[1:])
            elif line.startswith("+"):
                new_buf.append(line[1:])
            else:
                old_buf.append(line)
                new_buf.append(line)

        if in_chunk:
            chunks.append(
                DiffChunk(
                    type="modified",
                    old_start=old_start,
                    old_lines=list(old_buf),
                    new_start=new_start,
                    new_lines=list(new_buf),
                )
            )

        return chunks

    def get_summary(self) -> dict[str, Any]:
        old_len = len(self._old.splitlines())
        new_len = len(self._new.splitlines())
        chunks = self.compute_diff()
        return {
            "old_lines": old_len,
            "new_lines": new_len,
            "diff_chunks": len(chunks),
            "additions": sum(len(c.new_lines) for c in chunks),
            "deletions": sum(len(c.old_lines) for c in chunks),
        }
