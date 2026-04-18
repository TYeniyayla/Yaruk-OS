from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PageUpdate:
    page_number: int
    html: str
    timestamp: float


class LivePreview:
    def __init__(self) -> None:
        self._updates: list[PageUpdate] = []

    def add_page(self, page_number: int, html: str) -> None:
        import time

        self._updates.append(PageUpdate(page_number, html, time.monotonic()))

    def get_updates(self, since: float | None = None) -> list[PageUpdate]:
        if since is None:
            return list(self._updates)
        return [u for u in self._updates if u.timestamp > since]

    def clear(self) -> None:
        self._updates.clear()

    def render_page(self, page_number: int) -> str | None:
        for u in self._updates:
            if u.page_number == page_number:
                return u.html
        return None
