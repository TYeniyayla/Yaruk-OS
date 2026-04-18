from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class BlockReview:
    block_id: str
    page: int
    status: str
    action: str | None = None
    note: str | None = None


class ReviewDashboard:
    def __init__(self, confidence_threshold: float = 0.65) -> None:
        self._threshold = confidence_threshold
        self._reviews: list[BlockReview] = []
        self._actions: dict[str, str] = {}

    def add_review_item(self, block_id: str, page: int, status: str) -> None:
        review = BlockReview(block_id=block_id, page=page, status=status)
        self._reviews.append(review)

    def mark_action(self, block_id: str, action: str, note: str | None = None) -> None:
        self._actions[block_id] = action
        for r in self._reviews:
            if r.block_id == block_id:
                r.action = action
                r.note = note

    def get_pending(self) -> list[BlockReview]:
        return [
            r for r in self._reviews if r.status in ("low-confidence", "review-needed")
        ]

    def get_by_status(self, status: str) -> list[BlockReview]:
        return [r for r in self._reviews if r.status == status]

    def get_summary(self) -> dict[str, Any]:
        pending = self.get_pending()
        by_status: dict[str, int] = {}
        for r in self._reviews:
            by_status[r.status] = by_status.get(r.status, 0) + 1
        return {
            "total_reviews": len(self._reviews),
            "pending_count": len(pending),
            "by_status": by_status,
            "threshold": self._threshold,
        }

    def export_actions(self) -> dict[str, dict[str, str]]:
        result: dict[str, dict[str, str]] = {}
        for block_id, action in self._actions.items():
            result[block_id] = {"action": action}
            for r in self._reviews:
                if r.block_id == block_id:
                    result[block_id]["page"] = str(r.page)
                    if r.note:
                        result[block_id]["note"] = r.note
                    break
        return result
