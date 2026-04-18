from __future__ import annotations

import unicodedata


def normalize_utf8(text: str) -> str:
    return unicodedata.normalize("NFC", text)


def detect_rtl(text: str) -> bool:
    return any(unicodedata.bidirectional(ch) in ("R", "AL", "AN") for ch in text[:500])


def estimate_language_mix(text: str) -> dict[str, float]:
    """
    Basit heuristik: ASCII vs non-ASCII oran.
    Gercek uygulamada langdetect/fasttext entegre edilecek.
    """
    if not text:
        return {}
    total = len(text)
    ascii_count = sum(1 for c in text if c.isascii() and c.isalpha())
    non_ascii = sum(1 for c in text if not c.isascii() and c.isalpha())
    result: dict[str, float] = {}
    if ascii_count:
        result["latin"] = round(ascii_count / total, 3)
    if non_ascii:
        result["non_latin"] = round(non_ascii / total, 3)
    return result
