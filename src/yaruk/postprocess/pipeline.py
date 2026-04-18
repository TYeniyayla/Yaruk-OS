"""Markdown / text post-processing pipeline.

Two layers:
  * ``clean_block_text``   — applied per IR block (renderer-agnostic hygiene).
  * ``beautify_markdown``  — applied to assembled markdown (layout-level hygiene).

Rationale: artefacts like ``<sup>&</sup>``, stray ``&amp;``, U+FFFD replacement
characters and soft hyphens originate in engine outputs (Marker/Docling/MinerU).
If we only sanitize at the final markdown layer, those artefacts leak into
``pages/page_XXX.json`` and any GUI/TUI that reads IR directly. We clean at
both layers to guarantee consistency.
"""
from __future__ import annotations

import contextlib
import html
import re
import unicodedata

_BROKEN_SUP_RE = re.compile(r"<sup>\s*&(?:amp;)?\s*</sup>", re.IGNORECASE)
_EMPTY_SUP_SUB_RE = re.compile(r"<(sup|sub)>\s*</\1>", re.IGNORECASE)
_LONE_AMP_ENT_RE = re.compile(r"&(?!(?:#\d+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]{1,8});)")
_MULTI_SPACES_RE = re.compile(r"[ \t]{2,}")
_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff]")


def _strip_replacement_chars(text: str) -> str:
    """Drop Unicode U+FFFD (replacement char) — artefact of failed decoding.

    We delete rather than substitute: replacement chars appear in table cells
    / dingbats / glyph mapping failures; a neighbouring word is almost always
    readable without them, and keeping them confuses downstream MT/TTS.
    """
    if "\ufffd" not in text:
        return text
    return text.replace("\ufffd", "")


def _normalize_unicode(text: str) -> str:
    """NFKC + remove zero-width + normalize soft hyphens."""
    text = unicodedata.normalize("NFKC", text)
    text = _ZERO_WIDTH_RE.sub("", text)
    text = text.replace("\u00ad", "")  # soft hyphen
    return text


def _clean_html_artifacts(text: str) -> str:
    """Remove/repair stray HTML fragments engines embed in markdown."""
    text = _BROKEN_SUP_RE.sub("", text)
    text = _EMPTY_SUP_SUB_RE.sub("", text)
    text = _LONE_AMP_ENT_RE.sub("&amp;", text)
    if any(tok in text for tok in ("&amp;", "&lt;", "&gt;", "&quot;", "&#")):
        with contextlib.suppress(Exception):
            text = html.unescape(text)
    return text


def clean_block_text(text: str) -> str:
    """Hygiene for IR block text (applied renderer-side, before markdown merge)."""
    if not text:
        return text
    text = _strip_replacement_chars(text)
    text = _normalize_unicode(text)
    text = _clean_html_artifacts(text)
    text = _MULTI_SPACES_RE.sub(" ", text)
    return text.strip()


def fix_whitespace(text: str) -> str:
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    return text.strip() + "\n"


def merge_broken_sentences(text: str) -> str:
    lines = text.split("\n")
    merged: list[str] = []
    for line in lines:
        if merged and merged[-1] and not merged[-1].endswith(("\n", ":", ".", "!", "?", "|")):
            stripped = line.strip()
            if stripped and stripped[0].islower():
                merged[-1] = merged[-1].rstrip() + " " + stripped
                continue
        merged.append(line)
    return "\n".join(merged)


def fix_heading_hierarchy(text: str) -> str:
    lines = text.split("\n")
    result: list[str] = []
    prev_level = 0
    for line in lines:
        m = re.match(r"^(#{1,6})\s", line)
        if m:
            level = len(m.group(1))
            if prev_level > 0 and level > prev_level + 1:
                level = prev_level + 1
                line = "#" * level + line[len(m.group(1)):]
            prev_level = level
        result.append(line)
    return "\n".join(result)


def sanitize_latex(text: str) -> str:
    text = re.sub(r"\$\s+", "$ ", text)
    text = re.sub(r"\s+\$", " $", text)
    return text


def beautify_markdown(text: str) -> str:
    text = _strip_replacement_chars(text)
    text = _normalize_unicode(text)
    text = _clean_html_artifacts(text)
    text = fix_whitespace(text)
    text = merge_broken_sentences(text)
    text = fix_heading_hierarchy(text)
    text = sanitize_latex(text)
    return text
