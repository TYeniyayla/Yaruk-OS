from __future__ import annotations

from yaruk.postprocess.pipeline import (
    beautify_markdown,
    clean_block_text,
    fix_heading_hierarchy,
    fix_whitespace,
)


def test_fix_whitespace_collapses_newlines() -> None:
    text = "a\n\n\n\n\nb"
    result = fix_whitespace(text)
    assert "\n\n\n" not in result


def test_heading_hierarchy_fixes_skip() -> None:
    text = "# Title\n#### Deep\nText"
    result = fix_heading_hierarchy(text)
    assert "## Deep" in result


def test_beautify_roundtrip() -> None:
    text = "# H1\n\n\n\nSome text\n"
    out = beautify_markdown(text)
    assert out.startswith("# H1")


def test_clean_block_text_removes_replacement_char() -> None:
    assert clean_block_text("foo\ufffdbar") == "foobar"


def test_clean_block_text_strips_broken_sup() -> None:
    assert "<sup>" not in clean_block_text("x<sup>&</sup>y")
    assert "<sup>" not in clean_block_text("x<sup>&amp;</sup>y")


def test_clean_block_text_repairs_lone_ampersand() -> None:
    out = clean_block_text("R & C")
    assert "&amp;" not in out  # html.unescape roundtrip keeps it as '&'
    assert "&" in out


def test_clean_block_text_drops_zero_width_and_soft_hyphen() -> None:
    assert clean_block_text("a\u200bb\u00adc") == "abc"


def test_beautify_markdown_cleans_artifacts() -> None:
    bad = "Foo <sup>&</sup> bar\ufffd baz\n\n\n\nq"
    out = beautify_markdown(bad)
    assert "<sup>" not in out
    assert "\ufffd" not in out
