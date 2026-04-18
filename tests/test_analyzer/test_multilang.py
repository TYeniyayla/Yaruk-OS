from __future__ import annotations

from yaruk.analyzer.multilang import detect_rtl, estimate_language_mix, normalize_utf8


def test_normalize_utf8() -> None:
    text = "caf\u0065\u0301"
    result = normalize_utf8(text)
    assert "é" in result


def test_detect_rtl_arabic() -> None:
    assert detect_rtl("مرحبا") is True


def test_detect_rtl_latin() -> None:
    assert detect_rtl("hello world") is False


def test_estimate_language_mix() -> None:
    result = estimate_language_mix("Hello dünya")
    assert "latin" in result


def test_estimate_language_mix_empty() -> None:
    result = estimate_language_mix("")
    assert result == {}
