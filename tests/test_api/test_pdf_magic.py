from __future__ import annotations

import pytest

from yaruk.api.security import validate_pdf_magic


def test_validate_pdf_magic_accepts_minimal() -> None:
    validate_pdf_magic(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")


def test_validate_pdf_magic_rejects_plain_text() -> None:
    with pytest.raises(ValueError, match="PDF"):
        validate_pdf_magic(b"hello world")


def test_validate_pdf_magic_rejects_too_short() -> None:
    with pytest.raises(ValueError):
        validate_pdf_magic(b"%PDF")
