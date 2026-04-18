from __future__ import annotations

import re
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent

GOLDEN_DATASET = [
    {
        "name": "academic_article",
        "pdf": "pdfs/academic_sample.pdf",
        "expected_md": "golden_output/academic_sample.md",
        "expected_json": "golden_output/academic_sample.json",
        "thresholds": {
            "layout_fidelity": 0.85,
            "reading_order": 0.90,
            "table_structure": 0.80,
            "equation_accuracy": 0.75,
        },
    },
    {
        "name": "datasheet",
        "pdf": "pdfs/datasheet.pdf",
        "expected_md": "golden_output/datasheet.md",
        "expected_json": "golden_output/datasheet.json",
        "thresholds": {
            "layout_fidelity": 0.80,
            "reading_order": 0.85,
            "table_structure": 0.90,
            "equation_accuracy": 0.70,
        },
    },
    {
        "name": "scanned_doc",
        "pdf": "pdfs/scanned_doc.pdf",
        "expected_md": "golden_output/scanned_doc.md",
        "expected_json": "golden_output/scanned_doc.json",
        "thresholds": {
            "layout_fidelity": 0.70,
            "reading_order": 0.75,
            "table_structure": 0.60,
            "equation_accuracy": 0.50,
        },
    },
]

METRIC_FUNCTIONS = {}


def register_metric(name: str, fn: callable) -> None:
    METRIC_FUNCTIONS[name] = fn


def compute_metric(name: str, actual: str, expected: str) -> float:
    if name not in METRIC_FUNCTIONS:
        raise ValueError(f"Unknown metric: {name}")
    return METRIC_FUNCTIONS[name](actual, expected)


def layout_fidelity(actual: str, expected: str) -> float:
    if not expected:
        return 0.0
    actual_clean = " ".join(actual.split())
    expected_clean = " ".join(expected.split())
    if actual_clean == expected_clean:
        return 1.0
    common = sum(1 for a, e in zip(actual_clean, expected_clean, strict=False) if a == e)
    return common / max(len(expected_clean), 1)


def reading_order_accuracy(actual: str, expected: str) -> float:
    actual_lines = [line for line in actual.split("\n") if line.strip()]
    expected_lines = [line for line in expected.split("\n") if line.strip()]
    if not expected_lines:
        return 0.0
    matches = sum(1 for a, e in zip(actual_lines, expected_lines, strict=False) if a == e)
    return matches / len(expected_lines)


def table_structure_score(actual: str, expected: str) -> float:
    actual_tables = actual.count("|")
    expected_tables = expected.count("|")
    if expected_tables == 0:
        return 1.0 if actual_tables == 0 else 0.0
    return min(actual_tables, expected_tables) / max(actual_tables, expected_tables)


def equation_accuracy(actual: str, expected: str) -> float:
    actual_eqs = len(re.findall(r"\$\$.*?\$\$|\$.*?\$", actual, re.DOTALL))
    expected_eqs = len(re.findall(r"\$\$.*?\$\$|\$.*?\$", expected, re.DOTALL))
    if expected_eqs == 0:
        return 1.0 if actual_eqs == 0 else 0.0
    return min(actual_eqs, expected_eqs) / max(actual_eqs, expected_eqs)


register_metric("layout_fidelity", layout_fidelity)
register_metric("reading_order", reading_order_accuracy)
register_metric("table_structure", table_structure_score)
register_metric("equation_accuracy", equation_accuracy)
