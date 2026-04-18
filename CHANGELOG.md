# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-04-18

### Added

- PDF orchestration pipeline with multiple engines (Marker, Docling, MinerU, MarkItDown, OpenDataLoader adapter path).
- Canonical IR (Pydantic), dynamic router, segmenter (ODL-PDF CLI + pdfplumber), disk cache with optional TTL/LRU eviction.
- JSON-RPC worker processes with resource limits and security hardening on IPC.
- FastAPI `yaruk serve`, CLI `yaruk convert`, optional TUI/GUI stubs.
- Tests, Ruff, mypy config, GitHub Actions CI, English landing README.
- `.env.example`, Dockerfiles-friendly layout, `CONTRIBUTING.md`.

### Changed

- `__version__` resolved from installed package metadata (`importlib.metadata`).

[0.1.0]: #
