<div align="center">

<img src="assets/yaruk-os-logo.png" alt="Yaruk-OS" width="200" />

# Yaruk-OS

**Shed light on complex documents — without breaking their spatial story.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![uv](https://img.shields.io/badge/uv-package%20manager-FF4154?style=for-the-badge)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-see%20LICENSE-2ea44f?style=for-the-badge)](LICENSE)

<br />

</div>

---

## Why Yaruk-OS?

Technical PDFs are more than plain text: **schematics, plots, tables, LaTeX, multi-column layouts**. A single “one-size-fits-all” extractor often mangles reading order, geometry, or structure.

**Yaruk-OS** is a **hardware-agnostic, plug-and-play PDF orchestration pipeline** that:

- **Segments** the document and classifies content by intent  
- **Routes** each segment to the engine that fits best (Marker, Docling, MinerU, OpenDataLoader-PDF, MarkItDown, …)  
- **Merges** everything into a shared **Canonical IR** (Pydantic models) and exports clean **Markdown / JSON**  
- **Degrades gracefully** when VRAM is tight or a worker fails — CPU fallbacks, subprocess isolation, and bounded IPC  

---

## Highlights

| | |
|---|---|
| **Spatial fidelity** | Preserve `bbox`, page index, reading order, and block relationships where the stack allows. |
| **Right tool, right block** | Equations, tables, figures, and body text can follow different engines in one run. |
| **Resilient execution** | JSON-RPC workers with resource limits, stall watchdogs, and structured logging (`trace_id`). |
| **Observable** | JSON logging, job queue (SQLite / SQLModel), and a clear on-disk output contract. |
| **API-ready** | Optional FastAPI service for headless conversion (`yaruk serve`). |

---

## Architecture (at a glance)

```mermaid
flowchart LR
  PDF[PDF input] --> PRE[Pre-analysis]
  PRE --> SEG[Segmentation]
  SEG --> RTR[Dynamic router]
  RTR --> W1[Engines]
  W1 --> MRG[IR merge]
  MRG --> OUT[Markdown / JSON / assets]
```

For the full product vision and phased roadmap, see the design documents in the repository (when included).

---

## Engines

| Engine | Typical strength |
|--------|------------------|
| **Marker** | Fast general-purpose Markdown |
| **Docling** | Tables & semantic structure |
| **MinerU** | Math-heavy / academic layouts |
| **OpenDataLoader-PDF** | Reading order & layout grid (CLI integration) |
| **MarkItDown** | Lightweight office-style conversion |

Engines run in **isolated subprocesses** where configured, with guarded IPC between the orchestrator and workers.

---

## Quick start

**Requirements:** Python **3.11+**, [uv](https://github.com/astral-sh/uv) recommended.

```bash
git clone <this-repository-url>
cd Yaruk-OS

uv sync --all-extras    # or: uv sync
```

### CLI

```bash
# Convert a single PDF into an output directory (Canonical IR + merged.md)
uv run yaruk convert ./document.pdf -o ./out

# Optional: JSON status line, page cap, debug logging
uv run yaruk convert ./document.pdf -o ./out --json --max-pages 50 --debug

# Hardware & provider probe
uv run yaruk info
```

### REST API (optional extras)

```bash
uv sync --extra api
uv run yaruk serve --host 127.0.0.1 --port 8000
```

Uploads are size-limited and PDF magic-byte checked by default — tune with `YARUK_API_*` settings.

---

## Development

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Environment variable templates: [`.env.example`](.env.example).

```bash
uv sync --all-extras
uv run ruff check src tests
uv run mypy src/yaruk
uv run pytest tests/
```

Optional: `pre-commit install` if you use the bundled hook configuration.

### Docker (API, CPU)

```bash
docker compose build
docker compose up
```

---

## Repository layout (short)

```
src/yaruk/          Application package (core, engines, api, output, queue, …)
tests/              Pytest suite & fixtures
models/vlm/         VLM manifest & optional local weights (see models/vlm/README.md)
flatpak/ appimage/  Distribution manifests
```

---

## Security

Treat all PDFs as **untrusted input**. The stack uses subprocess isolation, bounded JSON-RPC lines, validated temp paths for large payloads, and configurable API upload limits. For dependency advisories, run your preferred scanner (e.g. `pip-audit`) on the locked environment.

---

## License

See [`LICENSE`](LICENSE).

---

<div align="center">

<sub>Built for engineers who care about **layout**, not just **strings**.</sub>

</div>
