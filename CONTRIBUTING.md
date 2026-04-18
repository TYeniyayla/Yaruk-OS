# Contributing to Yaruk-OS

Thanks for your interest. This project uses **uv**, **Ruff**, **mypy** (strict in `src/yaruk`), and **pytest**.

## Setup

```bash
uv sync --all-extras
cp .env.example .env   # optional
uv run pytest tests/ -q
uv run ruff check src tests
uv run mypy src/yaruk
```

## Pull requests

1. **One logical change per PR** when possible.
2. Run **tests** and **Ruff** before pushing; fix new mypy errors in touched files.
3. **Document** user-visible behavior in `README.md` or `CHANGELOG.md` when relevant.
4. Large binaries (VLM weights) stay **out of git** unless you use Git LFS by project policy — see `models/vlm/README.md`.

## Code style

- Match existing patterns in `src/yaruk`.
- Prefer typed public APIs; avoid `Any` unless necessary.
- Engine integrations live under `src/yaruk/engines/<name>/` with `worker.py`, `adapter.py`, and provider `__init__.py`.

## Security

Do not commit secrets. Use `.env` (gitignored) and CI secrets for tokens.
