from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="yaruk", description="Yaruk-OS CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    convert = sub.add_parser("convert", help="Dokuman donustur")
    convert.add_argument("input", type=Path)
    convert.add_argument("-o", "--output", type=Path, required=True)
    convert.add_argument("--config", type=Path, default=None)
    convert.add_argument("--project-config", type=Path, default=None)
    convert.add_argument("--debug", action="store_true")
    convert.add_argument("--max-pages", type=int, default=None, help="isleme sayfa limiti")
    convert.add_argument("--json", action="store_true", help="stdout'a JSON cikti")
    convert.add_argument("--stream", action="store_true", help="sayfa sayfa stdout'a stream et")
    convert.add_argument(
        "--set", nargs="*", metavar="KEY=VALUE",
        help="runtime config override (key=value)",
    )

    batch = sub.add_parser("batch", help="Toplu donusum (glob/dizin)")
    batch.add_argument("inputs", nargs="+", type=Path)
    batch.add_argument("-o", "--output", type=Path, required=True)
    batch.add_argument("--config", type=Path, default=None)
    batch.add_argument("--json", action="store_true")
    batch.add_argument("--debug", action="store_true")

    info = sub.add_parser("info", help="Donanim ve provider bilgisi")
    info.add_argument("--json", action="store_true")

    serve = sub.add_parser("serve", help="REST API sunucusu baslat")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    sub.add_parser("tui", help="TUI arayuzunu baslat")

    return parser


def _parse_cli_overrides(pairs: list[str] | None) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    if not pairs:
        return overrides
    for pair in pairs:
        if "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        try:
            overrides[k] = json.loads(v)
        except json.JSONDecodeError:
            overrides[k] = v
    return overrides


def _setup_logging(debug: bool) -> None:
    from yaruk.observability.logging import configure_logging
    level = "DEBUG" if debug else "INFO"
    configure_logging(level)


def _build_settings(
    config_path: Path | None = None,
    project_config: Path | None = None,
    cli_overrides: dict[str, Any] | None = None,
) -> Any:
    from yaruk.core.config import YarukSettings
    from yaruk.core.config_loader import load_config_layers

    layers = load_config_layers(config_path, project_config, cli_overrides)
    return YarukSettings(**{k: v for k, v in layers.items() if k in YarukSettings.model_fields})


def _run_convert(args: argparse.Namespace) -> int:
    from yaruk.core.orchestrator import Orchestrator, OrchestratorConfig
    from yaruk.core.progress import cli_progress_callback

    debug = getattr(args, "debug", False)
    _setup_logging(debug)

    overrides = _parse_cli_overrides(getattr(args, "set", None))
    settings = _build_settings(args.config, getattr(args, "project_config", None), overrides)
    args.output.mkdir(parents=True, exist_ok=True)

    progress_cb = None if args.json else cli_progress_callback

    db_path = args.output / ".yaruk_queue.sqlite"
    cfg = OrchestratorConfig(settings=settings, output_dir=args.output, db_path=db_path)
    orch = Orchestrator(cfg, progress_callback=progress_cb)

    input_path = Path(args.input)
    if not input_path.exists():
        sys.stderr.write(f"Hata: Dosya bulunamadi: {input_path}\n")
        return 1

    start = time.monotonic()

    if args.json:
        sys.stdout.write(json.dumps({"status": "processing", "input": str(input_path)}) + "\n")
        sys.stdout.flush()

    try:
        result = orch.process_sync(input_path, max_pages=getattr(args, "max_pages", None))
    except Exception as exc:
        if args.json:
            sys.stdout.write(json.dumps({"status": "failed", "error": str(exc)}) + "\n")
        else:
            sys.stderr.write(f"\nDonusum hatasi: {exc}\n")
        return 1

    elapsed = time.monotonic() - start

    if args.json:
        sys.stdout.write(json.dumps({
            "status": "done",
            "pages": result.total_pages,
            "elapsed_s": round(elapsed, 2),
            "output": str(args.output),
        }) + "\n")
    else:
        sys.stdout.write(
            f"Tamamlandi: {result.total_pages} sayfa, {elapsed:.1f}s\n"
            f"Cikti: {args.output}\n"
        )

    if getattr(args, "stream", False):
        for page in result.pages:
            sys.stdout.write(json.dumps({
                "page": page.page_number,
                "blocks": len(page.blocks),
            }) + "\n")

    return 0


def _run_batch(args: argparse.Namespace) -> int:
    from yaruk.core.orchestrator import Orchestrator, OrchestratorConfig

    _setup_logging(getattr(args, "debug", False))
    settings = _build_settings(args.config)
    args.output.mkdir(parents=True, exist_ok=True)
    db_path = args.output / ".yaruk_queue.sqlite"
    cfg = OrchestratorConfig(settings=settings, output_dir=args.output, db_path=db_path)
    orch = Orchestrator(cfg)

    all_files: list[Path] = []
    for inp in args.inputs:
        if inp.is_dir():
            all_files.extend(sorted(inp.glob("*.pdf")))
        elif inp.exists():
            all_files.append(inp)
        else:
            sys.stderr.write(f"Uyari: dosya/dizin bulunamadi: {inp}\n")

    total = len(all_files)
    success = 0
    failed = 0

    for idx, f in enumerate(all_files, 1):
        if args.json:
            sys.stdout.write(json.dumps({
                "file": str(f), "status": "processing", "index": idx, "total": total,
            }) + "\n")
            sys.stdout.flush()

        try:
            result = orch.process_sync(f)
            success += 1
            if args.json:
                sys.stdout.write(json.dumps({
                    "file": str(f), "status": "done", "pages": result.total_pages,
                }) + "\n")
            else:
                sys.stdout.write(f"[{idx}/{total}] {f.name}: {result.total_pages} sayfa\n")
        except Exception as exc:
            failed += 1
            if args.json:
                sys.stdout.write(json.dumps({
                    "file": str(f), "status": "failed", "error": str(exc),
                }) + "\n")
            else:
                sys.stderr.write(f"[{idx}/{total}] {f.name}: HATA - {exc}\n")

    if not args.json:
        sys.stdout.write(f"\nToplam: {total}, Basarili: {success}, Basarisiz: {failed}\n")

    return 0 if failed == 0 else 1


def _run_info(args: argparse.Namespace) -> int:
    from yaruk.core.config import YarukSettings
    from yaruk.core.hardware import probe_hardware
    from yaruk.core.memory_guard import DynamicMemoryGuard

    hw = probe_hardware()
    guard = DynamicMemoryGuard(YarukSettings())
    mem = guard.decide()

    data = {
        "os": hw.os,
        "arch": hw.arch,
        "ram_mb": hw.total_ram_mb,
        "nvidia": hw.has_nvidia,
        "nvidia_vram_total_mb": hw.nvidia_vram_total_mb,
        "nvidia_vram_free_mb": hw.nvidia_vram_free_mb,
        "gpu_available": mem.can_use_gpu,
        "max_batch_size": mem.max_batch_size,
        "memory_reason": mem.reason,
    }
    if args.json:
        sys.stdout.write(json.dumps(data) + "\n")
    else:
        for k, v in data.items():
            sys.stdout.write(f"{k}: {v}\n")
    return 0


def _run_serve(args: argparse.Namespace) -> int:
    try:
        import uvicorn  # type: ignore[import-untyped]
    except ImportError:
        sys.stderr.write("uvicorn gerekli: pip install uvicorn\n")
        return 1
    from yaruk.api.server import create_app
    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def _run_tui(_args: argparse.Namespace) -> int:
    try:
        from yaruk.ui.tui.app import YarukTui
    except ImportError:
        sys.stderr.write("textual gerekli: pip install textual\n")
        return 1
    YarukTui().run()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    handlers = {
        "convert": _run_convert,
        "batch": _run_batch,
        "info": _run_info,
        "serve": _run_serve,
        "tui": _run_tui,
    }

    handler = handlers.get(args.command)
    if handler:
        return handler(args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

