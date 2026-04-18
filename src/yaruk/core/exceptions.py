"""Typed errors for conversion pipeline boundaries (API, CLI, orchestration)."""


class YarukError(Exception):
    """Base class for recoverable Yaruk-OS failures."""


class ConversionError(YarukError):
    """End-to-end PDF/document conversion failed."""


class EngineWorkerError(YarukError):
    """An isolated engine subprocess or handler returned a hard failure."""


class CacheError(YarukError):
    """Disk cache read/write or eviction failed."""
