from __future__ import annotations

import importlib
import importlib.metadata
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from yaruk import __version__ as yaruk_version
from yaruk.core.provider import BaseProvider

log = logging.getLogger(__name__)

CURRENT_IR_VERSION = "v1"


@dataclass(frozen=True)
class ProviderSpec:
    name: str
    factory: Callable[[], BaseProvider]
    distribution: str | None = None
    lazy: bool = True


@dataclass
class DiscoveryReport:
    loaded: list[str] = field(default_factory=list)
    skipped_version: list[str] = field(default_factory=list)
    skipped_ir: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _version_tuple(v: str) -> tuple[int, ...]:
    parts: list[int] = []
    for x in v.split("."):
        try:
            parts.append(int(x))
        except ValueError:
            parts.append(0)
    return tuple(parts)


class ProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, ProviderSpec] = {}
        self._instances: dict[str, BaseProvider] = {}

    def register(
        self,
        name: str,
        factory: Callable[[], BaseProvider],
        distribution: str | None = None,
        lazy: bool = True,
    ) -> None:
        self._providers[name] = ProviderSpec(
            name=name, factory=factory, distribution=distribution, lazy=lazy,
        )

    def discover_entrypoints(self, group: str = "yaruk.providers") -> DiscoveryReport:
        report = DiscoveryReport()
        eps = importlib.metadata.entry_points()
        for ep in eps.select(group=group):
            try:
                factory = ep.load()
            except Exception as exc:
                report.errors.append(f"{ep.name}: load failed ({exc})")
                continue

            if isinstance(factory, type) and issubclass(factory, BaseProvider):
                prov_factory: Callable[[], BaseProvider] = factory  # type: ignore[assignment]
            else:
                prov_factory = factory  # type: ignore[assignment]

            try:
                prov = prov_factory()
            except Exception as exc:
                report.errors.append(f"{ep.name}: instantiation failed ({exc})")
                continue

            if not self._version_compatible(prov):
                report.skipped_version.append(prov.name)
                continue

            if not self._ir_compatible(prov):
                report.skipped_ir.append(prov.name)
                continue

            dist_name = ep.dist.name if ep.dist else None
            self.register(prov.name, prov_factory, distribution=dist_name, lazy=True)
            report.loaded.append(prov.name)
        return report

    def _version_compatible(self, provider: BaseProvider) -> bool:
        return _version_tuple(yaruk_version) >= _version_tuple(provider.min_yaruk_version)

    def _ir_compatible(self, provider: BaseProvider) -> bool:
        return CURRENT_IR_VERSION in provider.supported_ir_versions

    def get(self, name: str) -> BaseProvider:
        if name in self._instances:
            return self._instances[name]
        spec = self._providers[name]
        instance = spec.factory()
        self._instances[name] = instance
        return instance

    def list(self) -> list[str]:
        return sorted(self._providers.keys())

    def rescan(self, group: str = "yaruk.providers") -> DiscoveryReport:
        return self.discover_entrypoints(group)

