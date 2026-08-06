from __future__ import annotations

from collections.abc import Mapping, Sequence


ROUTES: dict[str, tuple[str, ...]] = {
    "A": ("B", "C"),
    "B": ("D",),
    "C": ("D",),
    "D": (),
}


class RouteError(ValueError):
    """Raised when a node is not part of the configured camera route."""


class RouteManager:
    def __init__(
        self,
        routes: Mapping[str, Sequence[str]] | None = None,
    ) -> None:
        selected = routes or ROUTES
        self._routes = {
            source: tuple(targets)
            for source, targets in selected.items()
        }
        self._validate_routes()

    @property
    def nodes(self) -> frozenset[str]:
        return frozenset(self._routes)

    def targets_for(self, source_node: str) -> tuple[str, ...]:
        try:
            return self._routes[source_node]
        except KeyError as error:
            raise RouteError(f"지원하지 않는 Node입니다: {source_node}") from error

    def _validate_routes(self) -> None:
        known_nodes = set(self._routes)
        for source, targets in self._routes.items():
            if not source:
                raise RouteError("라우팅 Node ID는 비어 있을 수 없습니다.")
            unknown_targets = set(targets) - known_nodes
            if unknown_targets:
                joined = ", ".join(sorted(unknown_targets))
                raise RouteError(
                    f"{source}의 대상이 라우팅 테이블에 없습니다: {joined}"
                )
