from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any


class EventRepository(ABC):
    """Persistence boundary to be implemented by Memory or Django storage."""

    @abstractmethod
    def record_entry(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_match(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_unknown(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def record_timeout(self, message: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def update_node_status(self, message: dict[str, Any]) -> None:
        raise NotImplementedError


class MemoryEventRepository(EventRepository):
    """In-memory event log used until the Django repository is available."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.node_statuses: dict[str, dict[str, Any]] = {}

    def record_entry(self, message: dict[str, Any]) -> None:
        self._append("ENTRY", message)

    def record_match(self, message: dict[str, Any]) -> None:
        self._append("MATCH", message)

    def record_unknown(self, message: dict[str, Any]) -> None:
        self._append("UNKNOWN", message)

    def record_timeout(self, message: dict[str, Any]) -> None:
        self._append("TIMEOUT", message)

    def update_node_status(self, message: dict[str, Any]) -> None:
        saved = deepcopy(message)
        self.node_statuses[message["node_id"]] = saved
        self._append("NODE_STATUS", message)

    def _append(self, event_type: str, message: dict[str, Any]) -> None:
        self.events.append(
            {
                "event_type": event_type,
                "message": deepcopy(message),
            }
        )
