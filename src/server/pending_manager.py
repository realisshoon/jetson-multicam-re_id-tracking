from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class PendingCandidateError(ValueError):
    """Raised when a candidate cannot make the requested state transition."""


class CandidateState(str, Enum):
    WAITING_BC = "WAITING_BC"
    WAITING_D = "WAITING_D"
    COMPLETED = "COMPLETED"
    TIMED_OUT = "TIMED_OUT"


@dataclass
class PendingCandidate:
    global_id: str
    source_node: str
    entry_message: dict[str, Any]
    state: CandidateState = CandidateState.WAITING_BC
    forwarded_nodes: set[str] = field(default_factory=set)
    matched_nodes: set[str] = field(default_factory=set)


class PendingManager:
    def __init__(self) -> None:
        self._candidates: dict[str, PendingCandidate] = {}

    def register(self, entry_message: dict[str, Any]) -> PendingCandidate:
        global_id = entry_message["global_id"]
        if global_id in self._candidates:
            raise PendingCandidateError(
                f"이미 등록된 global_id입니다: {global_id}"
            )
        candidate = PendingCandidate(
            global_id=global_id,
            source_node=entry_message["node_id"],
            entry_message=deepcopy(entry_message),
        )
        self._candidates[global_id] = candidate
        return candidate

    def get(self, global_id: str) -> PendingCandidate:
        try:
            return self._candidates[global_id]
        except KeyError as error:
            raise PendingCandidateError(
                f"등록되지 않은 global_id입니다: {global_id}"
            ) from error

    def mark_forwarded(self, global_id: str, node_id: str) -> bool:
        candidate = self.get(global_id)
        if candidate.state in {
            CandidateState.COMPLETED,
            CandidateState.TIMED_OUT,
        }:
            raise PendingCandidateError(
                f"종료된 후보에는 전송할 수 없습니다: {global_id}"
            )
        if node_id in candidate.forwarded_nodes:
            return False
        candidate.forwarded_nodes.add(node_id)
        if node_id == "D":
            candidate.state = CandidateState.WAITING_D
        return True

    def record_match(self, global_id: str, node_id: str) -> PendingCandidate:
        candidate = self.get(global_id)
        if candidate.state in {
            CandidateState.COMPLETED,
            CandidateState.TIMED_OUT,
        }:
            raise PendingCandidateError(
                f"종료된 후보의 결과는 받을 수 없습니다: {global_id}"
            )
        if node_id not in candidate.forwarded_nodes:
            raise PendingCandidateError(
                f"후보를 전송하지 않은 Node의 결과입니다: {node_id}"
            )
        if node_id in candidate.matched_nodes:
            raise PendingCandidateError(
                f"이미 받은 Node 결과입니다: {node_id}/{global_id}"
            )
        candidate.matched_nodes.add(node_id)
        if node_id == "D":
            candidate.state = CandidateState.COMPLETED
        return candidate

    def timeout(self, global_id: str) -> PendingCandidate:
        candidate = self.get(global_id)
        if candidate.state is CandidateState.COMPLETED:
            raise PendingCandidateError(
                f"완료된 후보는 timeout 처리할 수 없습니다: {global_id}"
            )
        candidate.state = CandidateState.TIMED_OUT
        return candidate
