from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class NodeDMatchingConfig:
    clock_tolerance_seconds: float
    min_passage_to_d_seconds: float
    max_passage_to_d_seconds: float
    boundary_band_ratio: float
    interior_band_ratio: float
    confirmation_window_size: int
    confirmation_required_passes: int
    confirmation_min_sample_interval_seconds: float
    confirmation_max_score_spread: float
    min_journey_margin: float
    source: Path


@dataclass
class TrackEligibility:
    local_track_id: int
    first_seen_at: datetime
    seen_at_boundary: bool = False
    entered_at: datetime | None = None

    @property
    def crossed_entry_boundary(self) -> bool:
        return self.entered_at is not None


@dataclass(frozen=True)
class ConfirmationSample:
    sampled_at: datetime
    passed: bool
    score: float


@dataclass
class MatchConfirmation:
    journey_id: str | None = None
    samples: deque[ConfirmationSample] = field(default_factory=deque)
    last_sample_at: datetime | None = None

    def reset(self, journey_id: str | None = None) -> None:
        self.journey_id = journey_id
        self.samples.clear()
        self.last_sample_at = None


@dataclass(frozen=True)
class ConfirmationResult:
    accepted_sample: bool
    confirmed: bool
    sample_count: int
    pass_count: int
    score_spread: float
    reset_reason: str | None = None


def _mapping(document: Any, key: str, source: Path) -> dict[str, Any]:
    if not isinstance(document, dict) or not isinstance(document.get(key), dict):
        raise ValueError(f"node D matching config missing '{key}': {source}")
    return document[key]


def load_node_d_matching_config(path: Path) -> NodeDMatchingConfig:
    source = path.resolve()
    with source.open("r", encoding="utf-8") as config_file:
        document = yaml.safe_load(config_file)

    time_config = _mapping(document, "time", source)
    entry_config = _mapping(document, "entry", source)
    confirmation = _mapping(document, "confirmation", source)
    competition = _mapping(document, "competition", source)

    config = NodeDMatchingConfig(
        clock_tolerance_seconds=float(time_config["clock_tolerance_seconds"]),
        min_passage_to_d_seconds=float(
            time_config["min_passage_to_d_seconds"]
        ),
        max_passage_to_d_seconds=float(
            time_config["max_passage_to_d_seconds"]
        ),
        boundary_band_ratio=float(entry_config["boundary_band_ratio"]),
        interior_band_ratio=float(entry_config["interior_band_ratio"]),
        confirmation_window_size=int(confirmation["window_size"]),
        confirmation_required_passes=int(confirmation["required_passes"]),
        confirmation_min_sample_interval_seconds=float(
            confirmation["min_sample_interval_seconds"]
        ),
        confirmation_max_score_spread=float(
            confirmation["max_score_spread"]
        ),
        min_journey_margin=float(competition["min_journey_margin"]),
        source=source,
    )
    _validate(config)
    return config


def _validate(config: NodeDMatchingConfig) -> None:
    if config.clock_tolerance_seconds < 0:
        raise ValueError("clock_tolerance_seconds must be >= 0")
    if config.min_passage_to_d_seconds <= 0:
        raise ValueError("min_passage_to_d_seconds must be > 0")
    if config.max_passage_to_d_seconds <= config.min_passage_to_d_seconds:
        raise ValueError("max passage duration must exceed minimum duration")
    if not 0 < config.boundary_band_ratio < config.interior_band_ratio < 0.5:
        raise ValueError("entry ratios must satisfy 0 < boundary < interior < 0.5")
    if config.confirmation_window_size < 2:
        raise ValueError("confirmation window_size must be >= 2")
    if not 1 < config.confirmation_required_passes <= config.confirmation_window_size:
        raise ValueError("confirmation required_passes must be in [2, window_size]")
    if config.confirmation_min_sample_interval_seconds <= 0:
        raise ValueError("confirmation sample interval must be > 0")
    if config.confirmation_max_score_spread <= 0:
        raise ValueError("confirmation max_score_spread must be > 0")
    if config.min_journey_margin < 0:
        raise ValueError("min_journey_margin must be >= 0")


def parse_aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timezone-aware timestamp required: {value!r}")
    return parsed


def update_track_entry(
    state: TrackEligibility,
    box: tuple[int, int, int, int] | list[int],
    frame_width: int,
    frame_height: int,
    observed_at: datetime,
    config: NodeDMatchingConfig,
) -> TrackEligibility:
    """Update an internal frame-boundary crossing state; draws no UI geometry."""

    x1, y1, x2, y2 = box
    center_x = ((x1 + x2) / 2.0) / max(1, frame_width)
    center_y = ((y1 + y2) / 2.0) / max(1, frame_height)
    boundary = config.boundary_band_ratio
    interior = config.interior_band_ratio
    at_boundary = (
        center_x <= boundary
        or center_x >= 1.0 - boundary
        or center_y <= boundary
        or center_y >= 1.0 - boundary
    )
    in_interior = (
        interior <= center_x <= 1.0 - interior
        and interior <= center_y <= 1.0 - interior
    )

    if at_boundary:
        state.seen_at_boundary = True
    elif state.seen_at_boundary and in_interior and state.entered_at is None:
        state.entered_at = observed_at
    return state


def temporal_rejection_reason(
    track: TrackEligibility,
    passage_at: datetime,
    evaluated_at: datetime,
    config: NodeDMatchingConfig,
) -> tuple[str | None, float]:
    tolerance = config.clock_tolerance_seconds
    duration = (evaluated_at - passage_at).total_seconds()
    if track.first_seen_at.timestamp() < passage_at.timestamp() - tolerance:
        return "PREEXISTING_TRACK", duration
    if not track.crossed_entry_boundary:
        return "PREEXISTING_TRACK", duration
    if track.entered_at is not None and (
        track.entered_at.timestamp() < passage_at.timestamp() - tolerance
    ):
        return "PREEXISTING_TRACK", duration
    if duration < config.min_passage_to_d_seconds:
        return "TOO_EARLY", duration
    if duration > config.max_passage_to_d_seconds:
        return "EXPIRED_JOURNEY", duration
    return None, duration


def add_confirmation_sample(
    state: MatchConfirmation,
    journey_id: str,
    sampled_at: datetime,
    passed: bool,
    score: float,
    config: NodeDMatchingConfig,
) -> ConfirmationResult:
    if state.journey_id != journey_id:
        state.reset(journey_id)

    if state.last_sample_at is not None:
        elapsed = (sampled_at - state.last_sample_at).total_seconds()
        if elapsed < config.confirmation_min_sample_interval_seconds:
            return _confirmation_result(state, config, accepted_sample=False)

    prior_scores = [sample.score for sample in state.samples]
    if prior_scores and max(prior_scores) - score > config.confirmation_max_score_spread:
        state.reset(journey_id)
        return ConfirmationResult(
            accepted_sample=False,
            confirmed=False,
            sample_count=0,
            pass_count=0,
            score_spread=0.0,
            reset_reason="SCORE_DROP",
        )

    state.samples.append(ConfirmationSample(sampled_at, passed, score))
    while len(state.samples) > config.confirmation_window_size:
        state.samples.popleft()
    state.last_sample_at = sampled_at

    scores = [sample.score for sample in state.samples]
    if max(scores) - min(scores) > config.confirmation_max_score_spread:
        state.reset(journey_id)
        return ConfirmationResult(
            accepted_sample=False,
            confirmed=False,
            sample_count=0,
            pass_count=0,
            score_spread=0.0,
            reset_reason="SCORE_SPREAD",
        )
    return _confirmation_result(state, config, accepted_sample=True)


def _confirmation_result(
    state: MatchConfirmation,
    config: NodeDMatchingConfig,
    accepted_sample: bool,
) -> ConfirmationResult:
    samples = list(state.samples)
    scores = [sample.score for sample in samples]
    pass_count = sum(sample.passed for sample in samples)
    confirmed = (
        len(samples) == config.confirmation_window_size
        and pass_count >= config.confirmation_required_passes
    )
    return ConfirmationResult(
        accepted_sample=accepted_sample,
        confirmed=confirmed,
        sample_count=len(samples),
        pass_count=pass_count,
        score_spread=(max(scores) - min(scores) if scores else 0.0),
    )
