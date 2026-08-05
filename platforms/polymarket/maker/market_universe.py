"""Build one deterministic, non-secret market universe for every LP account."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


SECTIONS = ("markets", "night_markets")
TRANSIENT_FIELDS = frozenset({"pending_activation", "pending_command_id"})
IDENTITY_FIELDS = ("token_id", "paired_token_id", "side", "condition_id")


@dataclass(frozen=True)
class MarketUniverseBuild:
    payload: dict[str, Any]
    exact_duplicates_removed: int
    conflicts_resolved: int


def load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"market source not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"market source is not valid JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"market source must be a JSON object: {path}")
    return payload


def _canonical_row(raw: Mapping[str, Any], *, source: str, section: str) -> dict[str, Any]:
    row = {
        str(key): value
        for key, value in raw.items()
        if str(key) not in TRANSIENT_FIELDS
    }
    token_id = str(row.get("token_id") or "").strip()
    paired_token_id = str(row.get("paired_token_id") or "").strip()
    if not token_id:
        raise ValueError(f"{source}.{section} contains a market without token_id")
    if not paired_token_id:
        raise ValueError(
            f"{source}.{section} token {token_id[-10:]} is missing paired_token_id"
        )
    if token_id == paired_token_id:
        raise ValueError(
            f"{source}.{section} token {token_id[-10:]} is paired with itself"
        )
    row["token_id"] = token_id
    row["paired_token_id"] = paired_token_id
    try:
        json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{source}.{section} token {token_id[-10:]} is not JSON serializable"
        ) from exc
    return row


def market_event_key(row: Mapping[str, Any]) -> str:
    token_id = str(row.get("token_id") or "").strip()
    paired_token_id = str(row.get("paired_token_id") or "").strip()
    return "pair:" + ":".join(sorted((token_id, paired_token_id)))


def _row_json(row: Mapping[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def normalize_market_source(
    payload: Mapping[str, Any],
    *,
    source: str,
    dedupe_exact: bool = False,
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    normalized = {section: [] for section in SECTIONS}
    events: dict[str, tuple[str, dict[str, Any]]] = {}
    tokens: dict[str, str] = {}
    removed = 0

    for section in SECTIONS:
        raw_rows = payload.get(section, [])
        if raw_rows is None:
            raw_rows = []
        if not isinstance(raw_rows, list):
            raise ValueError(f"{source}.{section} must be an array")
        for ordinal, raw in enumerate(raw_rows, start=1):
            if not isinstance(raw, Mapping):
                raise ValueError(f"{source}.{section}[{ordinal}] must be an object")
            row = _canonical_row(raw, source=source, section=section)
            token_id = row["token_id"]
            event_key = market_event_key(row)
            previous_token_event = tokens.get(token_id)
            if previous_token_event is not None and previous_token_event != event_key:
                raise ValueError(
                    f"{source} token {token_id[-10:]} belongs to multiple events"
                )
            previous = events.get(event_key)
            if previous is not None:
                previous_section, previous_row = previous
                if previous_section != section:
                    raise ValueError(
                        f"{source} event {event_key[-21:]} appears in day and night sections"
                    )
                if _row_json(previous_row) == _row_json(row) and dedupe_exact:
                    removed += 1
                    continue
                detail = (
                    "exact duplicate"
                    if _row_json(previous_row) == _row_json(row)
                    else "conflicting rows"
                )
                raise ValueError(
                    f"{source}.{section} event {event_key[-21:]} has {detail}"
                )
            tokens[token_id] = event_key
            events[event_key] = (section, row)
            normalized[section].append(row)

    for section in SECTIONS:
        normalized[section].sort(key=lambda row: market_event_key(row))
    return normalized, removed


def _identity_conflicts(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
) -> list[str]:
    conflicts = []
    for field in IDENTITY_FIELDS:
        left = str(first.get(field) or "").strip().casefold()
        right = str(second.get(field) or "").strip().casefold()
        if left and right and left != right:
            conflicts.append(field)
    return conflicts


def build_market_universe(
    sources: Sequence[tuple[str, Mapping[str, Any]]],
    *,
    prefer_source: str = "",
    dedupe_exact: bool = False,
) -> MarketUniverseBuild:
    if not sources:
        raise ValueError("at least one market source is required")
    normalized_sources = [(label.strip(), payload) for label, payload in sources]
    labels = [label for label, _ in normalized_sources]
    if any(not label for label in labels):
        raise ValueError("market source labels must not be blank")
    if len(set(labels)) != len(labels):
        raise ValueError("market source labels must be unique")
    preferred = prefer_source.strip()
    if preferred and preferred not in labels:
        raise ValueError(f"preferred source {preferred!r} is not present")

    ordered_sources = sorted(
        normalized_sources,
        key=lambda item: (0 if item[0] == preferred else 1, item[0]),
    )
    combined: dict[str, tuple[str, str, dict[str, Any]]] = {}
    removed = 0
    resolved = 0
    for label, payload in ordered_sources:
        normalized, source_removed = normalize_market_source(
            payload,
            source=label,
            dedupe_exact=dedupe_exact,
        )
        removed += source_removed
        for section in SECTIONS:
            for row in normalized[section]:
                event_key = market_event_key(row)
                previous = combined.get(event_key)
                if previous is None:
                    combined[event_key] = (label, section, row)
                    continue
                previous_label, previous_section, previous_row = previous
                if previous_section != section:
                    day_label = (
                        previous_label if previous_section == "markets" else label
                    )
                    night_label = (
                        previous_label
                        if previous_section == "night_markets"
                        else label
                    )
                    raise ValueError(
                        f"event {event_key[-21:]} is day in {day_label} and night in {night_label}"
                    )
                identity_conflicts = _identity_conflicts(previous_row, row)
                if identity_conflicts:
                    raise ValueError(
                        f"event {event_key[-21:]} has identity conflicts: "
                        + ", ".join(identity_conflicts)
                    )
                if _row_json(previous_row) == _row_json(row):
                    continue
                if not preferred:
                    raise ValueError(
                        f"event {event_key[-21:]} differs between {previous_label} and {label}; "
                        "set --prefer-source after review"
                    )
                if preferred not in {previous_label, label}:
                    raise ValueError(
                        f"event {event_key[-21:]} differs between {previous_label} and {label}, "
                        f"but preferred source {preferred} has no row for that event"
                    )
                resolved += 1
                if label == preferred:
                    combined[event_key] = (label, section, row)

    output: dict[str, Any] = {
        "schema_version": 1,
        "markets": [],
        "night_markets": [],
        "build": {
            "sources": sorted(labels),
            "preferred_source": preferred or None,
            "exact_duplicates_removed": removed,
            "conflicts_resolved": resolved,
        },
    }
    for event_key in sorted(combined):
        _label, section, row = combined[event_key]
        output[section].append(row)
    return MarketUniverseBuild(
        payload=output,
        exact_duplicates_removed=removed,
        conflicts_resolved=resolved,
    )


def apply_market_universe(
    base: Mapping[str, Any],
    universe: Mapping[str, Any],
) -> dict[str, Any]:
    normalized, _removed = normalize_market_source(
        universe,
        source="market_universe",
        dedupe_exact=False,
    )
    output = dict(base)
    output["markets"] = normalized["markets"]
    output["night_markets"] = normalized["night_markets"]
    return output
