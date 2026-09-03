"""Four student-owned boundaries used by the live platform.

Run ``uv run pytest starter-tests -q`` while completing these functions.  Do
not change their signatures: Kafka, Delta, Feast and ``/ready`` call them.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lab28_platform.contracts import IngestionEvent


def event_headers(
    traceparent: str | None, idempotency_key: str
) -> list[tuple[str, bytes]]:
    """Return byte-valued Kafka headers for trace and replay correlation.

    ``idempotency-key`` is always required.  Omit ``traceparent`` when no trace
    is active rather than sending an empty, invalid W3C header.
    """
    headers: list[tuple[str, bytes]] = [
        ("idempotency-key", idempotency_key.encode("utf-8")),
    ]
    if traceparent is not None:
        headers.append(("traceparent", traceparent.encode("utf-8")))
    return headers


def dedupe_latest(events: Iterable[IngestionEvent]) -> list[IngestionEvent]:
    """Return one newest event per idempotency key, in deterministic key order.

    Compare ``(occurred_at, event_id)`` so ties do not depend on Kafka delivery
    order.  The Spark Delta MERGE calls this through ``delta_store``.
    """
    # Read input exactly once - collect all events
    all_events = list(events)
    if not all_events:
        return []

    # Group by idempotency_key and find the latest per key
    latest_by_key: dict[str, IngestionEvent] = {}
    for event in all_events:
        key = event.idempotency_key
        existing = latest_by_key.get(key)
        if existing is None:
            latest_by_key[key] = event
        else:
            # Compare (occurred_at, event_id) - higher is newer
            if (event.occurred_at, event.event_id) > (
                existing.occurred_at,
                existing.event_id,
            ):
                latest_by_key[key] = event

    # Return in deterministic key order (sorted by idempotency_key)
    return [latest_by_key[key] for key in sorted(latest_by_key.keys())]


def feast_online_request(asker_id: str) -> dict[str, Any]:
    """Build the Feast ``/get-online-features`` request for ``asker_activity_v1``."""
    from lab28_platform.contracts import FEATURE_REFS

    return {
        "entities": {"asker_id": [asker_id]},
        "features": list(FEATURE_REFS),
        "full_feature_names": False,
    }


def readiness_status(probes: Iterable[dict[str, Any]]) -> str:
    """Return ``ready``, ``degraded`` or ``not_ready`` from probe severity."""
    # Convert to list to allow multiple iterations
    probe_list = list(probes)

    # Priority 1: any mandatory probe failed → not_ready
    for probe in probe_list:
        if probe.get("mandatory", False) and not probe.get("ready", False):
            return "not_ready"

    # Priority 2: any optional probe failed → degraded
    for probe in probe_list:
        if not probe.get("mandatory", False) and not probe.get("ready", False):
            return "degraded"

    # Priority 3: all probes passed
    return "ready"
