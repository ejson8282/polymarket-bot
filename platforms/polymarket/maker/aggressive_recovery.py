"""Pure recovery policy for aggressive LP WATCH/QUARANTINE states."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RecoveryDecision:
    ready: bool
    good_samples: int
    reason: str


def evaluate_recovery_sample(
    *,
    now: float,
    timer_expired: bool,
    snapshot_fresh: bool,
    book_valid: bool,
    eligibility_ok: bool,
    last_sample_at: float,
    good_samples: int,
    required_samples: int,
    sample_interval_sec: float,
) -> RecoveryDecision:
    """Require spaced, consecutive healthy evidence before re-entering."""
    if not timer_expired:
        return RecoveryDecision(False, 0, "cooldown_active")
    if not snapshot_fresh:
        return RecoveryDecision(False, 0, "snapshot_stale")
    if not book_valid:
        return RecoveryDecision(False, 0, "book_invalid")
    if not eligibility_ok:
        return RecoveryDecision(False, 0, "eligibility_not_restored")
    if last_sample_at > 0 and now - last_sample_at < sample_interval_sec:
        return RecoveryDecision(False, good_samples, "waiting_next_sample")
    samples = max(0, int(good_samples)) + 1
    required = max(1, int(required_samples))
    return RecoveryDecision(
        samples >= required,
        samples,
        "healthy" if samples >= required else "collecting_healthy_samples",
    )
