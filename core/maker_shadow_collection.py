"""Persistent metrics for read-only maker shadow comparisons."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


FRESHNESS_VIOLATIONS = {"missing_book_age", "stale_book"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def source_fingerprint(payloads: Iterable[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for name, payload in sorted(payloads):
        encoded_name = name.encode("utf-8")
        digest.update(len(encoded_name).to_bytes(4, "big"))
        digest.update(encoded_name)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def initialize_database(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS shadow_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                venue TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                source_timestamps_json TEXT NOT NULL,
                desired_orders INTEGER NOT NULL,
                actual_orders INTEGER NOT NULL,
                books INTEGER NOT NULL,
                fresh INTEGER NOT NULL,
                matched INTEGER NOT NULL,
                safety_matched INTEGER NOT NULL,
                actions_matched INTEGER NOT NULL,
                python_can_execute INTEGER NOT NULL,
                rust_can_execute INTEGER NOT NULL,
                snapshot_path TEXT NOT NULL,
                python_result_json TEXT NOT NULL,
                rust_result_json TEXT NOT NULL,
                UNIQUE(venue, source_fingerprint)
            );

            CREATE INDEX IF NOT EXISTS idx_shadow_samples_venue_time
                ON shadow_samples(venue, observed_at);

            CREATE TABLE IF NOT EXISTS shadow_collector_status (
                venue TEXT PRIMARY KEY,
                last_poll_at TEXT NOT NULL,
                last_new_state_at TEXT,
                last_fingerprint TEXT,
                last_error TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS shadow_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                venue TEXT NOT NULL,
                error TEXT NOT NULL
            );
            """
        )


def last_fingerprint(database: Path, venue: str) -> str:
    initialize_database(database)
    with _connect(database) as connection:
        row = connection.execute(
            "SELECT last_fingerprint FROM shadow_collector_status WHERE venue = ?",
            (venue,),
        ).fetchone()
    return str(row["last_fingerprint"] or "") if row else ""


def record_comparison(
    *,
    database: Path,
    venue: str,
    fingerprint: str,
    snapshot: dict[str, Any],
    comparison: dict[str, Any],
    snapshot_path: Path,
    observed_at: str | None = None,
) -> bool:
    initialize_database(database)
    timestamp = observed_at or utc_now()
    metadata = snapshot.get("metadata") if isinstance(snapshot.get("metadata"), dict) else {}
    python_result = comparison.get("python") if isinstance(comparison.get("python"), dict) else {}
    rust_result = comparison.get("rust") if isinstance(comparison.get("rust"), dict) else {}
    violation_codes = {
        str(row[0])
        for row in python_result.get("risk_violations") or []
        if isinstance(row, (list, tuple)) and row
    }
    fresh = bool(snapshot.get("desired")) and not (
        violation_codes & FRESHNESS_VIOLATIONS
    )
    values = (
        timestamp,
        venue,
        fingerprint,
        _json(metadata.get("source_ts")),
        len(snapshot.get("desired") or []),
        len(snapshot.get("actual") or []),
        len(snapshot.get("books") or []),
        int(fresh),
        int(bool(comparison.get("matched"))),
        int(bool(comparison.get("safety_matched"))),
        int(bool(comparison.get("actions_matched"))),
        int(bool(python_result.get("can_execute"))),
        int(bool(rust_result.get("can_execute"))),
        str(snapshot_path),
        _json(python_result),
        _json(rust_result),
    )
    with _connect(database) as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO shadow_samples (
                observed_at, venue, source_fingerprint, source_timestamps_json,
                desired_orders, actual_orders, books, fresh, matched,
                safety_matched, actions_matched, python_can_execute,
                rust_can_execute, snapshot_path, python_result_json,
                rust_result_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        inserted = cursor.rowcount == 1
        connection.execute(
            """
            INSERT INTO shadow_collector_status (
                venue, last_poll_at, last_new_state_at, last_fingerprint, last_error
            ) VALUES (?, ?, ?, ?, '')
            ON CONFLICT(venue) DO UPDATE SET
                last_poll_at = excluded.last_poll_at,
                last_new_state_at = CASE
                    WHEN ? THEN excluded.last_new_state_at
                    ELSE shadow_collector_status.last_new_state_at
                END,
                last_fingerprint = excluded.last_fingerprint,
                last_error = ''
            """,
            (venue, timestamp, timestamp, fingerprint, int(inserted)),
        )
    return inserted


def record_unchanged_poll(
    *,
    database: Path,
    venue: str,
    fingerprint: str,
    observed_at: str | None = None,
) -> None:
    initialize_database(database)
    timestamp = observed_at or utc_now()
    with _connect(database) as connection:
        connection.execute(
            """
            INSERT INTO shadow_collector_status (
                venue, last_poll_at, last_new_state_at, last_fingerprint, last_error
            ) VALUES (?, ?, NULL, ?, '')
            ON CONFLICT(venue) DO UPDATE SET
                last_poll_at = excluded.last_poll_at,
                last_fingerprint = excluded.last_fingerprint,
                last_error = ''
            """,
            (venue, timestamp, fingerprint),
        )


def record_error(
    *,
    database: Path,
    venue: str,
    error: str,
    observed_at: str | None = None,
) -> None:
    initialize_database(database)
    timestamp = observed_at or utc_now()
    message = str(error).strip()[:2000]
    with _connect(database) as connection:
        connection.execute(
            "INSERT INTO shadow_errors (observed_at, venue, error) VALUES (?, ?, ?)",
            (timestamp, venue, message),
        )
        connection.execute(
            """
            INSERT INTO shadow_collector_status (
                venue, last_poll_at, last_new_state_at, last_fingerprint, last_error
            ) VALUES (?, ?, NULL, NULL, ?)
            ON CONFLICT(venue) DO UPDATE SET
                last_poll_at = excluded.last_poll_at,
                last_error = excluded.last_error
            """,
            (venue, timestamp, message),
        )


def summary(database: Path, venue: str | None = None) -> dict[str, Any]:
    initialize_database(database)
    parameters: tuple[Any, ...] = ()
    where = ""
    if venue:
        where = "WHERE venue = ?"
        parameters = (venue,)
    with _connect(database) as connection:
        rows = connection.execute(
            f"""
            SELECT
                venue,
                COUNT(*) AS samples,
                SUM(fresh) AS fresh_samples,
                SUM(matched) AS matched_samples,
                SUM(CASE WHEN matched = 0 THEN 1 ELSE 0 END) AS mismatched_samples,
                SUM(CASE WHEN safety_matched = 0 THEN 1 ELSE 0 END) AS safety_mismatches,
                SUM(CASE WHEN actions_matched = 0 THEN 1 ELSE 0 END) AS action_mismatches,
                SUM(CASE WHEN fresh = 1 AND matched = 1 THEN 1 ELSE 0 END) AS fresh_matched,
                MIN(observed_at) AS first_observed_at,
                MAX(observed_at) AS last_observed_at
            FROM shadow_samples
            {where}
            GROUP BY venue
            ORDER BY venue
            """,
            parameters,
        ).fetchall()
        statuses = connection.execute(
            f"""
            SELECT venue, last_poll_at, last_new_state_at, last_fingerprint, last_error
            FROM shadow_collector_status
            {where}
            ORDER BY venue
            """,
            parameters,
        ).fetchall()
        error_rows = connection.execute(
            f"SELECT venue, COUNT(*) AS errors FROM shadow_errors {where} GROUP BY venue",
            parameters,
        ).fetchall()

    errors_by_venue = {str(row["venue"]): int(row["errors"] or 0) for row in error_rows}
    venue_rows = []
    for row in rows:
        samples = int(row["samples"] or 0)
        fresh_samples = int(row["fresh_samples"] or 0)
        matched_samples = int(row["matched_samples"] or 0)
        fresh_matched = int(row["fresh_matched"] or 0)
        venue_name = str(row["venue"])
        venue_rows.append(
            {
                "venue": venue_name,
                "samples": samples,
                "fresh_samples": fresh_samples,
                "matched_samples": matched_samples,
                "mismatched_samples": int(row["mismatched_samples"] or 0),
                "difference_rate": _rate(samples - matched_samples, samples),
                "fresh_difference_rate": _rate(
                    fresh_samples - fresh_matched,
                    fresh_samples,
                ),
                "safety_mismatches": int(row["safety_mismatches"] or 0),
                "action_mismatches": int(row["action_mismatches"] or 0),
                "errors": errors_by_venue.get(venue_name, 0),
                "first_observed_at": row["first_observed_at"],
                "last_observed_at": row["last_observed_at"],
            }
        )
    return {
        "generated_at": utc_now(),
        "database": str(database),
        "venues": venue_rows,
        "status": [dict(row) for row in statuses],
    }


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator
