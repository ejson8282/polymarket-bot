"""Shared Python/Rust shadow comparison helpers.

This module only evaluates normalized JSON snapshots. It has no exchange,
signing, cancellation, or order-placement capability.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from core.maker_shadow_reference import canonical_result, evaluate_case


def run_rust_case(binary: Path, case_path: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [str(binary), str(case_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Rust dry-run failed for {case_path.name}: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )
    result = json.loads(completed.stdout)
    if not isinstance(result, dict):
        raise ValueError(f"Rust dry-run returned a non-object for {case_path.name}")
    return result


def compare_case(binary: Path, case_path: Path) -> dict[str, Any]:
    case = json.loads(case_path.read_text(encoding="utf-8"))
    if not isinstance(case, dict):
        raise ValueError(f"expected a JSON object: {case_path}")
    python_result = evaluate_case(case)
    rust_result = run_rust_case(binary, case_path)
    python_canonical = canonical_result(python_result)
    rust_canonical = canonical_result(rust_result)
    return comparison_result(
        case_name=case_path.name,
        python_canonical=python_canonical,
        rust_canonical=rust_canonical,
    )


def comparison_result(
    *,
    case_name: str,
    python_canonical: dict[str, Any],
    rust_canonical: dict[str, Any],
) -> dict[str, Any]:
    safety_fields = (
        "can_execute",
        "risk_allowed",
        "risk_violations",
        "error",
    )
    action_fields = (
        "actions",
        "unmanaged_order_ids",
        "warnings",
    )
    safety_matched = all(
        python_canonical.get(field) == rust_canonical.get(field)
        for field in safety_fields
    )
    actions_matched = all(
        python_canonical.get(field) == rust_canonical.get(field)
        for field in action_fields
    )
    return {
        "case": case_name,
        "matched": python_canonical == rust_canonical,
        "safety_matched": safety_matched,
        "actions_matched": actions_matched,
        "python": python_canonical,
        "rust": rust_canonical,
    }
