from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_DIR = Path(__file__).resolve().parents[3]


def _subprocess_env() -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("PYTHONPYCACHEPREFIX", "/tmp/predictfun_pycache")
    return env


def _run(name: str, args: list[str], *, timeout: int = 90) -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, *args],
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
        env=_subprocess_env(),
        timeout=timeout,
    )
    output = "\n".join(x for x in [proc.stdout.strip(), proc.stderr.strip()] if x)
    return {
        "name": name,
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "output_tail": output[-4000:],
    }


def _run_runner_once() -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, "-m", "platforms.predictfun.maker.runner", "--once"],
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
        env=_subprocess_env(),
        timeout=90,
    )
    output = "\n".join(x for x in [proc.stdout.strip(), proc.stderr.strip()] if x)
    last_error = ""
    try:
        state = json.loads(proc.stdout.strip())
        last_error = str(state.get("last_error") or "")
    except Exception:
        last_error = "runner did not emit parseable JSON"
    return {
        "name": "runner_once",
        "ok": proc.returncode == 0 and not last_error,
        "returncode": proc.returncode,
        "last_error": last_error,
        "output_tail": output[-4000:],
    }


def run_smoke(*, include_ws: bool) -> dict[str, Any]:
    checks = [
        _run(
            "py_compile",
            [
                "-m",
                "py_compile",
                "platforms/predictfun/client.py",
                "platforms/predictfun/scanner.py",
                "platforms/predictfun/ws_watch.py",
                "platforms/predictfun/maker/dry_run.py",
                "platforms/predictfun/maker/executor.py",
                "platforms/predictfun/maker/intents.py",
                "platforms/predictfun/maker/reconcile.py",
                "platforms/predictfun/maker/runner.py",
                "platforms/predictfun/maker/selftest.py",
                "platforms/predictfun/maker/simulator.py",
                "platforms/predictfun/maker/risk.py",
                "platforms/predictfun/maker/research.py",
                "platforms/predictfun/maker/ops.py",
                "dashboard/predictfun_view.py",
            ],
            timeout=30,
        ),
        _run("selftest", ["-m", "platforms.predictfun.maker.selftest"], timeout=30),
        _run_runner_once(),
    ]
    if include_ws:
        checks.append(
            _run(
                "ws_smoke",
                ["-m", "platforms.predictfun.ws_watch", "--max-messages", "5", "--timeout-sec", "8"],
                timeout=20,
            )
        )
    return {
        "ok": all(row["ok"] for row in checks),
        "checks": checks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict.fun ops helpers.")
    sub = parser.add_subparsers(dest="command", required=True)
    smoke = sub.add_parser("smoke", help="Run PF compile/selftest/runner smoke checks.")
    smoke.add_argument("--include-ws", action="store_true")
    args = parser.parse_args()

    if args.command == "smoke":
        result = run_smoke(include_ws=bool(args.include_ws))
        print(json.dumps(result, indent=2))
        if not result["ok"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
