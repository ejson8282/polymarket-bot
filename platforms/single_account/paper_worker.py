from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from platforms.single_account.scorer import score_candidates
from platforms.single_account.signals import load_config, load_market_signals, resolve_path


_STOP = False


def _handle_stop(signum: int, frame: object) -> None:
    global _STOP
    _STOP = True


def run_once(config_path: Path) -> dict[str, Any]:
    cfg = load_config(config_path)
    signals = load_market_signals(config_path, cfg)
    decisions = score_candidates(cfg, signals)
    now = _utc_now()
    rows = [row.to_dict() for row in decisions]
    actionable = [row for row in rows if row.get("decision") == "allow"]
    state = {
        "ts": now,
        "mode": "paper_only",
        "pid": os.getpid(),
        "config_path": str(config_path),
        "summary": {
            "signals": len(signals),
            "decisions": len(rows),
            "actionable": len(actionable),
            "top_symbol": actionable[0]["symbol"] if actionable else "",
            "top_strategy": actionable[0]["strategy_label"] if actionable else "",
            "top_score": actionable[0]["score"] if actionable else 0.0,
            "skip_reasons": _count_by(rows, "reason"),
        },
        "decisions": rows[:50],
    }
    # 施工包01 §B7:state JSON 新增顶层 sim 键(其余输出原样保留,dashboard 不受影响)
    state["sim"] = {"db": "data/single_account_paper.db", "schema_version": 1}
    state_path = _output_path(config_path, cfg, "state_path", "../../data/single_account_paper_state.json")
    decisions_path = _output_path(config_path, cfg, "decisions_path", "../../data/single_account_decisions.jsonl")
    _write_json(state_path, state)
    _append_jsonl(decisions_path, {"ts": now, "summary": state["summary"], "decisions": rows[:50]})
    _record_decisions_db(config_path, cfg, rows)  # 施工包01 §B7:decisions 逐行入库
    return state


def _record_decisions_db(config_path: Path, cfg: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    """把本轮 decisions 写进 sim 库 decisions 表(taken = decision=='allow')。
    入库失败不影响原有输出与循环(§B7 兼容要求)。"""
    try:
        from platforms.single_account.sim import persistence as sim_persistence

        sim_cfg = cfg.get("sim") if isinstance(cfg.get("sim"), dict) else {}
        db_raw = str(sim_cfg.get("db") or "../../data/single_account_paper.db")
        conn = sim_persistence.open_paper_db(resolve_path(config_path, db_raw))
        ts_epoch = int(time.time())
        with conn:
            for row in rows:
                sim_persistence.insert_decision(
                    conn,
                    ts_epoch,
                    str(row.get("strategy") or ""),
                    str(row.get("symbol") or ""),
                    str(row.get("side") or ""),
                    json.dumps(row, ensure_ascii=False),
                    1 if row.get("decision") == "allow" else 0,
                    str(row.get("reason") or ""),
                )
        conn.close()
    except Exception:
        pass


def run_loop(config_path: Path, *, interval_sec: float, once: bool = False) -> dict[str, Any]:
    cfg = load_config(config_path)
    pid_path = _output_path(config_path, cfg, "pid_path", "../../data/.single_account_paper.pid")
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()), encoding="utf-8")
    state: dict[str, Any] = {}
    try:
        while not _STOP:
            state = run_once(config_path)
            if once:
                break
            slept = 0.0
            while slept < interval_sec and not _STOP:
                step = min(1.0, interval_sec - slept)
                time.sleep(step)
                slept += step
    finally:
        try:
            if pid_path.exists() and pid_path.read_text(encoding="utf-8").strip() == str(os.getpid()):
                pid_path.unlink()
        except Exception:
            pass
    return state


def _output_path(config_path: Path, cfg: dict[str, Any], key: str, default: str) -> Path:
    out = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    return resolve_path(config_path, str(out.get(key) or default))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    default_config = Path(__file__).with_name("config.json")
    parser = argparse.ArgumentParser(description="Single-account paper strategy worker.")
    parser.add_argument("--config", default=str(default_config))
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    config_path = Path(args.config).resolve()
    cfg = load_config(config_path)
    runner_cfg = cfg.get("runner") if isinstance(cfg.get("runner"), dict) else {}
    interval_sec = args.interval_sec if args.interval_sec > 0 else float(runner_cfg.get("interval_sec") or 300)
    state = run_loop(config_path, interval_sec=interval_sec, once=args.once)
    print(json.dumps(state, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

