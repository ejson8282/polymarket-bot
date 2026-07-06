"""施工包05 · 5B pmbot 横幅 + 账号矩阵 + Single Account 决策面板(纯构造层)。

视觉契约:docs/dashboard_spec/latitude_console_full.html(pm 页)与
single_account_cockpit.html。本模块只读现有状态文件/DB,不写任何东西;
HTML 构造均为纯函数,app.py / predictfun_view.py 以 st.markdown 渲染。
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import html as html_mod

PM_CSS = """
<style>
:root{--pm-bg:#0E1116;--pm-panel:#151B23;--pm-line:#242E3A;--pm-text:#E8EDF2;
  --pm-muted:#8B98A9;--pm-faint:#5A6676;--pm-ok:#46B26B;--pm-warn:#E7C547;
  --pm-danger:#E5484D;--pm-mono:"IBM Plex Mono","SF Mono",Menlo,Consolas,monospace}
.pm-banner{display:flex;align-items:center;gap:14px;padding:10px 16px;border-radius:8px;
  margin:0 0 10px;font-family:var(--pm-mono);font-weight:700;letter-spacing:.12em;font-size:15px}
.pm-banner.mainnet{background:rgba(229,72,77,.16);border:1px solid var(--pm-danger);color:#FF8A8E}
.pm-banner.testnet{background:rgba(59,74,94,.35);border:1px solid #3B4A5E;color:#AFC3DC}
.pm-banner .pm-sub{font-size:11px;font-weight:400;letter-spacing:.05em;color:var(--pm-muted);flex:1}
.pm-matrix{width:100%;border-collapse:collapse;font-size:12.5px;color:var(--pm-text)}
.pm-matrix th{font-family:var(--pm-mono);font-size:10.5px;font-weight:500;color:var(--pm-faint);
  text-align:left;letter-spacing:.06em;padding:6px 8px;border-bottom:1px solid var(--pm-line)}
.pm-matrix td{padding:8px;border-bottom:1px solid #1A222C;vertical-align:middle;
  font-family:var(--pm-mono);font-variant-numeric:tabular-nums}
.pm-chip{font-family:var(--pm-mono);font-size:10.5px;padding:1px 7px;border-radius:99px;
  border:1px solid var(--pm-line);color:var(--pm-muted)}
.pm-chip.ok{color:var(--pm-ok);border-color:rgba(70,178,107,.4)}
.pm-chip.warn{color:var(--pm-warn);border-color:rgba(231,197,71,.4)}
.pm-chip.danger{color:var(--pm-danger);border-color:rgba(229,72,77,.5)}
.pm-skipbar{display:flex;height:20px;border-radius:6px;overflow:hidden;
  font-family:var(--pm-mono);font-size:10.5px;margin:4px 0}
.pm-skipbar i{display:flex;align-items:center;justify-content:center;color:#0E1116;
  font-weight:600;font-style:normal;white-space:nowrap;overflow:hidden}
.pm-skiplegend{display:flex;gap:12px;flex-wrap:wrap;font-family:var(--pm-mono);
  font-size:11px;color:var(--pm-muted);margin-top:6px}
.pm-eqcard{background:var(--pm-panel);border:1px solid var(--pm-line);border-radius:10px;
  padding:14px 16px;display:flex;gap:28px;flex-wrap:wrap}
.pm-eqcard .cell .l{font-size:12px;color:var(--pm-muted)}
.pm-eqcard .cell .v{font-family:var(--pm-mono);font-size:20px;font-weight:600;margin-top:2px}
.pm-placeholder{color:var(--pm-faint);font-family:var(--pm-mono);font-size:12px;
  border:1px dashed var(--pm-line);border-radius:8px;padding:12px 16px}
</style>
"""

_SKIP_COLORS = ["#5EC8D8", "#ECA13C", "#8B98A9", "#46B26B", "#E7C547", "#B085E8"]


def _esc(value: Any) -> str:
    return html_mod.escape(str(value))


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# §5B.1 横幅
# ---------------------------------------------------------------------------

def banner_html(kind: str) -> str:
    """kind: 'mainnet' → 红 MAINNET · LIVE;'testnet' → 蓝灰 TESTNET · DRY-RUN。
    字样与配色照 latitude_console_full 模板 pill.live / 蓝灰系。"""
    if kind == "mainnet":
        return (PM_CSS + '<div class="pm-banner mainnet">MAINNET · LIVE'
                '<span class="pm-sub">真实资金环境 — 操作前确认账号与市场</span></div>')
    return (PM_CSS + '<div class="pm-banner testnet">TESTNET · DRY-RUN'
            '<span class="pm-sub">测试网模拟环境 — 不产生真实交易</span></div>')


# ---------------------------------------------------------------------------
# §5B.2 账号矩阵
# ---------------------------------------------------------------------------

def account_matrix_rows(states: Dict[int, dict], alive: Dict[int, bool],
                        paused: Dict[int, bool],
                        rewards: Optional[dict] = None) -> List[dict]:
    """从 engine_state_N.json 集合构造矩阵行;行数随 roster 自动扩展。"""
    rows: List[dict] = []
    rewards = rewards or {}
    for idx in sorted(states):
        state = states.get(idx) or {}
        markets = state.get("markets") if isinstance(state.get("markets"), dict) else {}
        live_orders = 0
        for market in markets.values():
            if isinstance(market, dict):
                orders = market.get("live_orders") or market.get("orders")
                if isinstance(orders, list):
                    live_orders += len(orders)
                else:
                    live_orders += int(_f(market.get("live_order_count")))
        fills = state.get("fills") if isinstance(state.get("fills"), list) else []
        # 引擎 fills[].ts 为 unix 秒(engine._fills_record 口径);近 24h 记「今日」
        import time as _time

        cutoff = _time.time() - 86400
        fills_today = [f for f in fills if isinstance(f, dict) and _f(f.get("ts")) >= cutoff]
        volume_today = sum(abs(_f(f.get("price")) * _f(f.get("size"))) for f in fills_today)
        pnl_today = sum(_f(f.get("pnl")) for f in fills_today if f.get("pnl") is not None)
        sibling = state.get("sibling_registry") if isinstance(state.get("sibling_registry"), dict) else {}
        reward_entry = rewards.get(str(idx)) or rewards.get(idx) or {}
        if paused.get(idx):
            status, status_cls = "已暂停", "warn"
        elif alive.get(idx):
            status, status_cls = "运行中", "ok"
        else:
            status, status_cls = "已停止", "danger"
        funder = str(state.get("funder") or "")
        rows.append({
            "idx": idx,
            "funder_short": (funder[:6] + "…" + funder[-4:]) if len(funder) > 12 else (funder or f"acct {idx}"),
            "status": status, "status_cls": status_cls,
            "balance": state.get("balance"),
            "live_orders": live_orders,
            "fills_today": len(fills_today),
            "volume_today": volume_today,
            "pnl_today": pnl_today if fills_today else None,
            "rewards": reward_entry.get("total") or reward_entry.get("earnings"),
            "sibling_conflicts": sibling.get("conflicts_detected"),
            "sibling_mode": sibling.get("mode"),
        })
    return rows


def account_matrix_html(rows: List[dict]) -> str:
    if not rows:
        return PM_CSS + '<div class="pm-placeholder">暂无账号状态文件(engine_state_N.json)</div>'
    body = []
    for r in rows:
        balance = f"${_f(r['balance']):,.2f}" if r["balance"] is not None else "—"
        pnl = f"${r['pnl_today']:,.2f}" if r["pnl_today"] is not None else "—"
        reward = f"{_f(r['rewards']):,.2f}" if r["rewards"] is not None else "—"
        sibling = ("—" if r["sibling_conflicts"] is None
                   else f"{r['sibling_conflicts']}({_esc(r['sibling_mode'] or '')})")
        body.append(
            f'<tr><td>#{r["idx"]}</td><td>{_esc(r["funder_short"])}</td>'
            f'<td><span class="pm-chip {r["status_cls"]}">{_esc(r["status"])}</span></td>'
            f'<td>{balance}</td><td>{r["live_orders"]}</td>'
            f'<td>${r["volume_today"]:,.0f} / {r["fills_today"]}</td>'
            f'<td>{pnl}</td><td>{reward}</td><td>{sibling}</td></tr>')
    return (PM_CSS + '<table class="pm-matrix"><thead><tr>'
            '<th>#</th><th>FUNDER</th><th>状态</th><th>抵押/权益</th><th>挂单</th>'
            '<th>今日成交额 / 笔</th><th>今日 PNL</th><th>奖励估算</th>'
            '<th>SIBLING 冲突</th></tr></thead><tbody>' + "".join(body)
            + '</tbody></table>')


# ---------------------------------------------------------------------------
# §5B.3 Single Account 决策面板(01/02 已合并;无 DB 优雅降级)
# ---------------------------------------------------------------------------

def _paper_db_path(state_json_path: Path, repo_dir: Path) -> Optional[Path]:
    """从 paper state JSON 的顶层 sim 键(施工包01·B7)解析 DB 路径。"""
    try:
        state = json.loads(state_json_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    sim = state.get("sim") if isinstance(state.get("sim"), dict) else {}
    raw = str(sim.get("db") or "data/single_account_paper.db")
    path = Path(raw)
    return path if path.is_absolute() else repo_dir / path


def skip_reason_html(db_path: Optional[Path], limit: int = 6) -> str:
    """skip 原因分布条(decisions 表聚合);DB/表缺失 → 「待一期」占位,不报错。"""
    placeholder = (PM_CSS + '<div class="pm-placeholder">skip 原因分布 · 待一期'
                   '(模拟器 decisions 表未就绪)</div>')
    if not db_path or not Path(db_path).exists():
        return placeholder
    try:
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT COALESCE(NULLIF(skip_reason,''),'(其他)') reason, COUNT(*) n "
            "FROM decisions WHERE taken=0 GROUP BY reason ORDER BY n DESC LIMIT ?",
            (limit,)).fetchall()
        conn.close()
    except sqlite3.Error:
        return placeholder
    if not rows:
        return (PM_CSS + '<div class="pm-placeholder">decisions 表暂无 skip 记录</div>')
    total = sum(n for _, n in rows)
    segments = []
    legend = []
    for i, (reason, n) in enumerate(rows):
        color = _SKIP_COLORS[i % len(_SKIP_COLORS)]
        width = max(3.0, n / total * 100.0)
        segments.append(f'<i style="width:{width:.1f}%;background:{color}">{n}</i>')
        legend.append(f'<span><i style="display:inline-block;width:10px;height:8px;'
                      f'border-radius:2px;background:{color};margin-right:5px"></i>'
                      f'{_esc(reason)} · {n}</span>')
    return (PM_CSS + '<div class="pm-skipbar">' + "".join(segments) + '</div>'
            + '<div class="pm-skiplegend">' + "".join(legend) + '</div>')


def equity_summary_html(db_path: Optional[Path]) -> str:
    """虚拟权益摘要卡(equity_snapshots 最新行 + MDD);缺 DB → 「待一期」占位。"""
    placeholder = (PM_CSS + '<div class="pm-placeholder">虚拟权益摘要 · 待一期'
                   '(模拟器 equity_snapshots 未就绪)</div>')
    if not db_path or not Path(db_path).exists():
        return placeholder
    try:
        conn = sqlite3.connect(str(db_path))
        latest = conn.execute(
            "SELECT ts, equity, cash, unrealized FROM equity_snapshots "
            "ORDER BY ts DESC LIMIT 1").fetchone()
        series = [row[0] for row in conn.execute(
            "SELECT equity FROM equity_snapshots ORDER BY ts")]
        conn.close()
    except sqlite3.Error:
        return placeholder
    if not latest:
        return (PM_CSS + '<div class="pm-placeholder">equity_snapshots 暂无数据'
                '(先跑一次回放)</div>')
    peak = 0.0
    mdd = 0.0
    for equity in series:
        peak = max(peak, equity)
        if peak > 0:
            mdd = max(mdd, 1.0 - equity / peak)
    ts, equity, cash, unrealized = latest

    def cell(label: str, value: str) -> str:
        return (f'<div class="cell"><div class="l">{label}</div>'
                f'<div class="v">{value}</div></div>')

    from datetime import datetime, timezone
    when = datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%m-%d %H:%M UTC")
    return (PM_CSS + '<div class="pm-eqcard">'
            + cell("虚拟权益", f"${_f(equity):,.2f}")
            + cell("现金", f"${_f(cash):,.2f}")
            + cell("未实现", f"${_f(unrealized):,.2f}")
            + cell("MDD", f"{mdd * 100:.2f}%")
            + cell("快照时间", when)
            + '</div>')
