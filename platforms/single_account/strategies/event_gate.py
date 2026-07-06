"""施工包02 · §3 事件闸门(全局否决层,不是策略)。

数据源优先级:paper 库 `events` 表(施工包03 之后由 GPT 管线填;当前不存在则
自动退化)→ 静态宏观日历 `data/macro_calendar.json`,格式:
  [{"symbols": [], "type": "cpi|fomc|nfp|macro|earnings", "impact": "high",
    "window_start_ts": 0, "window_end_ts": 0}]

规则(§3):
- 宏观事件(CPI/FOMC/NFP/macro):窗口 ±30min 禁**全部** symbol 的新开仓;
- 财报(earnings):事件窗前 24h 至窗后 2h 禁**该 symbol**,且窗前即要求强制平净;
- 失败安全:events 表读取失败 → 只用静态日历;日历文件不存在 = 无事件(放行);
  日历存在但解析失败 → **封锁全部开仓**(绝不因数据坏了而放开闸门)。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CALENDAR_PATH = REPO_ROOT / "data" / "macro_calendar.json"

MACRO_TYPES = {"macro", "cpi", "fomc", "nfp"}
EARNINGS_TYPES = {"earnings"}
MACRO_BUFFER_S = 30 * 60
EARNINGS_PRE_S = 24 * 3600
EARNINGS_POST_S = 2 * 3600


@dataclass
class EventWindow:
    symbols: List[str]          # 空 = 全部 symbol
    type: str
    impact: str
    window_start_ts: int
    window_end_ts: int

    def applies_to(self, symbol: str) -> bool:
        return not self.symbols or symbol in self.symbols


class EventGate:
    def __init__(self, calendar_path: Optional[Path] = None,
                 paper_conn: Optional[sqlite3.Connection] = None) -> None:
        self.calendar_path = calendar_path or DEFAULT_CALENDAR_PATH
        self.paper_conn = paper_conn
        self._events: List[EventWindow] = []
        self._calendar_broken = False
        self.reload()

    # ---------- 数据装载(失败安全) ----------

    def reload(self) -> None:
        events: List[EventWindow] = []
        db_ok = False
        if self.paper_conn is not None:
            try:
                rows = self.paper_conn.execute(
                    "SELECT symbols_json, type, impact, window_start_ts, window_end_ts "
                    "FROM events").fetchall()
                for symbols_json, type_, impact, start, end in rows:
                    events.append(EventWindow(
                        symbols=[str(s) for s in json.loads(symbols_json or "[]")],
                        type=str(type_ or "").lower(), impact=str(impact or ""),
                        window_start_ts=int(start), window_end_ts=int(end)))
                db_ok = True
            except Exception:
                events = []  # events 表缺失/损坏 → 退化为只用静态日历(§3 失败模式)
        del db_ok  # 静态日历始终叠加兜底;events 表仅是额外来源
        self._calendar_broken = False
        try:
            if self.calendar_path.exists():
                raw = json.loads(self.calendar_path.read_text(encoding="utf-8"))
                for item in raw:
                    events.append(EventWindow(
                        symbols=[str(s) for s in (item.get("symbols") or [])],
                        type=str(item.get("type") or "").lower(),
                        impact=str(item.get("impact") or ""),
                        window_start_ts=int(item.get("window_start_ts")),
                        window_end_ts=int(item.get("window_end_ts"))))
        except Exception:
            self._calendar_broken = True  # 解析失败 → 封锁全部(见 blocked)
        self._events = events

    # ---------- 查询接口 ----------

    def blocked(self, symbol: str, ts: int) -> Tuple[bool, str]:
        """开仓否决查询。命中 → (True, reason)。平仓不经此闸门(组合层保证)。"""
        if self._calendar_broken:
            return True, "event_gate_calendar_read_error_fail_safe"
        for ev in self._events:
            if ev.type in MACRO_TYPES:
                if ev.window_start_ts - MACRO_BUFFER_S <= ts <= ev.window_end_ts + MACRO_BUFFER_S:
                    return True, f"macro_window:{ev.type}"
            elif ev.type in EARNINGS_TYPES and ev.applies_to(symbol):
                if ev.window_start_ts - EARNINGS_PRE_S <= ts <= ev.window_end_ts + EARNINGS_POST_S:
                    return True, f"earnings_window:{symbol}"
        return False, ""

    def force_flat(self, symbol: str, ts: int) -> Tuple[bool, str]:
        """财报前强制平净(§3):进入财报封锁窗(事件前 24h 起)即要求清仓该 symbol。"""
        if self._calendar_broken:
            return False, ""  # 数据坏时只封开仓,不强平(避免坏数据触发误平仓)
        for ev in self._events:
            if ev.type in EARNINGS_TYPES and ev.applies_to(symbol):
                if ev.window_start_ts - EARNINGS_PRE_S <= ts <= ev.window_end_ts + EARNINGS_POST_S:
                    return True, f"earnings_force_flat:{symbol}"
        return False, ""
