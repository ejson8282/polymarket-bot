"""施工包02 · §1 指标纯函数 sanity 测试。"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from platforms.single_account.strategies import indicators as ind
from platforms.single_account.strategies.base import risk_qty


def test_ema_constant_series():
    s = pd.Series([5.0] * 30)
    out = ind.ema(s, 10)
    assert abs(out.iloc[0] - 5.0) < 1e-12  # adjust=False 递推自首根有定义
    assert abs(out.iloc[-1] - 5.0) < 1e-12


def test_atr_constant_range():
    high = pd.Series([101.0] * 40)
    low = pd.Series([99.0] * 40)
    close = pd.Series([100.0] * 40)
    out = ind.atr(high, low, close, 14)
    assert abs(out.iloc[-1] - 2.0) < 1e-9  # 恒定 TR=2


def test_rsi_extremes():
    up = pd.Series(np.arange(1.0, 40.0))       # 单边上涨 → RSI≈100
    down = pd.Series(np.arange(40.0, 1.0, -1)) # 单边下跌 → RSI≈0
    assert ind.rsi(up, 3).iloc[-1] > 99.0
    assert ind.rsi(down, 3).iloc[-1] < 1.0


def test_donchian_excludes_current_bar():
    high = pd.Series([10.0] * 10 + [20.0])
    out = ind.donchian_high(high, 5)
    assert out.iloc[-1] == 10.0  # 不含当前根的 20


def test_bollinger_and_bandwidth():
    close = pd.Series([100.0] * 25)
    mid, upper, lower, bw = ind.bollinger(close, 20, 2.0)
    assert abs(mid.iloc[-1] - 100.0) < 1e-12
    assert abs(upper.iloc[-1] - lower.iloc[-1]) < 1e-12  # 零方差→零带宽
    assert abs(bw.iloc[-1]) < 1e-12


def test_adx_trending_vs_flat():
    n = 80
    trend_high = pd.Series(np.arange(n, dtype=float) + 1.0)
    trend_low = trend_high - 1.0
    trend_close = trend_high - 0.5
    flat_high = pd.Series([101.0] * n)
    flat_low = pd.Series([99.0] * n)
    flat_close = pd.Series([100.0] * n)
    assert ind.adx(trend_high, trend_low, trend_close, 14).iloc[-1] > 60
    flat_adx = ind.adx(flat_high, flat_low, flat_close, 14).iloc[-1]
    assert np.isnan(flat_adx) or flat_adx < 10  # 无方向运动


def test_zscore_and_percentile():
    s = pd.Series(list(np.ones(29)) + [2.0])
    z = ind.zscore(s, 30)
    assert z.iloc[-1] > 3.0
    pct = ind.rolling_percentile_rank(s, 30)
    assert pct.iloc[-1] == 1.0


def test_vwap_daily_resets():
    ts = pd.Series([0, 3600, 86400 + 0, 86400 + 3600])
    price = pd.Series([10.0, 20.0, 30.0, 40.0])
    vol = pd.Series([1.0, 1.0, 1.0, 1.0])
    out = ind.vwap_daily(ts, price, vol)
    assert abs(out.iloc[1] - 15.0) < 1e-12
    assert abs(out.iloc[2] - 30.0) < 1e-12  # 新的一天重置
    assert abs(out.iloc[3] - 35.0) < 1e-12


def test_resample_ohlcv_15m_to_1h():
    idx = [i * 900 for i in range(8)]
    bars = pd.DataFrame({
        "open": [1, 2, 3, 4, 5, 6, 7, 8],
        "high": [2, 3, 4, 5, 6, 7, 8, 9],
        "low": [0, 1, 2, 3, 4, 5, 6, 7],
        "close": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5],
        "volume": [1] * 8,
    }, index=idx, dtype=float)
    out = ind.resample_ohlcv(bars, "1h")
    assert len(out) == 2
    assert out.iloc[0]["open"] == 1 and out.iloc[0]["high"] == 5
    assert out.iloc[0]["close"] == 4.5 and out.iloc[0]["volume"] == 4
    assert list(out.index) == [0, 3600]


def test_risk_qty_budget_and_cap():
    # 0.75% × 10000 / |100−98| = 37.5
    assert abs(risk_qty(0.0075, 10000, 100.0, 98.0) - 37.5) < 1e-12
    # 名义上限 15%:qty ≤ 1500/100 = 15
    assert abs(risk_qty(0.0075, 10000, 100.0, 98.0, notional_cap_pct=0.15) - 15.0) < 1e-12
    assert risk_qty(0.0075, 10000, 100.0, 100.0) == 0.0  # 零距离防除零
