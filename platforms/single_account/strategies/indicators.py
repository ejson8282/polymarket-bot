"""施工包02 · §1 技术指标(纯函数,numpy/pandas,可单测;不用 TA-Lib)。

输入均为 pandas Series/DataFrame,输出与输入等长(暖机期为 NaN)。
Wilder 平滑用 ewm(alpha=1/period, adjust=False),与主流平台口径一致。
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def sma(values: pd.Series, period: int) -> pd.Series:
    return values.rolling(period, min_periods=period).mean()


def ema(values: pd.Series, period: int) -> pd.Series:
    """EMA(adjust=False 递推)自首根即有定义,与 TradingView 口径一致;
    否则 4h EMA200 需 33 天暖机,规格 §4 的 30 天回放将永远取不到值。"""
    return values.ewm(span=period, adjust=False, min_periods=1).mean()


def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    return pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    tr = true_range(high, low, close)
    return tr.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()


def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    up = high.diff()
    down = -low.diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=high.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=high.index)
    tr_smooth = true_range(high, low, close).ewm(alpha=1.0 / period, adjust=False,
                                                 min_periods=period).mean()
    plus_di = 100.0 * plus_dm.ewm(alpha=1.0 / period, adjust=False,
                                  min_periods=period).mean() / tr_smooth
    minus_di = 100.0 * minus_dm.ewm(alpha=1.0 / period, adjust=False,
                                    min_periods=period).mean() / tr_smooth
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()


def donchian_high(high: pd.Series, period: int) -> pd.Series:
    """近 period 根的最高(不含当前根:突破判定 close > 前 N 根最高)。"""
    return high.rolling(period, min_periods=period).max().shift(1)


def donchian_low(low: pd.Series, period: int) -> pd.Series:
    return low.rolling(period, min_periods=period).min().shift(1)


def bollinger(close: pd.Series, period: int = 20, num_std: float = 2.0):
    """返回 (mid, upper, lower, bandwidth);bandwidth = (upper−lower)/mid。"""
    mid = sma(close, period)
    std = close.rolling(period, min_periods=period).std(ddof=0)
    upper = mid + num_std * std
    lower = mid - num_std * std
    bandwidth = (upper - lower) / mid
    return mid, upper, lower, bandwidth


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100.0 - 100.0 / (1.0 + rs)
    return out.fillna(100.0).where(gain.notna() & loss.notna())


def zscore(values: pd.Series, window: int) -> pd.Series:
    mean = values.rolling(window, min_periods=max(2, window // 3)).mean()
    std = values.rolling(window, min_periods=max(2, window // 3)).std(ddof=0)
    return (values - mean) / std.replace(0, np.nan)


def rolling_percentile_rank(values: pd.Series, window: int) -> pd.Series:
    """当前值在过去 window 个观测中的分位(0..1)。"""

    def rank(arr: np.ndarray) -> float:
        return float((arr[:-1] <= arr[-1]).mean()) if len(arr) > 1 else np.nan

    return values.rolling(window, min_periods=max(2, window // 3)).apply(rank, raw=True)


def vwap_daily(ts: pd.Series, price: pd.Series, volume: pd.Series) -> pd.Series:
    """日内 VWAP,按 UTC 日重置;volume 缺失时退化为当日累计均价。"""
    day = pd.to_datetime(ts, unit="s", utc=True).dt.floor("D")
    vol = volume.fillna(1.0).replace(0, 1.0)
    pv = (price * vol).groupby(day).cumsum()
    vv = vol.groupby(day).cumsum()
    return pv / vv


def resample_ohlcv(bars: pd.DataFrame, rule: str) -> pd.DataFrame:
    """把 15m bars(index=open_ts epoch 秒,列 open/high/low/close/volume)聚合到
    更高周期('1h'/'4h')。只保留已完整结束的高周期K线由调用方自行判断。"""
    frame = bars.copy()
    frame.index = pd.to_datetime(frame.index, unit="s", utc=True)
    out = frame.resample(rule, label="left", closed="left").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }).dropna(subset=["open"])
    out.index = (out.index.view("int64") // 10 ** 9)
    return out
