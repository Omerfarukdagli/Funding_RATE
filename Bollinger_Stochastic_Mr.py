"""
BB + Stochastic Mean-Reversion Strategy (Refactored)
---------------------------------------------------
• Capital:           variable (backtest param) – default 1000 USD
• Position sizing:   risk-based; qty = (equity * risk_frac) / risk_per_unit
• Leverage:          affects margin only; PnL is leverage-agnostic
• Fees/Slippage:     taker fee modeled as FEE_RATE; fixed slippage (bps)
• Stops/Targets:     ATR-based (tp_mult, sl_mult)

Execution engine rules:
• Signal is evaluated on bar i; entry is placed at bar i+1 open (next-bar fill)
• Intrabar exit priority is conservative:
  LONG: if both TP and SL touched in the same bar → SL first
  SHORT: if both TP and SL touched in the same bar → SL first
• Soft exits (BB-mid/time stop) apply only if SL/TP not hit first

Notes:
This file previously mentioned OB/FVG; that is removed to match the actual logic.
Backtest runner expects a pandas DataFrame with columns ['open','high','low','close','volume'] indexed by datetime.
"""
import random

import pandas as pd
import numpy as np
import requests, datetime, time
import talib

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Robust HTTP session with retries ---
_session = requests.Session()
_retry_cfg = Retry(
    total=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
_session.mount("https://", HTTPAdapter(max_retries=_retry_cfg))
_session.mount("http://", HTTPAdapter(max_retries=_retry_cfg))

def _get(url, **kwargs):
    resp = _session.get(url, timeout=kwargs.pop("timeout", 15), **kwargs)
    resp.raise_for_status()
    return resp

def first_touch_exit_long(bar_open, bar_high, bar_low, sl, tp):
    """Conservative sequencing for LONG. Order: gap@open -> SL -> TP."""
    if bar_open <= sl:
        return bar_open, "SL_gap"
    if bar_open >= tp:
        return bar_open, "TP_gap"
    if bar_low <= sl:
        return sl, "SL_touch"
    if bar_high >= tp:
        return tp, "TP_touch"
    return None, None

def first_touch_exit_short(bar_open, bar_high, bar_low, sl, tp):
    """Conservative sequencing for SHORT. Order: gap@open -> SL -> TP."""
    if bar_open >= sl:
        return bar_open, "SL_gap"
    if bar_open <= tp:
        return bar_open, "TP_gap"
    if bar_high >= sl:
        return sl, "SL_touch"
    if bar_low <= tp:
        return tp, "TP_touch"
    return None, None

# --- Metrics helpers ---
def compute_mdd(equity_series):
    peak = -np.inf
    mdd = 0.0
    for v in equity_series:
        if not np.isfinite(v):
            continue
        if v > peak:
            peak = v
        dd = (peak - v) / max(peak, EPS)
        if dd > mdd:
            mdd = dd
    return mdd

# --- Binance Klines Fetcher ---
BASE = "https://fapi.binance.com/fapi/v1/klines"  # futures
MS = {"1m":60_000,"3m":180_000,"5m":300_000,"15m":900_000,"1h":3_600_000,"4h":14_400_000}

def fetch_binance_klines(symbol, interval="15m", days=60, limit=1500):
    end_time = int(time.time()*1000)
    start_time = end_time - days*86_400_000
    step = MS[interval] * min(limit, 1500)  # fapi 1500'e kadar destekler
    rows=[]
    while start_time < end_time:
        params = {"symbol":symbol, "interval":interval, "limit": min(limit,1500),
                  "startTime":start_time, "endTime": min(start_time+step, end_time)}
        r = _get(BASE, params=params, timeout=10)
        data = r.json()
        if not data: break
        rows.extend(data)
        start_time = data[-1][6] + 1  # close_time + 1ms
        time.sleep(0.15)

    df = pd.DataFrame(rows, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_asset_volume","n_trades",
        "taker_buy_vol","taker_buy_quote","ignore"
    ])
    df["open"]  = df["open"].astype(float)
    df["high"]  = df["high"].astype(float)
    df["low"]   = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"]= df["volume"].astype(float)
    df.set_index(pd.to_datetime(df["open_time"], unit="ms"), inplace=True)
    return df[["open","high","low","close","volume"]]

FEE_RATE      = 0.0005   # 0.05 %
SLIPPAGE      = 0.0005   # 0.05 %
SL_PCT        = 0.03     # 3 %
TP_PCT        = 0.10     # 10 %
POS_FRACTION  = 0.10     # 10 % of equity
FILL_NEXT_BAR = True  # enforce next-bar execution for entries
LEVERAGE      = 3
# --- Numerical guards ---
EPS = 1e-9
MIN_ATR_PCT = 1e-3  # 0.1% minimum relative ATR to allow entries (avoid ATR=0 loops)

# --- Noise & frequency controls ---
MIN_BB_WIDTH = 0.009   # (bb_upper - bb_lower)/price must be >= 0.6%
COOLDOWN_BARS = 4      # wait N bars after an entry before taking another
VOL_FILTER_MULT = 1.2  # require volume >= VOL_FILTER_MULT * SMA20(volume)

# --- Strict mode / quality gates ---
MIN_EDGE = 0.002        # 0.2% min edge to BB middle to cover fees+slip and leave profit
ADX_MAX = 20.0          # skip mean-reversion entries when ADX > ADX_MAX (trend too strong)
MAX_HOLD_BARS = 12      # hard time-stop in bars

# ---- Layered Strategy Hyperparameters ----
SHADOW_RATIO    = 2.0    # Hammer lower-shadow ≥ 2× body
BODY_MAX_PCT    = 0.3    # Hammer body / range < 30%
DOJI_BODY_PCT   = 0.1    # Doji body / range < 10%
DOJI_SHADOW_PCT = 0.6    # Doji shadow / range > 60%
EMA_FAST        = 50     # Trend filter fast EMA
EMA_SLOW        = 200    # Trend filter slow EMA
VOL_MULT        = 1.5    # Volume filter: volume ≥ VOL_MULT × 20MA
ATR_SL_MULT     = 1.2    # SSL = ATR × ATR_SL_MULT
ATR_TP_MULT     = 3.0    # TP  = ATR × ATR_TP_MULT

# --- Trend filter controls ---
TREND_FILTER = "directional"   # options: "none", "skip_strong", "directional"
TREND_DIV_THRESH = 0.015        # 1.5% divergence threshold for skip_strong mode

# ------------------------------------------------------------------ #
# Mean‑Reversion Scalping Strategy (BB + Stoch)                       #
# ------------------------------------------------------------------ #
def add_mr_indicators(df):
    # Bollinger Bands (20, 2σ)
    upper, middle, lower = talib.BBANDS(df['close'], timeperiod=20, nbdevup=2, nbdevdn=2)
    df['bb_upper'] = upper
    df['bb_middle']= middle
    df['bb_lower'] = lower
    # Stochastic %K, %D (14,3,3)
    k, d = talib.STOCH(df['high'], df['low'], df['close'],
                       fastk_period=14, slowk_period=3, slowd_period=3)
    df['stoch_k'] = k
    df['stoch_d'] = d
    # ATR for SL/TP sizing
    df['atr'] = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    # EMAs for trend filters
    df['ema_fast'] = talib.EMA(df['close'], timeperiod=50)
    df['ema_slow'] = talib.EMA(df['close'], timeperiod=200)
    # Volume SMA for liquidity/impulse filter
    df['vol_sma20'] = talib.SMA(df['volume'], timeperiod=20)
    # ADX for trend strength filter
    df['adx'] = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
    return df

def mr_backtest(df: pd.DataFrame, equity: float = 1000.0,
                tp_mult: float = 1.0, sl_mult: float = 0.6,
                risk_frac: float = 0.007, max_positions: int = 10) -> pd.DataFrame:
    """
    Mean-reversion scalper with next-bar execution, multi-position support,
    conservative intrabar sequencing, and corrected risk sizing.
    """
    df = add_mr_indicators(df.copy())

    # Precompute arrays
    closes = df['close'].values
    highs  = df['high'].values
    lows   = df['low'].values
    opens  = df['open'].values
    bb_upper = df['bb_upper'].values
    bb_lower = df['bb_lower'].values
    bb_middle= df['bb_middle'].values
    stoch_k  = df['stoch_k'].values
    stoch_d  = df['stoch_d'].values
    atr      = df['atr'].values
    ema_f    = df['ema_fast'].values
    ema_s    = df['ema_slow'].values
    vol_sma20= df['vol_sma20'].values
    adx_arr  = df['adx'].values
    index    = df.index

    trades = []
    eq = float(equity)
    positions = []  # dict(side, entry, entry_eff, qty, sl, tp, entry_time, entry_index, notional)
    last_entry_i = -10**9

    def close_position(pos_idx: int, exit_price: float, exit_time, tag: str):
        nonlocal eq
        pos = positions.pop(pos_idx)
        if pos['side'] == 'LONG':
            exit_eff = exit_price * (1 - SLIPPAGE)
            gross = (exit_eff - pos['entry_eff']) * pos['qty']
        else:
            exit_eff = exit_price * (1 + SLIPPAGE)
            gross = (pos['entry_eff'] - exit_eff) * pos['qty']
        fees  = FEE_RATE * (pos['entry_eff'] * pos['qty'] + exit_eff * pos['qty'])
        pnl   = gross - fees
        eq   += pnl

        # bars_held: guard against any sequencing drift; cap by MAX_HOLD_BARS
        try:
            exit_idx = df.index.get_loc(exit_time)
        except KeyError:
            # Fallback: if exit_time not in index (shouldn't happen with next-bar engine)
            exit_idx = pos['entry_index']
        bars_held = max(0, int(exit_idx - pos['entry_index']))
        if bars_held > MAX_HOLD_BARS:
            bars_held = MAX_HOLD_BARS

        trades.append({
            'entry_time': pos['entry_time'],
            'exit_time':  exit_time,
            'side':       pos['side'],
            'entry':      pos['entry_eff'],
            'exit':       exit_eff,
            'pnl$':       pnl,
            'equity$':    eq,
            'entry_notional$': pos['notional'],
            'bars_held':  bars_held,
            'exit_tag':   tag,
        })

    n = len(df)
    for i in range(n - 1):  # i+1'e bakacağımız için n-2'ye kadar
        price = closes[i]
        a = atr[i]
        if not (np.isfinite(price) and np.isfinite(a)):
            continue
        if a <= 0 or (price > 0 and (a / price) < MIN_ATR_PCT):
            continue

        bb_lo = bb_lower[i]
        bb_up = bb_upper[i]
        k_v   = stoch_k[i]
        d_v   = stoch_d[i]
        if any(np.isnan([bb_lo, bb_up, k_v, d_v])):
            continue

        bb_width = (bb_up - bb_lo) / max(price, EPS)
        if not np.isfinite(bb_width) or bb_width < MIN_BB_WIDTH:
            continue

        vol_sma = vol_sma20[i]
        vol_now = df['volume'].iat[i]
        if np.isfinite(vol_sma) and vol_sma > 0 and vol_now < VOL_FILTER_MULT * vol_sma:
            continue

        ef = ema_f[i]; es = ema_s[i]
        if not (np.isfinite(ef) and np.isfinite(es)):
            continue
        div_ratio = abs(ef - es) / max(price, EPS)
        if TREND_FILTER == "skip_strong" and div_ratio >= TREND_DIV_THRESH:
            continue
        adx_v = adx_arr[i]
        if np.isfinite(adx_v) and adx_v > ADX_MAX:
            continue
        directional_long_ok = True
        directional_short_ok = True
        if TREND_FILTER == "directional":
            directional_long_ok = price > es
            directional_short_ok = price < es

        if i > 0 and np.isfinite(stoch_k[i-1]) and np.isfinite(stoch_d[i-1]):
            long_cross  = (stoch_k[i-1] <= stoch_d[i-1]) and (k_v > d_v)
            short_cross = (stoch_k[i-1] >= stoch_d[i-1]) and (k_v < d_v)
        else:
            long_cross = short_cross = False

        # ---- Manage existing positions on next bar (i+1) ----
        next_o = opens[i+1]; next_h = highs[i+1]; next_l = lows[i+1]
        next_t = index[i+1]

        for p_idx in range(len(positions) - 1, -1, -1):
            pos = positions[p_idx]
            if pos['entry_index'] >= i+1:
                continue
            if pos['side'] == 'LONG':
                exit_price, tag = first_touch_exit_long(next_o, next_h, next_l, pos['sl'], pos['tp'])
            else:
                exit_price, tag = first_touch_exit_short(next_o, next_h, next_l, pos['sl'], pos['tp'])

            if exit_price is not None:
                close_position(p_idx, exit_price, next_t, tag)
                continue

            bb_mid = bb_middle[i+1]
            if np.isfinite(bb_mid):
                if (pos['side'] == 'LONG' and next_o >= bb_mid) or (pos['side'] == 'SHORT' and next_o <= bb_mid):
                    close_position(p_idx, next_o, next_t, 'BB_mid')
                    continue

            if (i+1 - pos['entry_index']) >= MAX_HOLD_BARS:
                close_position(p_idx, next_o, next_t, 'time_stop')
                continue

        # ---- New entries ----
        if len(positions) >= max_positions:
            continue

        if (k_v < 20 and d_v < 20 and long_cross
                and price <= bb_lo
                and (TREND_FILTER != "directional" or directional_long_ok)
                and (i - last_entry_i >= COOLDOWN_BARS)
                and np.isfinite(bb_middle[i])
                and ((bb_middle[i] - price) / max(price, EPS) >= MIN_EDGE)):
            risk_per_unit = max(a * sl_mult, price * MIN_ATR_PCT, EPS)
            qty = (eq * risk_frac) / max(risk_per_unit, EPS)
            if np.isfinite(qty) and qty > 0:
                entry_raw = opens[i+1]
                entry_eff = entry_raw * (1 + SLIPPAGE)
                positions.append({
                    'side': 'LONG',
                    'entry': entry_raw,
                    'entry_eff': entry_eff,
                    'qty':   qty,
                    'sl':    entry_raw - sl_mult * atr[i+1],
                    'tp':    entry_raw + tp_mult * atr[i+1],
                    'entry_time': index[i+1],
                    'entry_index': i+1,
                    'notional': qty * entry_raw,
                })
                last_entry_i = i

        elif (k_v > 80 and d_v > 80 and short_cross
                and price >= bb_up
                and (TREND_FILTER != "directional" or directional_short_ok)
                and (i - last_entry_i >= COOLDOWN_BARS)
                and np.isfinite(bb_middle[i])
                and ((price - bb_middle[i]) / max(price, EPS) >= MIN_EDGE)):
            risk_per_unit = max(a * sl_mult, price * MIN_ATR_PCT, EPS)
            qty = (eq * risk_frac) / max(risk_per_unit, EPS)
            if np.isfinite(qty) and qty > 0:
                entry_raw = opens[i+1]
                entry_eff = entry_raw * (1 - SLIPPAGE)
                positions.append({
                    'side': 'SHORT',
                    'entry': entry_raw,
                    'entry_eff': entry_eff,
                    'qty':   qty,
                    'sl':    entry_raw + sl_mult * atr[i+1],
                    'tp':    entry_raw - tp_mult * atr[i+1],
                    'entry_time': index[i+1],
                    'entry_index': i+1,
                    'notional': qty * entry_raw,
                })
                last_entry_i = i

    return pd.DataFrame(trades)

# --------------------------- CLI portfolio test -------------------- #
if __name__ == "__main__":
    def get_usdt_perp_symbols(sample_size=50, min_quote_vol=50_000_000):
        """Return random USDT PERPETUAL symbols with 24h quoteVolume >= threshold
        and onboarded at least 90 days ago.
        """
        ex_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        ex = _get(ex_url, timeout=10).json()
        now_ms = int(time.time() * 1000)
        ninety_days_ms = 90 * 86_400_000
        all_perp = {
            s['symbol'] for s in ex['symbols']
            if s.get('contractType') == "PERPETUAL"
               and s.get('quoteAsset') == "USDT"
               and s.get('status') == "TRADING"
               and (now_ms - int(s.get('onboardDate', 0))) >= ninety_days_ms
        }

        t24 = _get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15).json()
        liquid = {item['symbol'] for item in t24 if
                  item.get('quoteVolume') and float(item['quoteVolume']) >= min_quote_vol}
        candidates = list(all_perp & liquid)
        # Blacklist memecoins/illiquid symbols by substring
        blacklist_subs = ("1000", "10000", "BONK", "PEPE", "TURBO", "TRUMP", "BOME", "POPCAT", "FLOKI")
        candidates = [s for s in candidates if not any(sub in s for sub in blacklist_subs)]
        if not candidates:
            return []
        return random.sample(candidates, min(sample_size, len(candidates)))

    equity_start = 1000.0
    symbols = get_usdt_perp_symbols(10, min_quote_vol=50_000_000)
    print(f"Testing {len(symbols)} random USDT perpetual symbols (15‑min, 180 gün):\n")

    portfolio_stats = []
    for sym in symbols:
        try:
            df = fetch_binance_klines(sym, interval="15m", days=180)
            # Skip symbols with <30 days of data (≈ 2880 bars on 15‑min)
            if df.empty or (df.index[-1] - df.index[0] < pd.Timedelta(days=30)):
                print(f"{sym} skipped: <30 günlük veri")
                continue
            res = mr_backtest(df, equity=equity_start, risk_frac=0.005, sl_mult=0.8, tp_mult=1.0, max_positions=10)
            if res.empty:
                continue
            final_eq = res.iloc[-1]['equity$']
            winrate  = (res['pnl$']>0).mean()*100
            # Trade-based returns relative to notional
            trade_rets = (res['pnl$'] / res['entry_notional$']).replace([np.inf, -np.inf], np.nan).dropna()
            sharpe_tr  = np.nan
            if not trade_rets.empty and trade_rets.std()!=0:
                sharpe_tr = trade_rets.mean()/trade_rets.std()
            expectancy = res['pnl$'].mean() if not res['pnl$'].empty else np.nan
            mdd = compute_mdd(res['equity$'])

            long_tr = (res['side'] == 'LONG').sum() if not res.empty else 0
            short_tr = (res['side'] == 'SHORT').sum() if not res.empty else 0
            avg_hold = res['bars_held'].mean() if 'bars_held' in res and not res['bars_held'].empty else np.nan

            portfolio_stats.append({
                'symbol': sym, 'final_eq': final_eq,
                'winrate': winrate, 'sharpe_trade': sharpe_tr,
                'expectancy': expectancy, 'mdd': mdd,
                'trades': len(res), 'long_trades': int(long_tr), 'short_trades': int(short_tr),
                'avg_hold_bars': float(avg_hold) if avg_hold == avg_hold else np.nan
            })
            print(f"{sym:10} | Trades:{len(res):3d} (L:{int(long_tr)}/S:{int(short_tr)}) | "
                  f"WinRate:{winrate:5.1f}% | FinalEq:{final_eq:8.2f} | Sharpe(T):{sharpe_tr:5.2f} | "
                  f"Exp:{expectancy:8.2f} | MDD:{mdd:5.1%} | Hold(bar):{avg_hold if avg_hold == avg_hold else float('nan'):.1f}")
        except Exception as e:
            print(f"{sym} error: {e}")

    if not portfolio_stats:
        print("No trades executed on any symbol.")
        exit(0)

    dfp = pd.DataFrame(portfolio_stats)
    print("\n=== PORTFÖY ÖZETİ ===")
    print(f"Ortalama WinRate : {dfp['winrate'].mean():.2f}%")
    print(f"Ortalama FinalEq : {dfp['final_eq'].mean():.2f}")
    print(f"Ortalama Sharpe(T): {dfp['sharpe_trade'].mean():.2f}")
    print(f"Ortalama Expectancy: {dfp['expectancy'].mean():.2f}")
    print(f"Medyan MDD        : {dfp['mdd'].median():.2%}")
    total_trades = int(dfp['trades'].sum())
    print(f"Toplam Trades     : {total_trades}")
    print(f"Toplam Long Trades: {int(dfp['long_trades'].sum()) if 'long_trades' in dfp else 0}")
    print(f"Toplam Short Trades: {int(dfp['short_trades'].sum()) if 'short_trades' in dfp else 0}")