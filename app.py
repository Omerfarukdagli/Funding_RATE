# app.py
import os, asyncio, json, time, signal
from collections import defaultdict, deque
from typing import Optional
from fastapi import FastAPI
from starlette.middleware.wsgi import WSGIMiddleware
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from zoneinfo import ZoneInfo
import websockets, requests, pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import plotly.graph_objs as go

# ==== PostgreSQL (SQLAlchemy async) ====
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# ==== Global Ayarlar ====
WS_URL = "wss://fstream.binance.com/stream?streams=!markPrice@arr"
buffers = defaultdict(lambda: deque(maxlen=200_000))  # ~1 day @ ~3s ticks
last_min = defaultdict(lambda: -1)
SNAP_EVERY = 5_000
insert_cnt = 0
candles = defaultdict(lambda: deque(maxlen=500))
live_candles = {}
last_global_insert = time.time()

# ==== DB (Railway PostgreSQL) ====
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL eksik! Railway'de App → Variables → Add Variable Reference ile ekleyin.")

# SQLAlchemy asyncpg şeması
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# Private railway.internal host kullanırken SSL'i kapat (genelde gereksiz). Eğer URL'de sslmode=require varsa 'disable' yap.
parsed = urlparse(DATABASE_URL)
qs = parse_qs(parsed.query)
if parsed.hostname and parsed.hostname.endswith("railway.internal"):
    if qs.get("sslmode", [""])[0].lower() == "require":
        qs["sslmode"] = ["disable"]
    CONNECT_ARGS = {"ssl": False}
else:
    CONNECT_ARGS = {}

DATABASE_URL = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

engine: Optional[AsyncEngine] = create_async_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=5,
    pool_recycle=1800,   # uzun bağlantılarda kopma yaşamamak için
    connect_args=CONNECT_ARGS,
)

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS funding_min(
  symbol TEXT NOT NULL,
  minute_ts TIMESTAMPTZ NOT NULL,
  rate NUMERIC NOT NULL,
  PRIMARY KEY(symbol, minute_ts)
);
"""

# ==== Fonksiyonlar ====
async def ws_listener():
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                print("[WS] Connected")
                async for msg in ws:
                    data = json.loads(msg)
                    stream = data.get("stream")
                    payload = data.get("data", [])
                    if stream == "!markPrice@arr":
                        for item in payload:
                            sym = item["s"]
                            ts = int(item["E"])
                            rate = float(item["r"])
                            buffers[sym].append((ts, rate))
        except Exception as e:
            print("[WS] Error:", e)
            await asyncio.sleep(5)

async def kline_listener():
    url = "wss://fstream.binance.com/ws"
    params = {"streams": "@kline_1m"}
    while True:
        try:
            async with websockets.connect("wss://fstream.binance.com/stream?streams=!miniTicker@arr") as ws:
                print("[WS-KLINE] Connected")
                async for msg in ws:
                    pass  # Eğer kline datası eklenecekse burayı doldur
        except Exception as e:
            print("[WS-KLINE] Error:", e)
            await asyncio.sleep(5)

async def heartbeat():
    while True:
        await asyncio.sleep(60)
        print("[HB]", time.strftime("%H:%M:%S"))


# ==== DB INIT/RESTORE ====
async def init_db():
    if not engine:
        return
    async with engine.begin() as conn:
        await conn.execute(text(CREATE_SQL))

async def restore_from_db_last_day():
    if not engine:
        return
    # Load last 1 day of minute data per symbol and push into buffers as (ts_ms, rate)
    async with engine.connect() as conn:
        q = text(
            """
            SELECT symbol, EXTRACT(EPOCH FROM minute_ts)*1000 AS ts_ms, rate
            FROM funding_min
            WHERE minute_ts >= NOW() - INTERVAL '1 day'
            ORDER BY symbol, minute_ts
            """
        )
        res = await conn.execute(q)
        rows = res.fetchall()
        for sym, ts_ms, rate in rows:
            buffers[sym].append((int(ts_ms), float(rate)))
        if rows:
            print(f"[RESTORE-DB] restored rows={len(rows)}")

def snapshot_to_disk():
    try:
        table = pa.table({"symbol": [], "ts": [], "rate": []})
        for sym, dq in buffers.items():
            if dq:
                ts, rate = zip(*dq)
                t = pa.table({"symbol": [sym]*len(ts), "ts": ts, "rate": rate})
                table = pa.concat_tables([table, t])
        pq.write_table(table, "fund_snap.parquet")
        print("[SNAPSHOT] saved")
    except Exception as e:
        print("[SNAPSHOT] error:", e)

def restore_from_disk():
    try:
        if os.path.exists("fund_snap.parquet"):
            table = pq.read_table("fund_snap.parquet")
            df = table.to_pandas()
            for sym in df["symbol"].unique():
                sym_df = df[df["symbol"] == sym]
                buffers[sym].extend(zip(sym_df["ts"], sym_df["rate"]))
            print(f"[RESTORE] funding rows={len(df)}")
    except Exception as e:
        print("[RESTORE] error:", e)


# ==== BACKGROUND: Per-minute aggregator/upsert ====
async def minute_aggregator():
    if not engine:
        # No DB configured; just idle
        while True:
            await asyncio.sleep(5)
    # Track last written minute per symbol to avoid double-writes
    last_written = defaultdict(lambda: -1)
    while True:
        try:
            await asyncio.sleep(5)
            now_ms = int(time.time() * 1000)
            cur_min = (now_ms // 60000) * 60000
            prev_min = cur_min - 60000
            # For each symbol, find the last tick that belongs to prev_min and upsert
            payload = []
            for sym, dq in list(buffers.items()):
                if not dq:
                    continue
                # If we've already written prev_min, skip
                if last_written[sym] >= prev_min:
                    continue
                # Search from the end for last tick within prev_min window
                rate_to_write = None
                for ts, rate in reversed(dq):
                    minute_ms = (ts // 60000) * 60000
                    if minute_ms == prev_min:
                        rate_to_write = rate
                        break
                    if minute_ms < prev_min:
                        # passed the window
                        break
                if rate_to_write is not None:
                    payload.append((sym, prev_min, rate_to_write))
            if not payload:
                continue
            async with engine.begin() as conn:
                stmt = text(
                    """
                    INSERT INTO funding_min(symbol, minute_ts, rate)
                    VALUES (:s, to_timestamp(:ts_ms/1000.0), :r)
                    ON CONFLICT (symbol, minute_ts) DO UPDATE SET rate = EXCLUDED.rate
                    """
                )
                for s, ts_ms, r in payload:
                    await conn.execute(stmt, {"s": s, "ts_ms": ts_ms, "r": r})
                    last_written[s] = ts_ms
            print(f"[DB-UPsert] wrote {len(payload)} rows for minute {prev_min}")
        except Exception as e:
            print("[DB-UPsert] error:", e)

def fetch_rest_candles(symbol: str):
    try:
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=500"
        r = requests.get(url, timeout=5)
        data = r.json()
        candles[symbol] = deque([(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])) for c in data], maxlen=500)
    except Exception as e:
        print(f"[REST-CANDLE] {symbol} error:", e)

# ==== FastAPI ====
app = FastAPI()

@app.on_event("startup")
async def _startup():
    await init_db()
    await restore_from_db_last_day()
    restore_from_disk()
    app.state.tasks = [
        asyncio.create_task(ws_listener()),
        asyncio.create_task(kline_listener()),
        asyncio.create_task(heartbeat()),
        asyncio.create_task(minute_aggregator()),
    ]

@app.on_event("shutdown")
async def _shutdown():
    snapshot_to_disk()
    for t in getattr(app.state, "tasks", []):
        t.cancel()

@app.get("/funding/{symbol}")
async def get_symbol(symbol: str, limit: int = 1000, since: Optional[int] = None):
    dq = buffers.get(symbol.upper())
    if not dq:
        return {"error": "symbol not found"}
    data = list(dq)
    if since:
        data = [t for t in data if t[0] >= since]
    if limit:
        data = data[-limit:]
    ts, rt = zip(*data)
    return {"symbol": symbol.upper(), "timestamp": ts, "rate": rt}

# ==== Dash ====
def create_dash_app():
    dash_app = Dash(__name__, title="Funding Rate Dashboard", prevent_initial_callbacks=True)
    dash_app.layout = html.Div([
        html.H2("Funding Rate Dashboard"),
        html.Div(id="top10", style={"margin":"10px 0"}),
        html.Div(id="bottom10", style={"margin":"10px 0"}),
        dcc.Dropdown(id="symbol-dd", options=[], value=None, placeholder="Sembol seç…", style={"width": "300px"}),
        dcc.Graph(id="fund-graph"),
        dcc.Graph(id="fund-graph-1d"),
        dcc.Graph(id="price-graph"),
        dcc.Interval(id="refresh", interval=3_000, n_intervals=0)
    ])
    def _funding_per_minute_df(symbol: str) -> pd.DataFrame:
        data_slice = list(buffers.get(symbol, []))
        if not data_slice:
            return pd.DataFrame(columns=["ts", "rate_pct"])  # empty
        df = pd.DataFrame(data_slice, columns=["ts", "rate"])
        # floor to minute in ms
        df["minute_ms"] = (df["ts"] // 60000) * 60000
        # last value per minute (closest to minute end)
        df = (
            df.groupby("minute_ms", as_index=False)["rate"].last()
              .rename(columns={"minute_ms": "ts"})
        )
        # localize timestamps and compute percentage
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(ZoneInfo("Europe/Istanbul"))
        df["rate_pct"] = df["rate"] * 100.0
        return df

    @dash_app.callback(Output("symbol-dd", "options"), Input("refresh", "n_intervals"))
    def update_options(_):
        return [{"label": s, "value": s} for s in sorted(buffers.keys())]

    @dash_app.callback(Output("top10", "children"), Output("bottom10", "children"),
                       Input("refresh", "n_intervals"))
    def update_top_bottom(_):
        latest = [(s, dq[-1][1]*100) for s, dq in buffers.items() if dq]
        if not latest: return "", ""
        latest.sort(key=lambda t: t[1], reverse=True)
        top, bottom = latest[:10], latest[-10:]
        def make_table(title, rows):
            return html.Table([
                html.Thead(html.Tr([html.Th(title)])),
                html.Tbody([html.Tr([html.Td(sym), html.Td(f"{val:.4f}%")]) for sym,val in rows])
            ], style={"display":"inline-block", "margin":"0 15px"})
        return make_table("Top 10 +Funding", top), make_table("Bottom 10 −Funding", bottom)

    @dash_app.callback(
        Output("fund-graph", "figure"),
        Output("fund-graph-1d", "figure"),
        Input("symbol-dd", "value"),
        Input("refresh", "n_intervals"),
        prevent_initial_call=True,
    )
    def update_graphs(symbol, _):
        if not symbol or not buffers.get(symbol):
            return go.Figure(), go.Figure()

        df_min = _funding_per_minute_df(symbol)
        if df_min.empty:
            return go.Figure(), go.Figure()

        # Last 500 minutes
        df_500 = df_min.tail(500)
        fig_500 = go.Figure(go.Scatter(x=df_500["ts"], y=df_500["rate_pct"], mode="lines", name="Funding %"))
        fig_500.update_layout(
            title=f"{symbol} – Funding Rate (Last 500 min)",
            xaxis_title="Time (+03)",
            yaxis_title="Funding (%)",
        )

        # Last 1 day (1440 minutes)
        df_1d = df_min.tail(1440)
        fig_1d = go.Figure(go.Scatter(x=df_1d["ts"], y=df_1d["rate_pct"], mode="lines", name="Funding % (1D)"))
        fig_1d.update_layout(
            title=f"{symbol} – Funding Rate (Last 1 Day)",
            xaxis_title="Time (+03)",
            yaxis_title="Funding (%)",
        )

        return fig_500, fig_1d

    @dash_app.callback(Output("price-graph", "figure"),
                       Input("symbol-dd", "value"), Input("refresh", "n_intervals"),
                       prevent_initial_call=True)
    def update_price_graph(symbol, _):
        if not symbol: return go.Figure()
        if symbol not in candles or not candles[symbol]:
            fetch_rest_candles(symbol.upper())
        data_slice = list(candles.get(symbol, []))
        if not data_slice: return go.Figure()
        df = pd.DataFrame(data_slice, columns=["ts","open","high","low","close"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(ZoneInfo("Europe/Istanbul"))
        fig = go.Figure(go.Candlestick(x=df["ts"], open=df["open"], high=df["high"], low=df["low"], close=df["close"]))
        return fig

    return dash_app

dash_app = create_dash_app()
app.mount("/", WSGIMiddleware(dash_app.server))

# ==== Railway / Lokal Çalıştırma ====
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))