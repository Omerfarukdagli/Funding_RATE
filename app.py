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

# ==== Global Ayarlar ====
WS_URL = "wss://fstream.binance.com/stream?streams=!markPrice@arr"
buffers = defaultdict(lambda: deque(maxlen=1000))
last_min = defaultdict(lambda: -1)
SNAP_EVERY = 5_000
insert_cnt = 0
candles = defaultdict(lambda: deque(maxlen=500))
live_candles = {}
last_global_insert = time.time()

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
    restore_from_disk()
    app.state.tasks = [
        asyncio.create_task(ws_listener()),
        asyncio.create_task(kline_listener()),
        asyncio.create_task(heartbeat())
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
    dash_app = Dash(__name__, title="Funding Rate Dashboard")
    dash_app.layout = html.Div([
        html.H2("Funding Rate Dashboard"),
        html.Div(id="top10", style={"margin":"10px 0"}),
        html.Div(id="bottom10", style={"margin":"10px 0"}),
        dcc.Dropdown(id="symbol-dd", options=[], value=None, placeholder="Sembol seç…", style={"width": "300px"}),
        dcc.Graph(id="fund-graph"),
        dcc.Graph(id="price-graph"),
        dcc.Interval(id="refresh", interval=1_000, n_intervals=0)
    ])

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

    @dash_app.callback(Output("fund-graph", "figure"),
                       Input("symbol-dd", "value"), Input("refresh", "n_intervals"))
    def update_graph(symbol, _):
        if not symbol or not buffers[symbol]:
            return go.Figure()
        data_slice = list(buffers[symbol])[-500:]
        df = pd.DataFrame(data_slice, columns=["ts", "rate"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(ZoneInfo("Europe/Istanbul"))
        fig = go.Figure(go.Scatter(x=df["ts"], y=df["rate"]*100, mode="lines", name="Funding %"))
        fig.update_layout(title=f"{symbol} – Funding Rate (%)", xaxis_title="Time (+03)",
                          yaxis_title="Funding (%)", yaxis=dict(tickformat=".4f%%"))
        return fig

    @dash_app.callback(Output("price-graph", "figure"),
                       Input("symbol-dd", "value"), Input("refresh", "n_intervals"))
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