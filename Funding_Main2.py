import asyncio, json, time, gzip, signal, os
from collections import defaultdict, deque
import websockets
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI
import pandas as pd
from typing import Optional
import uvicorn
import requests
from zoneinfo import ZoneInfo  # Python 3.9+ timezone
from dash import Dash, html, dcc
from dash.dependencies import Input, Output

WS_URL = "wss://fstream.binance.com/stream?streams=!markPrice@arr"

buffers  = defaultdict(lambda: deque(maxlen=1000))   # { "BTCUSDT": deque[(ts, rate), …] }
last_min = defaultdict(lambda: -1)                   # dakikalık örnekleme kontrolü
SNAP_EVERY = 5_000                                   # kaç insert’te bir disk snapshot
insert_cnt = 0
candles = defaultdict(lambda: deque(maxlen=500))   # 1m OHLC for price chart
live_candles = {}                                  # <-- EKLE
last_global_insert = time.time()   # heartbeat için

async def kline_listener():
    """Collect closed 1‑minute klines for price candlestick."""
    KLINE_WS = "wss://fstream.binance.com/stream?streams=!kline_1m@arr"
    backoff = 1
    while True:
        try:
            async with websockets.connect(KLINE_WS, ping_interval=20) as ws:
                print("[WS‑KLINE] Connected")
                backoff = 1
                async for raw in ws:
                    msg = json.loads(raw)
                    payload = msg.get("data", [])
                    items = payload if isinstance(payload, list) else [payload]
                    for data in items:
                        k = data["k"]
                        sym = data["s"]
                        ts  = k["T"]       # candle close time (updates while forming)

                        o = float(k["o"])
                        h = float(k["h"])
                        l = float(k["l"])
                        c = float(k["c"])
                        rate = float(data["r"])
                        price = float(data["p"])  # mark price, canlı fiyat

                        # ----- CANLI MUM -----
                        cur_min = ts // 60_000
                        lv = live_candles.get(sym)

                        if lv is None or lv[0] != cur_min:
                            # önceki dakikayı finalize et
                            if lv:
                                ts_close = lv[0] * 60_000 + 59_000
                                candles[sym].append((ts_close, lv[1], lv[2], lv[3], lv[4]))
                            # yeni forming mum
                            live_candles[sym] = [cur_min, price, price, price, price]  # [min, O, H, L, C]
                        else:
                            # mevcut forming mumu güncelle
                            lv[2] = max(lv[2], price)  # high
                            lv[3] = min(lv[3], price)  # low
                            lv[4] = price  # close
                        dq = candles[sym]
                        if dq and dq[-1][0] == ts:
                            # replace last (same ts) to reflect live changes
                            dq[-1] = (ts, o, h, l, c)
                        else:
                            dq.append((ts, o, h, l, c))
        except Exception as e:
            print(f"[WS‑KLINE] Disconnected: {e}. Reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff*2, 60)

# ---------- 1) DINLEME GÖREVİ ----------
async def ws_listener():
    global insert_cnt, last_global_insert
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20) as ws:
                print("[WS] Connected")
                backoff = 1
                async for raw in ws:
                    msg = json.loads(raw)

                    # Binance `!markPrice@arr` döndüğü zaman `data` DİZİdir.
                    # Tek obje geldiğinde dict, çoklu geldiğinde list oluyor.
                    payload = msg.get("data", [])
                    items = payload if isinstance(payload, list) else [payload]

                    for data in items:
                        sym  = data["s"]
                        ts   = data["E"]             # epoch‑ms
                        rate = float(data["r"])      # funding rate
                        minute = ts // 60_000
                        if minute != last_min[sym]:      # sadece dakikanın son değeri
                            last_min[sym] = minute
                            buffers[sym].append((ts, rate))
                            last_global_insert = time.time()
                            insert_cnt += 1
                            if insert_cnt % SNAP_EVERY == 0:
                                await snapshot_to_disk()
        except Exception as e:
            print(f"[WS] Disconnected: {e}. Reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def heartbeat():
    """90 sn’den uzun süre veri gelmezse uyarı logla."""
    while True:
        await asyncio.sleep(30)
        if time.time() - last_global_insert > 90:
            print("[HB] WARNING: 90 s dir yeni funding verisi yok ‑ WS may be stale")

# ---------- 2) SNAPSHOT ----------
async def snapshot_to_disk(path="fund_snap.parquet"):
    tables = []
    candle_tables = []
    for sym, dq in buffers.items():
        if dq:
            arr_ts, arr_rt = zip(*dq)
            tbl = pa.table({"symbol": [sym]*len(dq),
                            "timestamp": pa.array(arr_ts, type=pa.int64()),
                            "rate": pa.array(arr_rt, type=pa.float32())})
            tables.append(tbl)
    for sym, dq in candles.items():
        if sym in live_candles:  # forming mumu ekle
            cur = live_candles[sym]
            ts_close = cur[0] * 60_000 + 59_000
            dq.append((ts_close, cur[1], cur[2], cur[3], cur[4]))
        if dq:
            (ts, o, h, l, c) = zip(*dq)
            tbl = pa.table({
                "symbol": [sym]*len(dq),
                "timestamp": pa.array(ts, type=pa.int64()),
                "open": pa.array(o, type=pa.float32()),
                "high": pa.array(h, type=pa.float32()),
                "low":  pa.array(l, type=pa.float32()),
                "close":pa.array(c, type=pa.float32())
            })
            candle_tables.append(tbl)
    if tables or candle_tables:
        pq.write_table(pa.concat_tables(tables + candle_tables), path, compression="zstd")
        row_cnt = sum(len(dq) for dq in buffers.values()) + sum(len(dq) for dq in candles.values())
        print(f"[SNAP] Wrote {row_cnt} rows → {path}")
    await asyncio.sleep(0)  # event‑loop’a bir şans ver

# ---------- RESTORE ----------
def restore_from_disk(path="fund_snap.parquet"):
    """Parquet dosyasından bellek tamponlarını doldurur (restart sonrası)."""
    if not os.path.exists(path):
        return
    table = pq.read_table(path)
    df = table.to_pandas()
    for sym, group in df.groupby("symbol"):
        tuples = list(zip(group["timestamp"], group["rate"]))
        buffers[sym].extend(tuples[-1000:])        # maxlen garantisi
        if tuples:
            last_min[sym] = max(ts // 60_000 for ts, _ in tuples)
    if {"open","high","low","close"}.issubset(df.columns):
        for sym, group in df.groupby("symbol"):
            tuples = list(zip(group["timestamp"],
                              group["open"],
                              group["high"],
                              group["low"],
                              group["close"]))
            candles[sym].extend(tuples[-500:])
    print(f"[RESTORE] Loaded {len(df)} rows from {path}")

def fetch_rest_candles(symbol: str, limit: int = 500):
    """Fallback REST fetch if WebSocket hasn't provided candles yet."""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit={limit}"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        for k in data:
            ts = k[6]           # closeTime
            o, h, l, c = map(float, (k[1], k[2], k[3], k[4]))
            candles[symbol].append((ts, o, h, l, c))
    except Exception as e:
        print(f"[REST] Failed to fetch klines for {symbol}: {e}")

 # ---------- DASH PANEL ----------
def create_dash_app():
    """Dash uygulaması – sembol seçip 500 dakikalık funding grafiği çizer."""
    dash_app = Dash(__name__, title="Funding Rate Dashboard")

    dash_app.layout = html.Div([
        html.H2("Funding Rate Dashboard"),
        html.Div(id="top10", style={"margin":"10px 0"}),
        html.Div(id="bottom10", style={"margin":"10px 0"}),
        dcc.Dropdown(id="symbol-dd", options=[], value=None,
                     placeholder="Sembol seç…", style={"width": "300px"}),
        dcc.Graph(id="fund-graph"),
        dcc.Graph(id="price-graph"),
        # Gerçek zamanlı güncelleme – her 1 saniye
        dcc.Interval(id="refresh", interval=1_000, n_intervals=0)
    ])

    # Dropdown seçeneklerini her dakika güncelle
    @dash_app.callback(
        Output("symbol-dd", "options"),
        Input("refresh", "n_intervals"),
        prevent_initial_call=False
    )
    def update_options(_):
        return [{"label": s, "value": s} for s in sorted(buffers.keys())]

    # En yüksek / en düşük 10 funding listesi
    @dash_app.callback(
        Output("top10", "children"),
        Output("bottom10", "children"),
        Input("refresh", "n_intervals"),
        prevent_initial_call=False
    )
    def update_top_bottom(_):
        # Son funding değerlerini topla
        latest = []
        for s, dq in buffers.items():
            if dq:
                latest.append((s, dq[-1][1]*100))   # % cinsinden
        if not latest:
            return "", ""
        latest.sort(key=lambda t: t[1], reverse=True)
        top = latest[:10]
        bottom = latest[-10:]

        def make_table(title, rows):
            return html.Table([
                html.Thead(html.Tr([html.Th(title)])),
                html.Tbody([
                    html.Tr([html.Td(sym), html.Td(f"{val:.4f}%")]) for sym,val in rows
                ])
            ], style={"display":"inline-block", "margin":"0 15px"})

        return make_table("Top 10 +Funding", top), make_table("Bottom 10 −Funding", bottom)

    # Grafik güncelle
    @dash_app.callback(
        Output("fund-graph", "figure"),
        Input("symbol-dd", "value"),
        Input("refresh", "n_intervals"),
        prevent_initial_call=False
    )
    def update_graph(symbol, _):
        import plotly.graph_objs as go
        import pandas as pd

        if not symbol or symbol not in buffers or not buffers[symbol]:
            return go.Figure()

        data_slice = list(buffers[symbol])[-500:]  # last 500 minutes
        df = pd.DataFrame(data_slice, columns=["ts", "rate"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(ZoneInfo("Europe/Istanbul"))

        fig = go.Figure(go.Scatter(
            x=df["ts"],
            y=df["rate"] * 100,   # convert to %
            mode="lines",
            name="Funding %"
        ))
        fig.update_layout(
            title=f"{symbol} – Funding Rate (%)",
            xaxis_title="Time (+03)",
            yaxis_title="Funding (%)",
            yaxis=dict(tickformat=".4f%%"),
            xaxis_rangeslider_visible=False
        )
        return fig

    @dash_app.callback(
        Output("price-graph", "figure"),
        Input("symbol-dd", "value"),
        Input("refresh", "n_intervals"),
        prevent_initial_call=False
    )
    def update_price_graph(symbol, _):
        import plotly.graph_objs as go
        import pandas as pd

        if not symbol:
            return go.Figure()

        if symbol not in candles or not candles[symbol]:
            fetch_rest_candles(symbol.upper())

        # === Align time window with funding graph ===
        fund_slice = list(buffers.get(symbol, []))[-500:]
        if fund_slice:
            fund_start_ts = fund_slice[0][0]
            fund_end_ts   = fund_slice[-1][0]
        else:
            fund_start_ts = 0
            fund_end_ts   = 9e15

        # Filter candles to the same time span
        data_slice = [t for t in candles[symbol] if fund_start_ts <= t[0] <= fund_end_ts]
        if not data_slice:
            return go.Figure()

        df = pd.DataFrame(data_slice, columns=["ts","open","high","low","close"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert(ZoneInfo("Europe/Istanbul"))

        fig = go.Figure(go.Candlestick(
            x=df["ts"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="Price"
        ))
        # Overlay close‑price line for clearer real‑time direction
        fig.add_trace(go.Scatter(
            x=df["ts"],
            y=df["close"],
            mode="lines",
            line=dict(width=1),
            name="Close",
            yaxis="y",  # same axis
            hoverinfo="skip"
        ))
        fig.update_layout(
            title=f"{symbol} – 1‑min Price (candles + close line)",
            xaxis_title="Time (+03)",
            yaxis_title="Price",
            xaxis_rangeslider_visible=False,
            hovermode="x unified"
        )
        return fig

    return dash_app

# ---------- 3) API ----------
app = FastAPI()

@app.get("/funding/{symbol}")
async def get_symbol(symbol: str,
                     limit: int = 1000,
                     since: Optional[int] = None):
    dq = buffers.get(symbol.upper())
    if not dq:
        return {"error": "symbol not found"}
    data = list(dq)
    if since:
        data = [t for t in data if t[0] >= since]
    if limit:
        data = data[-limit:]
    if not data:
        return {"symbol": symbol.upper(), "timestamp": [], "rate": []}
    ts, rt = zip(*data)
    return {"symbol": symbol.upper(), "timestamp": ts, "rate": rt}

# ---------- 4) MAIN ----------
async def main():
    restore_from_disk()   # restart sonrası tamponu doldur
    # ➊ WebSocket dinleyiciyi başlat
    listener_task = asyncio.create_task(ws_listener())
    hb_task = asyncio.create_task(heartbeat())
    kline_task = asyncio.create_task(kline_listener())

    # ➋ API sunucusunu ayrı bir thread'de başlat (bloklamaz)
    server_task = asyncio.to_thread(
        uvicorn.run,
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"   # INFO ile başlama satırı görünür
    )

    # ➌ Dash panelini ayrı thread'de başlat
    dash_app = create_dash_app()
    dash_task = asyncio.to_thread(
        dash_app.run,
        host="0.0.0.0",
        port=8050,
        debug=False
    )

    # ➍ Her dört görev de bitene kadar bekle (normalde sonsuz)
    try:
        await asyncio.gather(listener_task, kline_task, server_task, dash_task, hb_task)
    finally:
        # graceful shutdown – son snapshot
        await snapshot_to_disk()

# Graceful shutdown CTRL-C
signal.signal(signal.SIGINT, lambda *_: asyncio.get_event_loop().stop())

if __name__ == "__main__":
    asyncio.run(main())