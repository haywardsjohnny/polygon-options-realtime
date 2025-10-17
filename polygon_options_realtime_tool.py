#!/usr/bin/env python3
"""
Polygon Options Realtime Tool (All NASDAQ + NYSE Universe)
----------------------------------------------------------
Streams live options trades from Polygon for all NASDAQ & NYSE stocks.
Flags next-Friday expiries with strike > stock price and repeated trades.

Outputs:
  • ./out/options_trades_latest.csv
  • ./out/alerts.log
  • GitHub auto-push (optional)
"""

import asyncio
import os
import json
import time
import signal
import logging
import datetime as dt
from collections import defaultdict, deque
from dataclasses import dataclass
import ssl
import certifi

import pandas as pd
from dotenv import load_dotenv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from git import Repo, Actor

try:
    from polygon import RESTClient
except Exception:
    RESTClient = None

import websockets

# ---------------- Config ----------------
load_dotenv()
API_KEY = os.getenv("POLYGON_API_KEY", "")
ALERT_REPEAT_THRESHOLD = int(os.getenv("ALERT_REPEAT_THRESHOLD", "3"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./out")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")
GITHUB_PAT = os.getenv("GITHUB_PAT", "")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
PUSH_INTERVAL_S = int(os.getenv("PUSH_INTERVAL_S", "30"))
SNAPSHOT_INTERVAL_S = int(os.getenv("SNAPSHOT_INTERVAL_S", "5"))
INCLUDE_PUTS = os.getenv("INCLUDE_PUTS", "0").strip() == "1"

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(OUTPUT_DIR, "run.log"), mode="a")
    ]
)
logger = logging.getLogger("poly-opts")

# ---------------- Helpers ----------------
def next_friday_from(date: dt.date) -> dt.date:
    days_ahead = (4 - date.weekday()) % 7
    days_ahead = 7 if days_ahead == 0 else days_ahead
    return date + dt.timedelta(days=days_ahead)

@dataclass
class OptionPrint:
    ts: dt.datetime
    underlying: str
    expiry: dt.date
    side: str
    strike: float
    price: float
    size: float
    raw: dict

OPTION_TICKER_PREFIX = "O:"

def parse_polygon_option_symbol(sym: str):
    if sym.startswith(OPTION_TICKER_PREFIX):
        sym = sym[len(OPTION_TICKER_PREFIX):]
    idx_cp = max(sym.rfind('C'), sym.rfind('P'))
    expiry_str = sym[idx_cp-6:idx_cp]
    side = sym[idx_cp]
    strike_str = sym[idx_cp+1:]
    underlying = sym[:idx_cp-6]
    strike = int(strike_str) / 1000.0
    y = 2000 + int(expiry_str[:2])
    m = int(expiry_str[2:4])
    d = int(expiry_str[4:6])
    expiry = dt.date(y, m, d)
    return underlying, expiry, side, strike

# ---------------- State ----------------
UNDERLYING_LAST = {}
REPEATS = defaultdict(int)
WINDOW = deque(maxlen=50000)
ALERTS_SEEN = set()
TARGET_EXPIRY = next_friday_from(dt.date.today())

# ---------------- REST for all NASDAQ/NYSE symbols ----------------
async def refresh_symbols():
    if RESTClient is None:
        logger.warning("polygon-api-client missing; cannot refresh tickers")
        return []
    client = RESTClient(api_key=API_KEY)
    tickers = []
    try:
        resp = client.list_tickers(market="stocks", active=True, limit=1000)
        for t in resp:
            if t.primary_exchange in ("XNAS", "XNYS"):
                tickers.append(t.ticker)
    except Exception as e:
        logger.error(f"Failed to load tickers: {e}")
    return tickers

async def refresh_last_prices(all_syms):
    if RESTClient is None:
        return
    client = RESTClient(api_key=API_KEY)
    while True:
        try:
            for sym in all_syms:
                try:
                    lt = client.get_last_trade(sym)
                    UNDERLYING_LAST[sym] = lt.price
                except Exception:
                    continue
            await asyncio.sleep(SNAPSHOT_INTERVAL_S)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"refresh_last_prices error: {e}")
            await asyncio.sleep(2)

# ---------------- WebSocket consume ----------------
POLY_WS_URL = "wss://socket.polygon.io/options"

async def ws_consume(all_syms):
    auth_msg = {"action": "auth", "params": API_KEY}
    sub_msg = {"action": "subscribe", "params": "options/trades"}

    # ✅ Explicit SSL context using certifi
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    async for ws in websockets.connect(
        POLY_WS_URL,
        ping_interval=15,
        ping_timeout=15,
        ssl=ssl_context
    ):
        try:
            await ws.send(json.dumps(auth_msg))
            await ws.send(json.dumps(sub_msg))
            logger.info("Subscribed → options/trades")
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if isinstance(msg, list):
                    for ev in msg:
                        await handle_event(ev, all_syms)
                elif isinstance(msg, dict):
                    await handle_event(msg, all_syms)
        except Exception as e:
            logger.warning(f"WS error: {e}; reconnecting...")
            await asyncio.sleep(2)

# ---------------- Event Handling ----------------
async def handle_event(ev, all_syms):
    sym = ev.get('sym') or ev.get('ticker')
    if not sym or not sym.startswith(OPTION_TICKER_PREFIX):
        return
    try:
        under, expiry, side, strike = parse_polygon_option_symbol(sym)
    except Exception:
        return
    if under not in all_syms:
        return
    if expiry != TARGET_EXPIRY:
        return
    px = ev.get('p') or ev.get('price')
    sz = ev.get('s') or ev.get('size') or 0
    ts_nanos = ev.get('t') or int(time.time() * 1e9)
    ts = dt.datetime.fromtimestamp(ts_nanos / 1e9)
    last = UNDERLYING_LAST.get(under)
    if not last or strike <= last:
        return
    if (side == 'P') and (not INCLUDE_PUTS):
        return
    p = OptionPrint(
        ts=ts,
        underlying=under,
        expiry=expiry,
        side=side,
        strike=strike,
        price=px or 0.0,
        size=float(sz),
        raw=ev
    )
    WINDOW.append(p)
    key = (under, expiry, side, strike)
    REPEATS[key] += 1
    if REPEATS[key] >= ALERT_REPEAT_THRESHOLD and key not in ALERTS_SEEN:
        ALERTS_SEEN.add(key)
        write_alert(p, REPEATS[key], last)
    if len(WINDOW) % 100 == 0:
        write_csv()

# ---------------- Outputs ----------------
def write_csv():
    df = pd.DataFrame([{
        "Time": p.ts.isoformat(timespec='seconds'),
        "Underlying": p.underlying,
        "Expiry": p.expiry.isoformat(),
        "Side": p.side,
        "Strike": p.strike,
        "Price": p.price,
        "Size": p.size,
    } for p in WINDOW])
    df.to_csv(os.path.join(OUTPUT_DIR, "options_trades_latest.csv"), index=False)

def write_alert(p, count, last):
    msg = (f"{p.ts:%Y-%m-%d %H:%M:%S} | ALERT | {p.underlying} {p.side} "
           f"{p.strike:.2f} exp {p.expiry} repeats={count} last={last:.2f} opt_px={p.price}")
    logger.warning(msg)
    with open(os.path.join(OUTPUT_DIR, "alerts.log"), 'a') as f:
        f.write(msg + "\n")

# ---------------- GitHub Push ----------------
def ensure_repo_and_push_loop():
    if not (GITHUB_REPO and GITHUB_PAT):
        return None
    repo_path = os.path.abspath(OUTPUT_DIR)
    if not os.path.exists(os.path.join(repo_path, '.git')):
        repo = Repo.init(repo_path)
        remote_url = f"https://{GITHUB_PAT}:x-oauth-basic@github.com/{GITHUB_REPO}.git"
        try:
            repo.create_remote('origin', remote_url)
        except Exception:
            pass
    else:
        repo = Repo(repo_path)
    author = Actor("polybot", "polybot@example.com")

    async def pusher():
        while True:
            try:
                repo.git.add('.')
                if repo.is_dirty(untracked_files=True):
                    repo.index.commit(f"auto-update {dt.datetime.now(dt.UTC).isoformat()}", author=author)
                    repo.git.branch('-M', GITHUB_BRANCH)
                    repo.git.push('-u', 'origin', GITHUB_BRANCH)
                await asyncio.sleep(PUSH_INTERVAL_S)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"push error: {e}")
                await asyncio.sleep(5)
    return pusher

# ---------------- Main ----------------
async def main():
    if not API_KEY:
        raise SystemExit("POLYGON_API_KEY missing")
    all_syms = await refresh_symbols()
    logger.info(f"Loaded {len(all_syms)} symbols from NASDAQ/NYSE")
    tasks = [
        asyncio.create_task(refresh_last_prices(all_syms)),
        asyncio.create_task(ws_consume(all_syms))
    ]
    pusher = ensure_repo_and_push_loop()
    if pusher:
        tasks.append(asyncio.create_task(pusher()))
    stop = asyncio.Future()

    def _sig(*_):
        if not stop.done():
            stop.set_result(True)
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _sig)
        except Exception:
            pass
    await stop
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

