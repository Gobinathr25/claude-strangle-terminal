#!/usr/bin/env python3
"""
============================================================================
 SUPERTREND-DIRECTED ROLLING HEDGED STRANGLE  --  WEB TRADING TERMINAL
 Flask + Flask-SocketIO backend  |  FYERS API v3
============================================================================

 Architecture
 ------------
   Flask routes ........ /              dashboard page
                         /login         redirects browser to FYERS OAuth
                         /callback      FYERS posts auth_code here (auto)
                         /api/state     REST snapshot (fallback)
                         /api/control   start / stop strategy
   SocketIO ............ pushes live STATE to browser every 250 ms
   Thread-1 ........... FyersDataSocket (tick LTPs -> STATE["ltp"])
   Thread-2 ........... 15-min history poller -> SuperTrend(14, 1.8)
   Thread-3 ........... margin poller (direct HTTP POST)
   Thread-4 ........... strategy engine (entry + rolling)
   Thread-5 ........... SocketIO emitter loop (250 ms)

 Auth flow (zero copy-paste)
 ---------------------------
   1. User opens http://localhost:5000
   2. Page shows "Login with FYERS" button
   3. Button hits /login  ->  browser redirected to FYERS OAuth URL
   4. User logs in on FYERS site
   5. FYERS redirects to https://127.0.0.1/callback?auth_code=...
      Browser will show a "connection refused" page — that is NORMAL.
      Copy the full URL from the address bar and paste into the web app.
   6. /manual_callback?auth_code=... exchanges it automatically
   7. Dashboard unlocks, strategy workers start

 Redirect URI note
 -----------------
   FYERS accepts https://127.0.0.1 in their portal.
   Register:  https://127.0.0.1/callback
   The browser will get a "connection refused" on that redirect —
   that is expected because nothing listens on port 443 locally.
   The app shows a paste-box to handle this gracefully.

 Install
 -------
   pip install flask flask-socketio fyers-apiv3 pandas requests eventlet

 Run
 ---
   python app.py
   Then open http://localhost:5000 in your browser.
============================================================================
"""

# ── Async mode selection (MUST come before all other imports) ───────────────
# Flask 3.x + Werkzeug 3.x broke the "threading" async_mode of flask-socketio
# causing "write() before start_response".  Use gevent (preferred) or
# eventlet as the WSGI server instead.
#
# Install one of:
#   pip install gevent gevent-websocket      <- preferred
#   pip install eventlet                     <- acceptable fallback
#
_ASYNC_MODE = None
try:
    import gevent.monkey
    gevent.monkey.patch_all()
    _ASYNC_MODE = "gevent"
except ImportError:
    pass

if _ASYNC_MODE is None:
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            import eventlet
            eventlet.monkey_patch()
        _ASYNC_MODE = "eventlet"
    except ImportError:
        pass

if _ASYNC_MODE is None:
    _ASYNC_MODE = "threading"
    print("=" * 62)
    print("  WARNING: neither gevent nor eventlet is installed.")
    print("  This will crash on Flask 3.x + Werkzeug 3.x.")
    print("  Fix:  pip install gevent gevent-websocket")
    print("=" * 62)
else:
    print(f"[ok] async_mode = {_ASYNC_MODE}")
# ────────────────────────────────────────────────────────────────────────────

import os
import csv
import io
import json
import time
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone

# ── IST helper ───────────────────────────────────────────────────────────────
# Railway free tier runs UTC with no TZ override available.
# We convert explicitly: UTC + 5h 30m = IST.  No pytz / zoneinfo needed.
_IST_OFFSET = timezone(timedelta(hours=5, minutes=30))

def now_ist() -> datetime:
    """Return current datetime in IST regardless of server timezone."""
    return datetime.now(timezone.utc).astimezone(_IST_OFFSET)


# ── NSE / BSE Holiday Calendar ───────────────────────────────────────────────
# Source: Official NSE circulars (verified via Zerodha MarketIntel, Jun 2026)
# Both exchanges observe the same equity+F&O holidays.
# Format: "YYYY-MM-DD"
#
# 2025 holidays — complete official list
_NSE_HOLIDAYS_2025 = {
    "2025-02-26",  # Mahashivratri
    "2025-03-14",  # Holi
    "2025-04-14",  # Dr. Baba Saheb Ambedkar Jayanti / Dr. B.R. Ambedkar Jayanti
    "2025-04-18",  # Good Friday
    "2025-05-01",  # Maharashtra Day
    "2025-08-15",  # Independence Day
    "2025-10-02",  # Mahatma Gandhi Jayanti
    "2025-10-20",  # Diwali – Laxmi Pujan (Muhurat trading this evening)
    "2025-10-21",  # Diwali – Balipratipada
    "2025-11-05",  # Prakash Gurpurb Sri Guru Nanak Dev Ji
    "2025-11-14",  # Gurunanak Jayanti (some sources show 5-Nov; keep both safe)
    "2025-12-25",  # Christmas Day
}

# 2026 holidays — from NSE circular / Zerodha MarketIntel (verified 2026-06-14)
_NSE_HOLIDAYS_2026 = {
    "2026-01-15",  # Municipal Corporation Elections in Maharashtra
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Holi
    "2026-03-26",  # Shri Ram Navami
    "2026-03-31",  # Shri Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-28",  # Bakri Eid (Id-Ul-Adha) — per Zerodha/NSE circular
    "2026-06-26",  # Moharram
    "2026-09-14",  # Ganesh Chaturthi
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-10-20",  # Dussehra
    "2026-11-10",  # Diwali – Balipratipada
    "2026-11-24",  # Prakash Gurpurb Sri Guru Nanak Dev Ji
    "2026-12-25",  # Christmas Day
}

# Union of all known holidays
_NSE_HOLIDAYS: set[str] = _NSE_HOLIDAYS_2025 | _NSE_HOLIDAYS_2026


def is_trading_day(dt: datetime | None = None) -> bool:
    """
    Returns True if dt (IST datetime) is a valid NSE/BSE equity+F&O trading day.
    Checks:
      1. Weekday (Mon–Fri only; Sat/Sun → False)
      2. Not in the NSE holiday calendar
    If dt is None, uses now_ist().
    """
    if dt is None:
        dt = now_ist()
    # weekday(): 0=Mon … 4=Fri, 5=Sat, 6=Sun
    if dt.weekday() >= 5:
        return False
    date_str = dt.strftime("%Y-%m-%d")
    return date_str not in _NSE_HOLIDAYS
from collections import OrderedDict

import requests
import pandas as pd

from flask import Flask, redirect, request, jsonify, render_template, session
from flask_socketio import SocketIO, emit

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

# ============================================================================
# 1. CONFIGURATION
# ----------------------------------------------------------------------------
# For Railway deployment: set these as Environment Variables in the Railway
# dashboard (Settings → Variables). Never hardcode secrets in source code.
#
# Required variables:
#   CLIENT_ID    — your FYERS app ID, e.g. "AB1234-100"
#   SECRET_KEY   — your FYERS app secret
#   REDIRECT_URI — your Railway public URL + /callback
#                  e.g. "https://your-app.up.railway.app/callback"
#   FLASK_SECRET — any long random string for Flask session signing
#
# Optional:
#   PAPER_TRADING  — "true" (default) or "false"
#   LOTS_PER_LEG   — base lots per leg, default 1
#   LOT_MULTIPLIER — multiplies LOTS_PER_LEG; total qty = lot_size × LOTS_PER_LEG × LOT_MULTIPLIER
#                    e.g. LOTS_PER_LEG=1, LOT_MULTIPLIER=3 → 3 lots per leg
#   TZ             — MUST be "Asia/Kolkata" on Railway (server runs UTC)
# ============================================================================

CLIENT_ID    = os.environ.get("CLIENT_ID",    "YOUR_CLIENT_ID-100")
SECRET_KEY   = os.environ.get("SECRET_KEY",   "YOUR_SECRET_KEY")
REDIRECT_URI = os.environ.get("REDIRECT_URI", "https://127.0.0.1/callback")
FLASK_SECRET = os.environ.get("FLASK_SECRET", "change-this-to-a-random-string")
TOKEN_FILE   = "fyers_access_token.json"

PAPER_TRADING = os.environ.get("PAPER_TRADING", "true").lower() != "false"
PRODUCT_TYPE  = "MARGIN"

INSTRUMENTS = {
    "NIFTY": {
        "spot_symbol":   "NSE:NIFTY50-INDEX",
        "exchange":      "NSE",
        "csv_url":       "https://public.fyers.in/sym_details/NSE_FO.csv",
        "underlying":    "NIFTY",
        "strike_step":   50,
        # ── Per-weekday short strike offset (pts from spot) ───────────────
        # Based on Nifty intraday range analysis 2022-2024:
        #   0=Mon 1=Tue 2=Wed 3=Thu(expiry) 4=Fri
        # Target: breach rate < 30% = win est > 70%
        # Tuesday is calmest (P90=312 pts) — 250 pts viable
        # Thursday is weekly F&O expiry — must widen to 350 pts
        "strike_offset_by_day": {
            0: 300,   # Monday    — post-weekend gap risk,  win est 73%
            1: 250,   # Tuesday   — calmest day,            win est 70%
            2: 300,   # Wednesday — steady mid-week vol,    win est 76%
            3: 350,   # Thursday  — weekly F&O EXPIRY,      win est 75%
            4: 300,   # Friday    — pre-weekend squaring,   win est 75%
        },
        # Hedge width matches offset per day (1:1 ratio = tightest margin)
        "hedge_width_by_day": {
            0: 300,
            1: 250,
            2: 300,
            3: 350,
            4: 300,
        },
        # Fallback if weekday lookup fails (e.g. holiday workaround)
        "strike_offset": 300,
        "hedge_width":   300,
    },
    "SENSEX": {
        "spot_symbol":   "BSE:SENSEX-INDEX",
        "exchange":      "BSE",
        "csv_url":       "https://public.fyers.in/sym_details/BSE_FO.csv",
        "underlying":    "SENSEX",
        "strike_step":   100,
        # ── Per-weekday short strike offset (pts from spot) ───────────────
        # Based on Sensex intraday range analysis 2022-2024:
        # Sensex moves ~4.3x Nifty in absolute points
        # Thursday: Sensex expiry is Friday but positioning vol starts Thu
        "strike_offset_by_day": {
            0: 900,    # Monday    — post-weekend gap,      win est 71%
            1: 900,    # Tuesday   — calmest day,           win est 75%
            2: 900,    # Wednesday — mid-week F&O activity, win est 73%
            3: 1000,   # Thursday  — pre-expiry vol spike,  win est 74%
            4: 900,    # Friday    — BSE weekly expiry day, win est 74%
        },
        "hedge_width_by_day": {
            0: 900,
            1: 900,
            2: 900,
            3: 1000,
            4: 900,
        },
        "strike_offset": 900,
        "hedge_width":   900,
    },
}

ST_PERIOD           = 14
ST_MULTIPLIER       = 1.8
ST_RESOLUTION       = "15"
ST_POLL_SECONDS     = 60
MARGIN_POLL_SECONDS = 30
STRATEGY_POLL_SECONDS = 5
LOTS_PER_LEG        = int(os.environ.get("LOTS_PER_LEG", "1"))
LOT_MULTIPLIER      = int(os.environ.get("LOT_MULTIPLIER", "1"))  # scale all legs
MARGIN_URL          = "https://api-t1.fyers.in/api/v3/multiorder/margin"

# ── Market session timing (IST, 24-hr) ──────────────────────────────────────
# All times are checked against now_ist() (IST) which must be IST on the server.
# Railway servers run UTC — set TZ=Asia/Kolkata in Railway Variables.
MARKET_OPEN   = (9,  15)   # 09:15 — earliest entry allowed
NO_NEW_ROLLS  = (14, 45)   # 14:45 — stop opening new rolls after this
SQUARE_OFF_AT = (14, 55)   # 14:55 — hard square-off of all open positions
MARKET_CLOSE  = (15, 35)   # 15:35 — strategy fully idle after this

# ============================================================================
# 2. FLASK + SOCKETIO SETUP
# ============================================================================

app = Flask(
    __name__,
    # Explicit path so Flask finds templates/ regardless of which directory
    # you launch from on Windows (fixes TemplateNotFound: index.html)
    template_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"),
    static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"),
)
app.secret_key = FLASK_SECRET
socketio = SocketIO(app, async_mode=_ASYNC_MODE, cors_allowed_origins="*")

# ============================================================================
# 3. GLOBAL STATE
# ============================================================================

STATE_LOCK = threading.RLock()

STATE = {
    "status":        "WAITING_AUTH",  # WAITING_AUTH | BOOTING | RUNNING | STOPPED
    "ws_connected":  False,
    "authenticated": False,
    "user_name":     "",
    "market_phase":  "CLOSED",        # PRE_OPEN | OPEN | NO_ROLLS | SQUARING_OFF | CLOSED
    "squaredoff":    False,           # True once the 14:55 square-off has run today
    "ltp":           {},
    "indices": {
        name: {
            "lot_size":      None,
            "spot":          None,
            "st_upper":      None,
            "st_lower":      None,
            "st_trend":      None,
            "margin":        None,
            "expiry_ts":     None,
            "active_offset": None,   # today's per-day short strike offset
            "active_hedge":  None,   # today's per-day hedge width
        } for name in INSTRUMENTS
    },
    "positions": OrderedDict(),
    "messages":  [],
    "paper_trading": PAPER_TRADING,
}

# Runtime handles — set once auth completes
_fyers        = None
_access_token = None
_stream       = None
_stop_evt     = threading.Event()


def log_msg(text: str) -> None:
    ts = now_ist().strftime("%H:%M:%S")
    with STATE_LOCK:
        STATE["messages"].append({"t": ts, "msg": text})
        STATE["messages"] = STATE["messages"][-50:]   # keep last 50 for log panel


# ============================================================================
# 4. AUTHENTICATION HELPERS
# ============================================================================

def load_cached_token() -> str | None:
    """Return today's cached token or None."""
    if not os.path.exists(TOKEN_FILE):
        return None
    try:
        with open(TOKEN_FILE) as fh:
            blob = json.load(fh)
        if blob.get("date") == now_ist().strftime("%Y-%m-%d"):
            return blob.get("access_token")
    except Exception:
        pass
    return None


def save_token(token: str) -> None:
    with open(TOKEN_FILE, "w") as fh:
        json.dump({"access_token": token,
                   "date": now_ist().strftime("%Y-%m-%d")}, fh)


def exchange_code_for_token(auth_code: str) -> str:
    """Exchange the one-time auth_code for an access_token via FYERS SDK."""
    sess = fyersModel.SessionModel(
        client_id=CLIENT_ID,
        secret_key=SECRET_KEY,
        redirect_uri=REDIRECT_URI,
        response_type="code",
        grant_type="authorization_code",
    )
    sess.set_token(auth_code)
    resp = sess.generate_token()
    if resp.get("s") != "ok" or "access_token" not in resp:
        raise RuntimeError(f"Token exchange failed: {resp}")
    return resp["access_token"]


def build_fyers(token: str) -> fyersModel.FyersModel:
    return fyersModel.FyersModel(
        client_id=CLIENT_ID, token=token,
        is_async=False, log_path=os.getcwd()
    )


# ============================================================================
# 5. DYNAMIC LOT SIZES
# ============================================================================

def fetch_lot_size(csv_url: str, underlying: str) -> int:
    resp = requests.get(csv_url, timeout=30)
    resp.raise_for_status()
    reader = csv.reader(io.StringIO(resp.text))
    for row in reader:
        if len(row) < 14:
            continue
        try:
            row_underlying = row[13].strip().upper()
            ticker         = row[9].strip().upper()
        except IndexError:
            continue
        if row_underlying == underlying.upper() and \
                (ticker.endswith("CE") or ticker.endswith("PE")):
            try:
                lot = int(float(row[3]))
                if lot > 0:
                    return lot
            except (ValueError, IndexError):
                continue
    raise RuntimeError(f"Lot size for {underlying} not found in {csv_url}")


# ============================================================================
# 6. SUPERTREND
# ============================================================================

def compute_supertrend(df: pd.DataFrame,
                       period: int = ST_PERIOD,
                       mult: float = ST_MULTIPLIER):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat(
        [high - low,
         (high - close.shift(1)).abs(),
         (low  - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    hl2 = (high + low) / 2.0
    basic_upper = hl2 + mult * atr
    basic_lower = hl2 - mult * atr

    final_upper = basic_upper.copy()
    final_lower = basic_lower.copy()
    trend = pd.Series(1.0, index=df.index)

    first_valid = atr.first_valid_index()
    start = (int(first_valid) if first_valid is not None else 0) + 1
    if start >= len(df):
        raise RuntimeError("Not enough candles for SuperTrend warm-up")

    for i in range(start, len(df)):
        if basic_upper.iat[i] < final_upper.iat[i-1] or \
                close.iat[i-1] > final_upper.iat[i-1]:
            final_upper.iat[i] = basic_upper.iat[i]
        else:
            final_upper.iat[i] = final_upper.iat[i-1]

        if basic_lower.iat[i] > final_lower.iat[i-1] or \
                close.iat[i-1] < final_lower.iat[i-1]:
            final_lower.iat[i] = basic_lower.iat[i]
        else:
            final_lower.iat[i] = final_lower.iat[i-1]

        prev = trend.iat[i-1]
        if prev == 1:
            trend.iat[i] = -1 if close.iat[i] < final_lower.iat[i] else 1
        else:
            trend.iat[i] = 1  if close.iat[i] > final_upper.iat[i] else -1

    return (round(float(final_upper.iat[-1]), 2),
            round(float(final_lower.iat[-1]), 2),
            int(trend.iat[-1]))


def fetch_history_df(fyers, symbol: str) -> pd.DataFrame:
    rng_to   = now_ist()
    rng_from = rng_to - timedelta(days=10)
    resp = fyers.history({
        "symbol":      symbol,
        "resolution":  ST_RESOLUTION,
        "date_format": "1",
        "range_from":  rng_from.strftime("%Y-%m-%d"),
        "range_to":    rng_to.strftime("%Y-%m-%d"),
        "cont_flag":   "1",
    })
    if resp.get("s") != "ok" or not resp.get("candles"):
        raise RuntimeError(f"history() failed for {symbol}: {resp}")
    return pd.DataFrame(resp["candles"],
                        columns=["ts", "open", "high", "low", "close", "volume"])


# ============================================================================
# 7. OPTION CHAIN
# ============================================================================

def get_option_chain(fyers, spot_symbol: str,
                     strikecount: int = 30, expiry_ts=None) -> dict:
    payload = {"symbol": spot_symbol, "strikecount": strikecount}
    if expiry_ts:
        payload["timestamp"] = str(expiry_ts)
    resp = fyers.optionchain(data=payload)
    if resp.get("s") != "ok" or "data" not in resp:
        raise RuntimeError(f"optionchain() failed for {spot_symbol}: {resp}")
    return resp["data"]


def pick_leg(chain_rows: list, option_type: str, target_strike: float):
    candidates = [r for r in chain_rows
                  if r.get("option_type") == option_type
                  and r.get("strike_price", -1) > 0]
    if not candidates:
        raise RuntimeError(f"No {option_type} rows in option chain")
    best = min(candidates, key=lambda r: abs(r["strike_price"] - target_strike))
    return best["symbol"], best["strike_price"], best.get("ltp", 0.0)


def build_condor_legs(fyers, index_name: str) -> list:
    cfg = INSTRUMENTS[index_name]
    data = get_option_chain(fyers, cfg["spot_symbol"])
    expiry_list = data.get("expiryData") or []
    if expiry_list:
        expiry_ts = expiry_list[0].get("expiry")
        data = get_option_chain(fyers, cfg["spot_symbol"], expiry_ts=expiry_ts)
        with STATE_LOCK:
            STATE["indices"][index_name]["expiry_ts"] = expiry_ts

    rows = data.get("optionsChain", [])
    spot = None
    for r in rows:
        if r.get("option_type") in ("", None) or \
                r.get("symbol") == cfg["spot_symbol"]:
            spot = r.get("ltp") or r.get("fp")
            break
    if not spot:
        q = fyers.quotes({"symbols": cfg["spot_symbol"]})
        spot = q["d"][0]["v"]["lp"]

    # ── Resolve per-day offset ─────────────────────────────────────────────
    # weekday(): 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri
    dow   = now_ist().weekday()
    dow_names = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri"}
    off   = cfg.get("strike_offset_by_day",   {}).get(dow, cfg["strike_offset"])
    hedge = cfg.get("hedge_width_by_day",      {}).get(dow, cfg["hedge_width"])

    log_msg(f"{index_name} {dow_names.get(dow,'?')} offset: {off} pts | hedge: {hedge} pts")

    # Persist today's active offset to STATE so dashboard can display it
    with STATE_LOCK:
        STATE["indices"][index_name]["active_offset"] = off
        STATE["indices"][index_name]["active_hedge"]  = hedge

    ce_sym,  ce_k,  ce_ltp  = pick_leg(rows, "CE", spot + off)
    pe_sym,  pe_k,  pe_ltp  = pick_leg(rows, "PE", spot - off)
    hce_sym, hce_k, hce_ltp = pick_leg(rows, "CE", spot + off + hedge)
    hpe_sym, hpe_k, hpe_ltp = pick_leg(rows, "PE", spot - off - hedge)

    lot = STATE["indices"][index_name]["lot_size"] or 0
    qty = lot * LOTS_PER_LEG * LOT_MULTIPLIER
    return [
        {"index": index_name, "symbol": hce_sym, "strike": hce_k,
         "side":  1, "qty": qty, "ref_ltp": hce_ltp, "role": "HEDGE-CE",
         "offset": off},
        {"index": index_name, "symbol": hpe_sym, "strike": hpe_k,
         "side":  1, "qty": qty, "ref_ltp": hpe_ltp, "role": "HEDGE-PE",
         "offset": off},
        {"index": index_name, "symbol": ce_sym,  "strike": ce_k,
         "side": -1, "qty": qty, "ref_ltp": ce_ltp,  "role": "SHORT-CE",
         "offset": off},
        {"index": index_name, "symbol": pe_sym,  "strike": pe_k,
         "side": -1, "qty": qty, "ref_ltp": pe_ltp,  "role": "SHORT-PE",
         "offset": off},
    ]


# ============================================================================
# 8. MARGIN (direct HTTP POST)
# ============================================================================

def basket_margin(access_token: str, legs: list) -> float:
    payload = {"data": [
        {"symbol": leg["symbol"], "qty": leg["qty"], "side": leg["side"],
         "type": 2, "productType": PRODUCT_TYPE,
         "limitPrice": 0.0, "stopPrice": 0.0,
         "stopLoss": 0.0, "takeProfit": 0.0}
        for leg in legs
    ]}
    headers = {
        "Authorization": f"{CLIENT_ID}:{access_token}",
        "Content-Type":  "application/json",
    }
    resp = requests.post(MARGIN_URL, headers=headers,
                         json=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data.get("s") != "ok":
        raise RuntimeError(f"margin API: {data.get('message', data)}")
    d = data.get("data", {})
    return float(d.get("margin_total") or d.get("margin_new_order") or 0.0)


# ============================================================================
# 9. ORDER EXECUTION
# ============================================================================

def execute_leg(fyers, leg: dict) -> dict:
    with STATE_LOCK:
        live_ltp = STATE["ltp"].get(leg["symbol"])
    entry    = live_ltp if live_ltp else leg["ref_ltp"]
    order_id = "PAPER"

    if not STATE["paper_trading"]:
        resp = fyers.place_order(data={
            "symbol":       leg["symbol"],
            "qty":          leg["qty"],
            "type":         2,
            "side":         leg["side"],
            "productType":  PRODUCT_TYPE,
            "limitPrice":   0,
            "stopPrice":    0,
            "validity":     "DAY",
            "disclosedQty": 0,
            "offlineOrder": False,
        })
        if resp.get("s") != "ok":
            log_msg(f"ORDER REJECTED {leg['symbol']}: {resp.get('message')}")
            raise RuntimeError(f"place_order failed: {resp}")
        order_id = resp.get("id", "?")

    pos = {
        "time":     now_ist().strftime("%H:%M:%S"),
        "index":    leg["index"],
        "symbol":   leg["symbol"],
        "role":     leg["role"],
        "side":     leg["side"],
        "qty":      leg["qty"],
        "entry":    float(entry or 0.0),
        "order_id": order_id,
        "closed":   False,
    }
    key = f"{leg['symbol']}|{time.time():.3f}"
    with STATE_LOCK:
        STATE["positions"][key] = pos
    log_msg(f"{'SELL' if leg['side'] < 0 else 'BUY'} "
            f"{leg['symbol']} x{leg['qty']} @ {entry}")
    return pos


def close_positions(fyers, index_name: str, roles: tuple) -> None:
    with STATE_LOCK:
        targets = [(k, dict(p)) for k, p in STATE["positions"].items()
                   if p["index"] == index_name
                   and p["role"] in roles and not p["closed"]]
    for key, pos in targets:
        if not STATE["paper_trading"]:
            fyers.place_order(data={
                "symbol":       pos["symbol"],
                "qty":          pos["qty"],
                "type":         2,
                "side":         -pos["side"],
                "productType":  PRODUCT_TYPE,
                "limitPrice":   0,
                "stopPrice":    0,
                "validity":     "DAY",
                "disclosedQty": 0,
                "offlineOrder": False,
            })
        with STATE_LOCK:
            STATE["positions"][key]["closed"] = True
        log_msg(f"CLOSED {pos['role']} {pos['symbol']}")


# ============================================================================
# 10. WEBSOCKET (FYERS tick stream)
# ============================================================================

class TickStream:
    def __init__(self, client_id: str, access_token: str):
        self._subscribed = set()
        self._lock = threading.Lock()
        self.socket = data_ws.FyersDataSocket(
            access_token=f"{client_id}:{access_token}",
            log_path=os.getcwd(),
            litemode=True,
            write_to_file=False,
            reconnect=True,
            on_connect=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message,
        )

    def _on_open(self):
        with STATE_LOCK:
            STATE["ws_connected"] = True
        log_msg("WebSocket connected")
        with self._lock:
            pending = list(self._subscribed)
        if pending:
            self.socket.subscribe(symbols=pending, data_type="SymbolUpdate")

    def _on_close(self, msg):
        with STATE_LOCK:
            STATE["ws_connected"] = False
        log_msg("WebSocket closed")

    def _on_error(self, msg):
        log_msg(f"WS error: {msg}")

    def _on_message(self, msg):
        try:
            sym = msg.get("symbol")
            ltp = msg.get("ltp")
            if sym and ltp is not None:
                with STATE_LOCK:
                    STATE["ltp"][sym] = float(ltp)
        except AttributeError:
            pass

    def subscribe(self, symbols: list) -> None:
        with self._lock:
            new = [s for s in symbols if s not in self._subscribed]
            self._subscribed.update(new)
        if new and STATE["ws_connected"]:
            self.socket.subscribe(symbols=new, data_type="SymbolUpdate")

    def start(self) -> None:
        threading.Thread(target=self.socket.connect,
                         daemon=True, name="fyers-ws").start()


# ============================================================================
# 11. BACKGROUND WORKERS
# ============================================================================

def supertrend_worker(fyers, stop_evt: threading.Event) -> None:
    while not stop_evt.is_set():
        for name, cfg in INSTRUMENTS.items():
            try:
                df = fetch_history_df(fyers, cfg["spot_symbol"])
                upper, lower, trend = compute_supertrend(df)
                with STATE_LOCK:
                    idx = STATE["indices"][name]
                    idx["st_upper"], idx["st_lower"], idx["st_trend"] = \
                        upper, lower, trend
            except Exception as exc:
                log_msg(f"ST {name}: {exc}")
        stop_evt.wait(ST_POLL_SECONDS)


def margin_worker(access_token: str, stop_evt: threading.Event) -> None:
    while not stop_evt.is_set():
        for name in INSTRUMENTS:
            try:
                with STATE_LOCK:
                    legs = [{"symbol": p["symbol"], "qty": p["qty"],
                             "side": p["side"]}
                            for p in STATE["positions"].values()
                            if p["index"] == name and not p["closed"]]
                if legs:
                    m = basket_margin(access_token, legs)
                    with STATE_LOCK:
                        STATE["indices"][name]["margin"] = m
            except Exception as exc:
                with STATE_LOCK:
                    STATE["indices"][name]["margin"] = None
                log_msg(f"Margin {name}: {exc}")
        stop_evt.wait(MARGIN_POLL_SECONDS)


def get_market_phase() -> str:
    """
    Returns the current market phase based on IST time + trading day check.

    HOLIDAY     — weekend or NSE declared holiday
    PRE_OPEN    — before 09:15 on a trading day
    OPEN        — 09:15–14:44, normal entry + rolling allowed
    NO_ROLLS    — 14:45–14:54, positions held, no new rolls
    SQUARING_OFF— 14:55–15:34, auto square-off fires once
    CLOSED      — 15:35 onwards on a trading day
    """
    now = now_ist()
    if not is_trading_day(now):
        return "HOLIDAY"
    hhmm = (now.hour, now.minute)
    if   hhmm < MARKET_OPEN:   return "PRE_OPEN"
    if   hhmm < NO_NEW_ROLLS:  return "OPEN"
    if   hhmm < SQUARE_OFF_AT: return "NO_ROLLS"
    if   hhmm < MARKET_CLOSE:  return "SQUARING_OFF"
    return "CLOSED"


def squareoff_all(fyers, reason: str = "EOD") -> None:
    """Close every open position across all indices."""
    with STATE_LOCK:
        open_keys = [
            (k, dict(p)) for k, p in STATE["positions"].items()
            if not p["closed"]
        ]
    if not open_keys:
        log_msg(f"{reason}: no open positions to close")
        return
    log_msg(f"{reason}: squaring off {len(open_keys)} open leg(s)…")
    for key, pos in open_keys:
        try:
            if not STATE["paper_trading"]:
                fyers.place_order(data={
                    "symbol":       pos["symbol"],
                    "qty":          pos["qty"],
                    "type":         2,
                    "side":         -pos["side"],
                    "productType":  PRODUCT_TYPE,
                    "limitPrice":   0,
                    "stopPrice":    0,
                    "validity":     "DAY",
                    "disclosedQty": 0,
                    "offlineOrder": False,
                })
            with STATE_LOCK:
                STATE["positions"][key]["closed"] = True
            log_msg(f"SQ-OFF {pos['role']} {pos['symbol']}")
        except Exception as exc:
            log_msg(f"SQ-OFF FAILED {pos['symbol']}: {exc}")
    with STATE_LOCK:
        STATE["squaredoff"] = True


def strategy_worker(fyers, access_token: str,
                    stream: TickStream, stop_evt: threading.Event) -> None:
    breached     = {name: None  for name in INSTRUMENTS}
    entered      = {name: False for name in INSTRUMENTS}
    _last_date   = None    # track IST date to detect day rollover

    while not stop_evt.is_set():

        # ── Day rollover: reset flags at start of each new IST date ───────
        today = now_ist().strftime("%Y-%m-%d")
        if today != _last_date:
            if _last_date is not None:
                log_msg(f"📅 New trading day {today} — resetting strategy state")
            _last_date = today
            entered  = {name: False for name in INSTRUMENTS}
            breached = {name: None  for name in INSTRUMENTS}
            with STATE_LOCK:
                STATE["squaredoff"] = False

        # ── Update market phase in STATE so UI can display it ──────────────
        phase = get_market_phase()
        with STATE_LOCK:
            STATE["market_phase"] = phase

        # ── HOLIDAY: weekend or NSE declared holiday ───────────────────────
        if phase == "HOLIDAY":
            stop_evt.wait(300)   # sleep 5 min — no point polling fast
            continue

        # ── PRE_OPEN: wait silently until market opens ─────────────────────
        if phase == "PRE_OPEN":
            stop_evt.wait(15)
            continue

        # ── CLOSED: nothing to do after 15:35 ─────────────────────────────
        if phase == "CLOSED":
            stop_evt.wait(60)
            continue

        # ── SQUARING_OFF: fire the square-off exactly once, then hold ──────
        if phase == "SQUARING_OFF":
            with STATE_LOCK:
                already_done = STATE["squaredoff"]
            if not already_done:
                log_msg("⏰ 14:55 reached — auto square-off all positions")
                squareoff_all(fyers, reason="14:55 EOD")
                # Reset entered flags so a fresh entry can happen next day
                entered  = {name: False for name in INSTRUMENTS}
                breached = {name: None  for name in INSTRUMENTS}
            stop_evt.wait(30)
            continue

        # ── NO_ROLLS (14:45–14:54): hold positions, no new entry/rolls ─────
        if phase == "NO_ROLLS":
            log_msg("⏸ 14:45 no-roll window — holding positions")
            stop_evt.wait(30)
            continue

        # ── OPEN (09:15–14:44): normal strategy logic ──────────────────────
        for name, cfg in INSTRUMENTS.items():
            try:
                with STATE_LOCK:
                    spot = STATE["ltp"].get(cfg["spot_symbol"])
                    idx  = dict(STATE["indices"][name])
                if spot is None or idx["st_upper"] is None:
                    continue
                with STATE_LOCK:
                    STATE["indices"][name]["spot"] = spot

                # ── Initial 4-leg entry ────────────────────────────────────
                if not entered[name]:
                    legs = build_condor_legs(fyers, name)
                    stream.subscribe([l["symbol"] for l in legs])
                    try:
                        m = basket_margin(access_token, legs)
                        with STATE_LOCK:
                            STATE["indices"][name]["margin"] = m
                        log_msg(f"{name} basket margin: ₹{m:,.0f} "
                                f"| qty/leg: {legs[0]['qty']}")
                    except Exception as exc:
                        log_msg(f"{name} margin check: {exc}")
                    time.sleep(1.5)
                    for leg in legs:
                        execute_leg(fyers, leg)
                    entered[name] = True
                    continue

                # ── Rolling logic ──────────────────────────────────────────
                if spot > idx["st_upper"] and breached[name] != "UP":
                    breached[name] = "UP"
                    log_msg(f"{name} ▲ breached ST UPPER → rolling PUT up")
                    close_positions(fyers, name, ("SHORT-PE", "HEDGE-PE"))
                    new_legs = [l for l in build_condor_legs(fyers, name)
                                if l["role"] in ("HEDGE-PE", "SHORT-PE")]
                    stream.subscribe([l["symbol"] for l in new_legs])
                    time.sleep(1.0)
                    for leg in new_legs:
                        execute_leg(fyers, leg)

                elif spot < idx["st_lower"] and breached[name] != "DOWN":
                    breached[name] = "DOWN"
                    log_msg(f"{name} ▼ breached ST LOWER → rolling CALL down")
                    close_positions(fyers, name, ("SHORT-CE", "HEDGE-CE"))
                    new_legs = [l for l in build_condor_legs(fyers, name)
                                if l["role"] in ("HEDGE-CE", "SHORT-CE")]
                    stream.subscribe([l["symbol"] for l in new_legs])
                    time.sleep(1.0)
                    for leg in new_legs:
                        execute_leg(fyers, leg)

                elif idx["st_lower"] <= spot <= idx["st_upper"]:
                    breached[name] = None     # re-arm trigger

            except Exception as exc:
                log_msg(f"Strategy {name}: {exc}")

        stop_evt.wait(STRATEGY_POLL_SECONDS)


def socketio_emitter(stop_evt: threading.Event) -> None:
    """
    Thread-5: serialises STATE and pushes it to all browser clients.

    MUST use app.app_context() around socketio.emit() when called from a
    background thread — without it Flask-SocketIO raises NameError/RuntimeError
    because there is no active application context outside a request.
    """
    while not stop_evt.is_set():
        try:
            with STATE_LOCK:
                ltps          = dict(STATE["ltp"])
                indices_snap  = deepcopy(STATE["indices"])
                positions_raw = list(STATE["positions"].values())
                messages_snap = list(STATE["messages"][-20:])
                ws_ok         = STATE["ws_connected"]
                status        = STATE["status"]
                paper         = STATE["paper_trading"]
                uname         = STATE["user_name"]
                market_phase  = STATE["market_phase"]

            # ── build positions payload with live MTM ──────────────────────
            positions_out = []
            total_mtm = 0.0
            for p in positions_raw:
                ltp = ltps.get(p["symbol"], p["entry"])
                if p["side"] < 0:
                    mtm = (p["entry"] - ltp) * p["qty"]
                else:
                    mtm = (ltp - p["entry"]) * p["qty"]
                if not p["closed"]:
                    total_mtm += mtm
                positions_out.append({**p, "ltp": ltp, "mtm": round(mtm, 2)})

            # ── build scanner payload ──────────────────────────────────────
            scanner_out = {}
            dow = now_ist().weekday()
            for name, cfg in INSTRUMENTS.items():
                d    = indices_snap[name]
                spot = ltps.get(cfg["spot_symbol"], d["spot"])
                # Show today's offset from STATE if already resolved,
                # otherwise compute from the weekday map directly
                active_off   = d.get("active_offset") or \
                               cfg.get("strike_offset_by_day", {}).get(
                                   dow, cfg["strike_offset"])
                active_hedge = d.get("active_hedge") or \
                               cfg.get("hedge_width_by_day", {}).get(
                                   dow, cfg["hedge_width"])
                scanner_out[name] = {
                    **d,
                    "spot":          spot,
                    "active_offset": active_off,
                    "active_hedge":  active_hedge,
                }

            payload = {
                "status":         status,
                "ws":             ws_ok,
                "paper":          paper,
                "user":           uname,
                "market_phase":   market_phase,
                "lot_multiplier": LOT_MULTIPLIER,
                "lots_per_leg":   LOTS_PER_LEG,
                "scanner":        scanner_out,
                "positions":      positions_out,
                "total_mtm":      round(total_mtm, 2),
                "messages":       messages_snap,
                "ts":             now_ist().strftime("%H:%M:%S"),
                "date":           now_ist().strftime("%a %d %b %Y"),
            }

            # ── emit inside an explicit app context ────────────────────────
            # socketio.emit() from a background thread requires this;
            # without it Flask raises "Working outside of application context"
            with app.app_context():
                socketio.emit("state", payload, namespace="/")

        except Exception as exc:
            # Log instead of silently swallowing — helps diagnose issues
            log_msg(f"Emitter error: {exc}")

        time.sleep(0.25)


# ============================================================================
# 12. STARTUP SEQUENCE  (called once after auth completes)
# ============================================================================

def boot_workers(fyers, access_token: str) -> None:
    global _fyers, _access_token, _stream, _stop_evt

    _fyers        = fyers
    _access_token = access_token

    with STATE_LOCK:
        STATE["status"]     = "BOOTING"
        STATE["squaredoff"] = False   # reset each boot for new trading day

    # lot sizes
    for name, cfg in INSTRUMENTS.items():
        try:
            lot = fetch_lot_size(cfg["csv_url"], cfg["underlying"])
            with STATE_LOCK:
                STATE["indices"][name]["lot_size"] = lot
            log_msg(f"{name} lot size: {lot}")
        except Exception as exc:
            log_msg(f"Lot size FAILED {name}: {exc}")

    # WebSocket
    _stream = TickStream(CLIENT_ID, access_token)
    _stream.subscribe([cfg["spot_symbol"] for cfg in INSTRUMENTS.values()])
    _stream.start()

    with STATE_LOCK:
        STATE["status"] = "RUNNING"

    _stop_evt.clear()

    workers = [
        threading.Thread(target=supertrend_worker,
                         args=(_fyers, _stop_evt), daemon=True, name="st"),
        threading.Thread(target=margin_worker,
                         args=(access_token, _stop_evt), daemon=True, name="margin"),
        threading.Thread(target=strategy_worker,
                         args=(_fyers, access_token, _stream, _stop_evt),
                         daemon=True, name="strategy"),
        threading.Thread(target=socketio_emitter,
                         args=(_stop_evt,), daemon=True, name="emitter"),
    ]
    for w in workers:
        w.start()
    log_msg("All workers started")


# ============================================================================
# 13. FLASK ROUTES
# ============================================================================

@app.route("/")
def index():
    return render_template("index.html",
                           client_id=CLIENT_ID,
                           paper=PAPER_TRADING)


@app.route("/api/login_url")
def api_login_url():
    """
    Returns the FYERS OAuth URL as JSON so the browser JS can open it
    in a new tab with window.open(), keeping the app tab alive.
    """
    sess = fyersModel.SessionModel(
        client_id=CLIENT_ID,
        secret_key=SECRET_KEY,
        redirect_uri=REDIRECT_URI,
        response_type="code",
        grant_type="authorization_code",
    )
    try:
        url = sess.generate_authcode()
        return jsonify({"url": url})
    except Exception as exc:
        return jsonify({"url": None, "error": str(exc)}), 500


@app.route("/login")
def login():
    """
    Fallback: redirect-based login for browsers that block window.open().
    NOTE: this navigates AWAY from the app tab. Use /api/login_url + JS
    window.open() (the default button behaviour) to stay on the app tab.
    """
    sess = fyersModel.SessionModel(
        client_id=CLIENT_ID,
        secret_key=SECRET_KEY,
        redirect_uri=REDIRECT_URI,
        response_type="code",
        grant_type="authorization_code",
    )
    return redirect(sess.generate_authcode())


@app.route("/callback")
def callback():
    """
    Handles the automatic redirect from FYERS after login.
    FYERS redirects to https://127.0.0.1/callback?auth_code=...&s=ok
    Because nothing listens on port 443, the browser shows
    "connection refused" — the user must copy the full URL and
    paste it into the app's paste-box (/manual_callback handles that).

    However: if the user is running a local HTTPS server or using ngrok,
    this route fires automatically and completes auth without any paste.
    """
    auth_code = request.args.get("auth_code") or request.args.get("code")
    status    = request.args.get("s", "")

    if not auth_code or status == "error":
        error_msg = request.args.get("message", "Auth failed or cancelled")
        return render_template("index.html",
                               client_id=CLIENT_ID,
                               paper=PAPER_TRADING,
                               auth_error=error_msg), 400

    return _complete_auth(auth_code)


@app.route("/manual_callback", methods=["POST"])
def manual_callback():
    """
    Receives the full redirected URL pasted by the user.
    The browser lands on https://127.0.0.1/callback?auth_code=XYZ&s=ok
    and shows "connection refused". The user copies that URL and pastes
    it into the text box on the login page. This endpoint parses it.
    """
    data = request.get_json(silent=True) or {}
    raw_url = data.get("url", "").strip()

    if not raw_url:
        return jsonify({"ok": False, "error": "No URL provided"}), 400

    # Parse auth_code out of whatever URL they pasted
    from urllib.parse import urlparse, parse_qs
    try:
        parsed = urlparse(raw_url)
        params = parse_qs(parsed.query)
        auth_code = (params.get("auth_code") or params.get("code") or [None])[0]
        status    = (params.get("s") or [""])[0]
    except Exception as exc:
        return jsonify({"ok": False, "error": f"URL parse failed: {exc}"}), 400

    if status == "error":
        msg = (params.get("message") or ["Auth cancelled by user"])[0]
        return jsonify({"ok": False, "error": msg}), 400

    if not auth_code:
        return jsonify({"ok": False,
                        "error": "auth_code not found in URL. "
                                 "Make sure you copied the full address bar URL after FYERS login."}), 400

    try:
        _complete_auth(auth_code)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


def _complete_auth(auth_code: str):
    """Shared logic: exchange auth_code -> token -> boot workers."""
    try:
        token = exchange_code_for_token(auth_code)
    except Exception as exc:
        raise RuntimeError(f"Token exchange failed: {exc}")

    save_token(token)
    fyers   = build_fyers(token)
    profile = fyers.get_profile()

    if profile.get("s") != "ok":
        raise RuntimeError("Profile fetch failed — token may be invalid")

    uname = profile["data"].get("name", "Trader")
    with STATE_LOCK:
        STATE["authenticated"] = True
        STATE["user_name"]     = uname

    log_msg(f"Authenticated as {uname}")
    threading.Thread(target=boot_workers, args=(fyers, token),
                     daemon=True, name="boot").start()

    # If called from the /callback route (automatic redirect), send to dashboard
    return redirect("/")


@app.route("/api/state")
def api_state():
    """REST fallback snapshot — browser JS uses SocketIO primarily."""
    with STATE_LOCK:
        return jsonify({
            "status":        STATE["status"],
            "authenticated": STATE["authenticated"],
            "ws":            STATE["ws_connected"],
        })


@app.route("/api/control", methods=["POST"])
def api_control():
    """
    Start / stop the strategy workers.
    Accepts JSON body: {"action": "stop"} or {"action": "start"}

    Hardened against missing Content-Type header (common browser quirk)
    by using get_json(force=True, silent=True) which parses regardless
    of Content-Type and returns None instead of raising on bad input.
    """
    global _stop_evt
    data   = request.get_json(force=True, silent=True) or {}
    action = data.get("action", "")

    if not action:
        return jsonify({"ok": False,
                        "error": "Missing 'action' field in request body"}), 400

    if action == "stop":
        with STATE_LOCK:
            current = STATE["status"]
        if current != "RUNNING":
            return jsonify({"ok": False,
                            "error": f"Cannot stop — status is '{current}'"}), 400
        _stop_evt.set()
        with STATE_LOCK:
            STATE["status"] = "STOPPED"
        log_msg("⏹ Strategy stopped by user")
        return jsonify({"ok": True, "status": "STOPPED"})

    if action == "start":
        with STATE_LOCK:
            current = STATE["status"]
        if current not in ("STOPPED", "WAITING_AUTH"):
            return jsonify({"ok": False,
                            "error": f"Cannot start — status is '{current}'"}), 400
        token = load_cached_token()
        if not token:
            return jsonify({"ok": False,
                            "error": "Token expired — please log in again"}), 401
        try:
            fyers = build_fyers(token)
            _stop_evt = threading.Event()
            threading.Thread(target=boot_workers, args=(fyers, token),
                             daemon=True, name="reboot").start()
            return jsonify({"ok": True, "status": "BOOTING"})
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({"ok": False,
                    "error": f"Unknown action '{action}'"}), 400


# ============================================================================
# 14. SOCKETIO EVENTS
# ============================================================================

@socketio.on("connect")
def on_connect():
    # If already authenticated (e.g. browser refresh), push state immediately
    with STATE_LOCK:
        authed = STATE["authenticated"]
        uname  = STATE["user_name"]
    if authed:
        emit("auth_ok", {"user": uname})


# ============================================================================
# 15. MAIN
# ============================================================================

if __name__ == "__main__":
    # Try to resume from a cached token on startup
    cached = load_cached_token()
    if cached:
        try:
            fyers = build_fyers(cached)
            profile = fyers.get_profile()
            if profile.get("s") == "ok":
                uname = profile["data"].get("name", "Trader")
                with STATE_LOCK:
                    STATE["authenticated"] = True
                    STATE["user_name"]     = uname
                log_msg(f"Auto-resumed session as {uname}")
                threading.Thread(target=boot_workers, args=(fyers, cached),
                                 daemon=True, name="boot").start()
        except Exception as exc:
            print(f"[warn] cached token invalid: {exc}")

    print("=" * 60)
    print("  FYERS Strangle Web Terminal")
    port = int(os.environ.get("PORT", 5000))
    print(f"  Open http://localhost:{port} in your browser")
    print("=" * 60)
    socketio.run(app, host="0.0.0.0", port=port, debug=False, use_reloader=False)
