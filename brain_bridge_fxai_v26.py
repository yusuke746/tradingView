import os
import json
import time
import socket
from datetime import datetime, timezone, timedelta
from threading import Lock, Thread
from typing import Optional, Dict, Any, List

import zmq
import MetaTrader5 as mt5
from flask import Flask, request
from openai import OpenAI
from dotenv import load_dotenv

app = Flask(__name__)

# Load .env early
load_dotenv()


def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


# --- Core config ---
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "80"))
WEBHOOK_TOKEN = str(os.getenv("WEBHOOK_TOKEN", "") or "").strip()

SYMBOL = (os.getenv("SYMBOL", "GOLD") or "GOLD").strip().upper()

ZMQ_PORT = str(os.getenv("ZMQ_PORT", "5555") or "5555").strip()
ZMQ_BIND = str(os.getenv("ZMQ_BIND", "") or "").strip() or f"tcp://*:{ZMQ_PORT}"

ZMQ_HEARTBEAT_ENABLED = _env_bool("ZMQ_HEARTBEAT_ENABLED", "1")
ZMQ_HEARTBEAT_PORT = str(os.getenv("ZMQ_HEARTBEAT_PORT", "5556") or "5556").strip()
ZMQ_HEARTBEAT_BIND = str(os.getenv("ZMQ_HEARTBEAT_BIND", "") or "").strip() or f"tcp://*:{ZMQ_HEARTBEAT_PORT}"
ZMQ_HEARTBEAT_TIMEOUT_SEC = float(os.getenv("ZMQ_HEARTBEAT_TIMEOUT_SEC", "10"))

HEARTBEAT_STALE_MODE = str(os.getenv("HEARTBEAT_STALE_MODE", "freeze") or "freeze").strip().lower()


# --- Signal cache ---
CACHE_FILE = str(os.getenv("CACHE_FILE", "signals_cache.json") or "signals_cache.json").strip()
SIGNAL_LOOKBACK_SEC = int(os.getenv("SIGNAL_LOOKBACK_SEC", "1200"))
SIGNAL_MAX_AGE_SEC = int(os.getenv("SIGNAL_MAX_AGE_SEC", str(SIGNAL_LOOKBACK_SEC)))

CACHE_ASYNC_FLUSH_ENABLED = _env_bool("CACHE_ASYNC_FLUSH_ENABLED", "1")
CACHE_FLUSH_INTERVAL_SEC = float(os.getenv("CACHE_FLUSH_INTERVAL_SEC", "2.0"))
# Force flush even if signals keep arriving frequently
CACHE_FLUSH_FORCE_SEC = float(os.getenv("CACHE_FLUSH_FORCE_SEC", "10.0"))

CONFLUENCE_LOOKBACK_SEC = int(os.getenv("CONFLUENCE_LOOKBACK_SEC", "600"))
Q_TREND_MAX_AGE_SEC = int(os.getenv("Q_TREND_MAX_AGE_SEC", "300"))

ZONE_LOOKBACK_SEC = int(os.getenv("ZONE_LOOKBACK_SEC", "1200"))
ZONE_TOUCH_LOOKBACK_SEC = int(os.getenv("ZONE_TOUCH_LOOKBACK_SEC", "1200"))


# --- Behavior toggles ---
ASSUME_ACTION_IS_QTREND = _env_bool("ASSUME_ACTION_IS_QTREND", "0")
ALLOW_ADD_ON_ENTRIES = _env_bool("ALLOW_ADD_ON_ENTRIES", "1")
ALLOW_MULTI_ENTRY_SAME_QTREND = _env_bool("ALLOW_MULTI_ENTRY_SAME_QTREND", "0")
ADDON_MAX_ENTRIES_PER_QTREND = int(os.getenv("ADDON_MAX_ENTRIES_PER_QTREND", "2"))
ADDON_MAX_ENTRIES_PER_POSITION = int(os.getenv("ADDON_MAX_ENTRIES_PER_POSITION", "5"))


# --- AI config ---
OPENAI_API_KEY = str(os.getenv("OPENAI_API_KEY", "") or "").strip()
MODEL = str(os.getenv("OPENAI_MODEL", os.getenv("MODEL", "gpt-4o-mini")) or "gpt-4o-mini").strip()
OPENAI_MODEL = MODEL

API_TIMEOUT_SEC = float(os.getenv("API_TIMEOUT_SEC", "20"))
API_RETRY_COUNT = int(os.getenv("API_RETRY_COUNT", "3"))
API_RETRY_WAIT_SEC = float(os.getenv("API_RETRY_WAIT_SEC", "1.5"))

AI_ENTRY_DEFAULT_SCORE = int(os.getenv("AI_ENTRY_DEFAULT_SCORE", "50"))
AI_ENTRY_DEFAULT_LOT_MULTIPLIER = float(os.getenv("AI_ENTRY_DEFAULT_LOT_MULTIPLIER", "1.0"))
AI_ENTRY_MIN_SCORE = int(os.getenv("AI_ENTRY_MIN_SCORE", "70"))
AI_ENTRY_THROTTLE_SEC = float(os.getenv("AI_ENTRY_THROTTLE_SEC", "15"))
ADDON_MIN_AI_SCORE = int(os.getenv("ADDON_MIN_AI_SCORE", str(AI_ENTRY_MIN_SCORE)))

MIN_OTHER_SIGNALS_FOR_ENTRY = int(os.getenv("MIN_OTHER_SIGNALS_FOR_ENTRY", "1"))

CONFLUENCE_WINDOW_SEC = int(os.getenv("CONFLUENCE_WINDOW_SEC", "300"))
CONFLUENCE_DEBUG = _env_bool("CONFLUENCE_DEBUG", "0")
CONFLUENCE_DEBUG_MAX_LINES = int(os.getenv("CONFLUENCE_DEBUG_MAX_LINES", "80"))

AI_CLOSE_ENABLED = _env_bool("AI_CLOSE_ENABLED", "1")
AI_CLOSE_THROTTLE_SEC = float(os.getenv("AI_CLOSE_THROTTLE_SEC", "20"))
AI_CLOSE_MIN_CONFIDENCE = int(os.getenv("AI_CLOSE_MIN_CONFIDENCE", "55"))
AI_CLOSE_FALLBACK = str(os.getenv("AI_CLOSE_FALLBACK", "hold") or "hold").strip().lower()
AI_CLOSE_DEFAULT_CONFIDENCE = int(os.getenv("AI_CLOSE_DEFAULT_CONFIDENCE", "60"))


# --- Local entry safety guards (professional defaults) ---
# Gold day-trading (M5/M15) tends to suffer when spread is wide or ATR is too small.
ENTRY_MAX_SPREAD_POINTS = float(os.getenv("ENTRY_MAX_SPREAD_POINTS", "90"))
ENTRY_MIN_ATR_TO_SPREAD = float(os.getenv("ENTRY_MIN_ATR_TO_SPREAD", "3.0"))
ENTRY_COOLDOWN_SEC = float(os.getenv("ENTRY_COOLDOWN_SEC", "25"))


# --- Weekend discretionary close ---
WEEKEND_CLOSE_ENABLED = _env_bool("WEEKEND_CLOSE_ENABLED", "0")
WEEKEND_CLOSE_TZ = str(os.getenv("WEEKEND_CLOSE_TZ", "utc") or "utc").strip().lower()
WEEKEND_CLOSE_WEEKDAY = int(os.getenv("WEEKEND_CLOSE_WEEKDAY", "4"))
WEEKEND_CLOSE_HOUR = int(os.getenv("WEEKEND_CLOSE_HOUR", "21"))
WEEKEND_CLOSE_MINUTE = int(os.getenv("WEEKEND_CLOSE_MINUTE", "55"))
WEEKEND_CLOSE_WINDOW_MIN = int(os.getenv("WEEKEND_CLOSE_WINDOW_MIN", "5"))
WEEKEND_CLOSE_POLL_SEC = float(os.getenv("WEEKEND_CLOSE_POLL_SEC", "30"))


# --- Init external clients ---
client = None
context = None
zmq_socket = None
_mt5_ready = False

_runtime_lock = Lock()
_runtime_initialized = False
_runtime_init_error: Optional[str] = None

signals_lock = Lock()
signals_cache: List[Dict[str, Any]] = []

_cache_dirty = False
_cache_last_save_at = 0.0
_cache_last_dirty_at = 0.0
_cache_flush_thread_started = False

_qtrend_lock = Lock()
_qtrend_state_by_symbol: Dict[str, Dict[str, Any]] = {}

_last_atr_by_symbol: Dict[str, float] = {}

_addon_lock = Lock()
_addon_state_by_symbol: Dict[str, Dict[str, Any]] = {}

_entry_lock = Lock()
_last_order_sent_at_by_symbol: Dict[str, float] = {}

# fxChartAI v2.6思想寄せ：


def _validate_config() -> List[str]:
    issues: List[str] = []

    if not ZMQ_BIND:
        issues.append("ZMQ_BIND is empty")
    if not WEBHOOK_PORT or int(WEBHOOK_PORT) <= 0:
        issues.append("WEBHOOK_PORT is invalid")

    if ZMQ_HEARTBEAT_ENABLED and (not ZMQ_HEARTBEAT_BIND):
        issues.append("ZMQ_HEARTBEAT_ENABLED but ZMQ_HEARTBEAT_BIND is empty")

    if not SYMBOL:
        issues.append("SYMBOL is empty")

    if not OPENAI_API_KEY:
        issues.append("OPENAI_API_KEY missing (AI features will be disabled)")

    return issues


def init_runtime() -> bool:
    """Initialize external dependencies.

    IMPORTANT: This is intentionally NOT executed at import-time.
    It is safe to call multiple times.
    """
    global client, context, zmq_socket, _mt5_ready, _runtime_initialized, _runtime_init_error, _cache_flush_thread_started

    with _runtime_lock:
        if _runtime_initialized:
            return True

        issues = _validate_config()
        for msg in issues:
            print(f"[FXAI][WARN] {msg}")

        # OpenAI
        try:
            client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
        except Exception as e:
            client = None
            print(f"[FXAI][WARN] OpenAI init failed: {e}")

        # ZMQ
        try:
            context = zmq.Context()
            zmq_socket = context.socket(zmq.PUSH)
            zmq_socket.bind(ZMQ_BIND)
        except Exception as e:
            _runtime_init_error = f"ZMQ init failed: {e}"
            print(f"[FXAI][FATAL] {_runtime_init_error}")
            return False

        # MT5
        try:
            _mt5_ready = bool(mt5.initialize())
        except Exception as e:
            _mt5_ready = False
            _runtime_init_error = f"MT5 init exception: {e}"
            print(f"[FXAI][FATAL] {_runtime_init_error}")
            return False

        if not _mt5_ready:
            _runtime_init_error = "MT5 initialization failed"
            print(f"[FXAI][FATAL] {_runtime_init_error}")
            return False

        # Restore cache
        try:
            _load_cache()
        except Exception as e:
            print(f"[FXAI][WARN] Cache load failed: {e}")

        # Start background loops
        if ZMQ_HEARTBEAT_ENABLED:
            Thread(target=_heartbeat_receiver_loop, daemon=True).start()
        if WEEKEND_CLOSE_ENABLED:
            Thread(target=_weekend_close_loop, daemon=True).start()
        if CACHE_ASYNC_FLUSH_ENABLED and (not _cache_flush_thread_started):
            Thread(target=_cache_flush_loop, daemon=True).start()
            _cache_flush_thread_started = True

        _runtime_initialized = True
        _runtime_init_error = None
        return True


def ensure_runtime_initialized() -> bool:
    """Ensure external dependencies are initialized (for WSGI / first request)."""
    ok = init_runtime()
    if not ok:
        _set_status(last_result="Init failed", last_result_at=time.time(), last_init_error=_runtime_init_error)
    return ok


def _extract_symbol_from_webhook(data: Dict[str, Any]) -> str:
    """Extract symbol from a TradingView webhook payload.

    Accepts multiple common field names and normalizes like "OANDA:XAUUSD" -> "XAUUSD".
    """
    if not isinstance(data, dict):
        return (SYMBOL or "").strip().upper()

    raw = (
        data.get("symbol")
        or data.get("ticker")
        or data.get("instrument")
        or data.get("market")
        or data.get("pair")
        or ""
    )

    if raw is None:
        raw = ""
    raw = str(raw).strip()
    if ":" in raw:
        raw = raw.split(":")[-1].strip()
    return raw.upper() if raw else (SYMBOL or "").strip().upper()


def _ensure_mt5_symbol_selected(symbol: str) -> tuple[str, bool]:
    """Try selecting the requested symbol; fallback to default SYMBOL when unavailable."""
    sym = (symbol or "").strip().upper()
    if not sym:
        sym = SYMBOL

    ok = False
    try:
        ok = bool(mt5.symbol_select(sym, True))
    except Exception:
        ok = False

    if ok:
        return sym, True

    # fallback
    fallback = (SYMBOL or "").strip().upper()
    if not fallback:
        return sym, False
    try:
        fb_ok = bool(mt5.symbol_select(fallback, True))
    except Exception:
        fb_ok = False
    return fallback, fb_ok

# --- Runtime status (for debugging / health checks) ---
_status_lock = Lock()
_last_status: Dict[str, Any] = {
    "started_at": time.time(),
    "last_webhook_at": None,
    "last_webhook_symbol": None,
    "last_webhook_source": None,
    "last_webhook_side": None,
    "last_result": None,  # e.g. Stored / Waiting confluence / Blocked by AI / OK
    "last_result_at": None,
    "last_order": None,
    "last_mgmt_action": None,
    "last_mgmt_confidence": None,
    "last_mgmt_reason": None,
    "last_mgmt_at": None,
    "last_mgmt_throttled": None,

    # Heartbeat (EA -> Python)
    "heartbeat_enabled": bool(ZMQ_HEARTBEAT_ENABLED),
    "heartbeat_timeout_sec": float(ZMQ_HEARTBEAT_TIMEOUT_SEC),
    "last_heartbeat_at": None,
    "last_heartbeat_payload": None,
}


def _set_status(**kwargs) -> None:
    with _status_lock:
        _last_status.update(kwargs)


def _get_status_snapshot() -> Dict[str, Any]:
    with _status_lock:
        snap = dict(_last_status)
    # add lightweight cache stats without holding status lock
    with signals_lock:
        snap["signals_cache_len"] = len(signals_cache)

    if bool(snap.get("heartbeat_enabled")):
        last_hb = snap.get("last_heartbeat_at")
        now = time.time()
        age = None
        fresh = False
        if isinstance(last_hb, (int, float)) and float(last_hb) > 0:
            age = float(now - float(last_hb))
            fresh = age <= float(snap.get("heartbeat_timeout_sec") or ZMQ_HEARTBEAT_TIMEOUT_SEC)
        snap["heartbeat_age_sec"] = age
        snap["heartbeat_fresh"] = fresh
    return snap


def _check_port_bindable(host: str, port: int) -> Optional[str]:
    """Return error message if the port cannot be bound, else None."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        return None
    except OSError as e:
        return str(e)
    finally:
        try:
            s.close()
        except Exception:
            pass


def _heartbeat_is_fresh(now_ts: Optional[float] = None) -> bool:
    """Return True if EA heartbeat is fresh (or heartbeat is disabled)."""
    if not ZMQ_HEARTBEAT_ENABLED:
        return True
    if now_ts is None:
        now_ts = time.time()
    with _status_lock:
        last_hb = _last_status.get("last_heartbeat_at")
    if not isinstance(last_hb, (int, float)):
        return False
    if float(last_hb) <= 0:
        return False
    return (float(now_ts) - float(last_hb)) <= float(ZMQ_HEARTBEAT_TIMEOUT_SEC)


def _summarize_heartbeat_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        keep = {}
        for k in (
            "type",
            "ts",
            "trade_server_ts",
            "gmt_ts",
            "server_gmt_offset_sec",
            "symbol",
            "account",
            "login",
            "server",
            "ok",
            "equity",
            "balance",
            "positions",
            "net_side",
            "halt",
            "magic",
        ):
            if k in payload:
                keep[k] = payload.get(k)
        if keep:
            return keep
        return {"keys": list(payload.keys())[:20]}
    if isinstance(payload, str):
        return payload[:200]
    return payload


_weekend_close_last_sent_week_by_symbol: Dict[str, str] = {}


def _now_for_weekend_close() -> datetime:
    if WEEKEND_CLOSE_TZ == "broker":
        with _status_lock:
            payload = _last_status.get("last_heartbeat_payload")
        if isinstance(payload, dict):
            try:
                gmt_ts = payload.get("gmt_ts")
                offset = payload.get("server_gmt_offset_sec")
                if gmt_ts is not None and offset is not None:
                    # Convert UTC epoch to broker's server wall-clock by applying the offset.
                    base = float(gmt_ts)
                    off = float(offset)
                    if base > 0:
                        return datetime.fromtimestamp(base + off, tz=timezone.utc)

                ts = payload.get("trade_server_ts") or payload.get("ts")
                ts_f = float(ts)
                if ts_f > 0:
                    # Fallback: treat the provided epoch as already in broker wall-clock.
                    return datetime.fromtimestamp(ts_f, tz=timezone.utc)
            except Exception:
                pass
        # Fallback: local timezone
        return datetime.now().astimezone()
    if WEEKEND_CLOSE_TZ == "utc":
        return datetime.now(timezone.utc)
    return datetime.now().astimezone()


def _week_key(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _is_within_weekend_close_window(dt: datetime) -> bool:
    if not WEEKEND_CLOSE_ENABLED:
        return False
    if int(dt.weekday()) != int(WEEKEND_CLOSE_WEEKDAY):
        return False
    close_dt = dt.replace(hour=int(WEEKEND_CLOSE_HOUR), minute=int(WEEKEND_CLOSE_MINUTE), second=0, microsecond=0)
    window_sec = max(0, int(WEEKEND_CLOSE_WINDOW_MIN)) * 60
    start_dt = close_dt - timedelta(seconds=window_sec)
    return start_dt <= dt <= close_dt


def _weekend_close_loop() -> None:
    """Optionally closes positions shortly before weekend (best-effort)."""
    if not WEEKEND_CLOSE_ENABLED:
        return

    while True:
        try:
            dt = _now_for_weekend_close()
            if not _is_within_weekend_close_window(dt):
                time.sleep(max(1.0, float(WEEKEND_CLOSE_POLL_SEC)))
                continue

            wk = _week_key(dt)
            sym = (SYMBOL or "").strip().upper() or "GOLD"
            last = _weekend_close_last_sent_week_by_symbol.get(sym)
            if last == wk:
                time.sleep(max(1.0, float(WEEKEND_CLOSE_POLL_SEC)))
                continue

            # Need fresh heartbeat so EA can actually receive CLOSE.
            if not _heartbeat_is_fresh(now_ts=time.time()):
                _set_status(last_result="Weekend close pending (heartbeat stale)", last_result_at=time.time())
                time.sleep(max(1.0, float(WEEKEND_CLOSE_POLL_SEC)))
                continue

            pos_summary = get_mt5_positions_summary(sym)
            if int((pos_summary or {}).get("positions_open") or 0) <= 0:
                _weekend_close_last_sent_week_by_symbol[sym] = wk
                _set_status(last_result="Weekend close skipped (no positions)", last_result_at=time.time())
                time.sleep(max(1.0, float(WEEKEND_CLOSE_POLL_SEC)))
                continue

            zmq_socket.send_json({"type": "CLOSE", "reason": "weekend_discretionary_close"})
            _weekend_close_last_sent_week_by_symbol[sym] = wk
            _set_status(
                last_result="Weekend CLOSE sent",
                last_result_at=time.time(),
                last_mgmt_action="CLOSE",
                last_mgmt_confidence=None,
                last_mgmt_reason="weekend_discretionary_close",
                last_mgmt_at=time.time(),
                last_mgmt_throttled=None,
            )
            time.sleep(max(1.0, float(WEEKEND_CLOSE_POLL_SEC)))
        except Exception as e:
            _set_status(last_result=f"Weekend close loop error: {e}", last_result_at=time.time())
            time.sleep(max(1.0, float(WEEKEND_CLOSE_POLL_SEC)))


def _heartbeat_receiver_loop() -> None:
    """Blocking loop that receives heartbeat messages from EA via ZMQ."""
    if not ZMQ_HEARTBEAT_ENABLED:
        return

    hb = context.socket(zmq.PULL)
    hb.bind(ZMQ_HEARTBEAT_BIND)
    hb.setsockopt(zmq.RCVTIMEO, 1000)  # 1s

    while True:
        try:
            msg = hb.recv_string()
        except zmq.error.Again:
            continue
        except Exception as e:
            _set_status(last_result=f"Heartbeat recv error: {e}", last_result_at=time.time())
            continue

        try:
            payload = json.loads(msg)
        except Exception:
            payload = {"raw": msg}

        _set_status(
            last_heartbeat_at=time.time(),
            last_heartbeat_payload=_summarize_heartbeat_payload(payload),
        )

# --- De-dupe / throttle ---
_last_ai_attempt_key = None
_last_ai_attempt_at = 0.0

_last_close_attempt_key = None
_last_close_attempt_at = 0.0



def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _stable_round_time(t: Optional[float], resolution_sec: float = 1.0) -> Optional[float]:
    try:
        if t is None:
            return None
        tt = float(t)
        if resolution_sec <= 0:
            return tt
        return round(tt / float(resolution_sec)) * float(resolution_sec)
    except Exception:
        return None


def _signal_dedupe_key(s: Dict[str, Any]) -> str:
    """Create a stable de-duplication key from a (preferably normalized) signal."""
    symbol = (s.get("symbol") or "").strip().upper()
    source = (s.get("source") or "").strip()
    event = (s.get("event") or "").strip().lower()
    sig_type = (s.get("signal_type") or "").strip().lower()
    confirmed = (s.get("confirmed") or "").strip().lower()
    side = (s.get("side") or "").strip().lower()

    # Prefer signal_time when present, fallback to receive_time.
    t = s.get("signal_time")
    if t is None:
        t = s.get("receive_time")
    t_rounded = _stable_round_time(t, 1.0)
    if t_rounded is None:
        t_rounded = 0.0

    # Key includes fields that define the economic meaning of an alert.
    return f"{symbol}|{source}|{event}|{sig_type}|{confirmed}|{side}|{t_rounded:.0f}"


def _append_signal_dedup_locked(signal: Dict[str, Any], dedupe_window_sec: float = 120.0) -> bool:
    """Append a signal into cache with de-duplication.

    Returns True if appended, False if treated as a duplicate.
    """
    if not isinstance(signal, dict):
        return False

    now = time.time()
    key = _signal_dedupe_key(signal)

    # Fast scan from the end (most recent first).
    for prev in reversed(signals_cache):
        try:
            prev_key = _signal_dedupe_key(prev)
            if prev_key != key:
                continue
            # If same key seen recently, treat as duplicate.
            prt = float(prev.get("receive_time") or 0.0)
            if prt > 0 and (now - prt) <= float(dedupe_window_sec or 0.0):
                return False
            # Same key regardless of age is also a duplicate in practice.
            return False
        except Exception:
            continue

    signals_cache.append(signal)
    return True


def _parse_signal_time_to_epoch(value):
    """TradingView timenow / ISO8601 / epoch を epoch(sec) に変換。失敗時 None。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        # ms epoch
        if v > 1e12:
            return v / 1000.0
        return v
    if not isinstance(value, str):
        return None

    s = value.strip()
    if not s:
        return None

    try:
        iso = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _normalize_signal_fields(signal: dict) -> dict:
    """fxChartAI側の NormalizeSourceName / Q-Trend variants を Python 側で寄せる。"""
    s = dict(signal)

    src = (s.get("source") or "").strip()
    strength = (s.get("strength") or "").strip().lower()

    src_lower = src.lower().replace(" ", "").replace("_", "")
    # Q-Trend: accept both legacy names and the new spec strings.
    # New spec:
    #   - source: "Q-Trend" (Normal)
    #   - source: "Q-Trend Strong" (Strong)
    # Legacy internal:
    #   - "Q-Trend-Normal" / "Q-Trend-Strong"
    if src_lower in {
        "q-trend",
        "qtrend",
        "qtrendnormal",
        "q-trendnormal",
        "qtrend-normal",
        "q-trend-normal",
        "qtrendstrong",
        "q-trendstrong",
        "qtrend-strong",
        "q-trend-strong",
        "qtrendstrongbuy",
        "qtrendstrongsell",
    }:
        is_strong = ("strong" in src_lower) or (strength == "strong")
        # Keep the new source strings as canonical going forward.
        s["source"] = "Q-Trend Strong" if is_strong else "Q-Trend"
        s["strength"] = "strong" if is_strong else "normal"
    elif src_lower in {"qtrendstrong", "qtrend strong".replace(" ", ""), "q-trendstrong"}:
        s["source"] = "Q-Trend Strong"
        s["strength"] = "strong"
    elif src_lower in {"qtrend", "q-trend"}:
        s["source"] = "Q-Trend"
        if not s.get("strength"):
            s["strength"] = "normal"
    elif src_lower in {"qtrend-normal", "q-trend-normal", "qtrendnormal"}:
        s["source"] = "Q-Trend"
        s["strength"] = "normal"
    elif src_lower in {"qtrend-strong", "q-trend-strong", "qtrendstrong"}:
        s["source"] = "Q-Trend Strong"
        s["strength"] = "strong"
    else:
        # Normalize common sources from your alert templates
        if src_lower in {"luxalgo_fvg", "luxalgofvg", "fvg"}:
            s["source"] = "FVG"
        elif src_lower in {"zonesdetector", "zones"}:
            s["source"] = "Zones"
        elif src_lower in {"osgfc"}:
            s["source"] = "OSGFC"
        else:
            s["source"] = src

    if isinstance(s.get("side"), str):
        side_norm = s["side"].strip().lower()
        s["side"] = side_norm if side_norm else ""
    if isinstance(s.get("strength"), str):
        s["strength"] = s["strength"].strip().lower()

    if isinstance(s.get("signal_type"), str):
        s["signal_type"] = s["signal_type"].strip().lower()
    if isinstance(s.get("event"), str):
        s["event"] = s["event"].strip().lower()
    if isinstance(s.get("confirmed"), str):
        s["confirmed"] = s["confirmed"].strip().lower()

    parsed = _parse_signal_time_to_epoch(s.get("time"))
    if parsed is not None:
        s["signal_time"] = parsed
    else:
        s["signal_time"] = s.get("receive_time")

    return s


def _is_qtrend_source(source: str) -> bool:
    src = (source or "").strip().lower().replace("_", "")
    if not src:
        return False
    # Accept both new spec strings and older internal ones.
    return src in {
        "q-trend",
        "qtrend",
        "q-trendstrong",
        "qtrendstrong",
        "q-trend-normal",
        "qtrend-normal",
        "q-trend-strong",
        "qtrend-strong",
        "q-trendnormal",
        "qtrendnormal",
        "qtrendnormalbuy",
        "qtrendnormalsell",
        "qtrendstrongbuy",
        "qtrendstrongsell",
        "q-trendstrongbuy",
        "q-trendstrongsell",
        # canonical outputs from _normalize_signal_fields
        "q-trendstrong".replace("-", ""),
        "qtrendstrong".replace("-", ""),
    } or ("qtrend" in src) or ("q-trend" in src)


def _update_qtrend_context_from_signal(normalized: Dict[str, Any]) -> None:
    """Update in-memory Q-Trend state.

    New spec: Q-Trend is context only; always stored here for the latest environment.
    Direction uses side (buy/sell) as UP/DOWN equivalent.
    """
    if not isinstance(normalized, dict):
        return
    symbol = (normalized.get("symbol") or "").strip().upper()
    if not symbol:
        return

    side = (normalized.get("side") or "").strip().lower()
    if side not in {"buy", "sell"}:
        return

    strength = (normalized.get("strength") or "normal").strip().lower()
    strength = "strong" if strength == "strong" else "normal"

    now = float(normalized.get("receive_time") or time.time())
    with _qtrend_lock:
        _qtrend_state_by_symbol[symbol] = {
            "side": side,
            "strength": strength,
            "updated_at": now,
            "tf": normalized.get("tf"),
            "price": normalized.get("price"),
            "confirmed": normalized.get("confirmed"),
            "event": normalized.get("event"),
            "source": normalized.get("source"),
        }


def _get_qtrend_context(symbol: str, now: Optional[float] = None) -> Optional[Dict[str, Any]]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    if now is None:
        now = time.time()
    with _qtrend_lock:
        st = _qtrend_state_by_symbol.get(sym)
    if not st:
        return None
    updated_at = float(st.get("updated_at") or 0.0)
    if updated_at <= 0:
        return None

    # Reuse the existing max age config (originally used for Q-Trend trigger freshness).
    max_age = float(Q_TREND_MAX_AGE_SEC or 300)
    if max_age > 0 and (float(now) - updated_at) > max_age:
        return None
    return dict(st)


def _dir_from_side(side: str) -> str:
    s = (side or "").strip().lower()
    if s == "buy":
        return "UP"
    if s == "sell":
        return "DOWN"
    return "UNKNOWN"


def _collect_recent_context_signals(symbol: str, now: float) -> Dict[str, Any]:
    """Collect recent Zones/FVG context for AI.

    Keeps the payload compact: mainly latest touches and recent confirmed zones.
    """
    normalized = _filter_fresh_signals(symbol, now)
    zones_confirmed_recent = 0
    latest_zone_touch = None
    latest_fvg_touch = None

    # We also keep a small list of recent context events for transparency/debugging.
    recent_events = []
    for s in sorted(normalized, key=lambda x: float(x.get("signal_time") or x.get("receive_time") or 0.0), reverse=True):
        src = s.get("source")
        evt = s.get("event")
        side = s.get("side")
        st = float(s.get("signal_time") or s.get("receive_time") or 0.0)

        if src == "Zones" and (s.get("signal_type") == "structure") and (evt == "new_zone_confirmed"):
            rt = float(s.get("receive_time") or 0.0)
            if rt > 0 and (now - rt) <= float(ZONE_LOOKBACK_SEC):
                zones_confirmed_recent += 1

        if src == "Zones" and evt in {"zone_retrace_touch", "zone_touch"} and side in {"buy", "sell"}:
            if latest_zone_touch is None or st > float(latest_zone_touch.get("signal_time") or 0.0):
                latest_zone_touch = {
                    "side": side,
                    "event": evt,
                    "signal_time": st,
                    "confirmed": s.get("confirmed"),
                    "strength": s.get("strength"),
                }

        if src == "FVG" and evt == "fvg_touch" and side in {"buy", "sell"}:
            if latest_fvg_touch is None or st > float(latest_fvg_touch.get("signal_time") or 0.0):
                latest_fvg_touch = {
                    "side": side,
                    "event": evt,
                    "signal_time": st,
                    "confirmed": s.get("confirmed"),
                    "strength": s.get("strength"),
                }

        if len(recent_events) < 12:
            if src in {"Zones", "FVG"}:
                recent_events.append(
                    {
                        "source": src,
                        "event": evt,
                        "side": side,
                        "signal_time": st,
                        "confirmed": s.get("confirmed"),
                        "strength": s.get("strength"),
                        "signal_type": s.get("signal_type"),
                    }
                )

    return {
        "zones_confirmed_recent": int(zones_confirmed_recent),
        "latest_zone_touch": latest_zone_touch,
        "latest_fvg_touch": latest_fvg_touch,
        "recent_events": list(recent_events),
    }


def _collect_window_signals_around_trigger(
    symbol: str,
    center_ts: float,
    trigger_side: str,
    window_sec: float = 300.0,
) -> Dict[str, Any]:
    """Collect signals within ±window_sec of the trigger time.

    User requirement: When Lorentzian fires, include cached signals in the same
    direction within a ±5min window (plus other window signals for transparency).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return {"center_ts": float(center_ts or 0.0), "window_sec": float(window_sec or 0.0), "aligned": [], "opposed": [], "neutral": []}

    try:
        center = float(center_ts)
    except Exception:
        center = time.time()

    try:
        w = float(window_sec)
    except Exception:
        w = 300.0
    if w <= 0:
        w = 300.0

    trig_side = (trigger_side or "").strip().lower()

    with signals_lock:
        snapshot = [dict(s) for s in signals_cache if isinstance(s, dict)]

    def _sig_ts(s: Dict[str, Any]) -> float:
        try:
            return float(s.get("signal_time") or s.get("receive_time") or 0.0)
        except Exception:
            return 0.0

    window_raw: List[Dict[str, Any]] = []
    for s in snapshot:
        if (s.get("symbol") or "").strip().upper() != sym:
            continue
        st = _sig_ts(s)
        if st <= 0:
            continue
        if abs(st - center) > w:
            continue
        window_raw.append(s)

    window_raw.sort(key=_sig_ts)

    # Strict allow-listing to keep the window payload high-signal.
    # User requirement: include Q-Trend (context) in the ±5min window.
    allowed_sources = {"Q-Trend", "Q-Trend Strong", "Zones", "FVG"}
    allowed_events_by_source = {
        # Q-Trend context may not have a strong event taxonomy; allow any.
        "Q-Trend": None,
        "Q-Trend Strong": None,
        # Zones: keep only key touch/structure events.
        "Zones": {
            "zone_retrace_touch",
            "zone_touch",
            "new_zone_confirmed",
            "zone_confirmed",
        },
        # FVG: keep touch only.
        "FVG": {"fvg_touch"},
    }

    # De-duplicate by (source,event,side) keeping the latest by time.
    best_by_key: Dict[tuple, Dict[str, Any]] = {}

    for s in window_raw:
        src = (s.get("source") or "")
        evt = (s.get("event") or "")
        side = (s.get("side") or "").strip().lower()
        st = _sig_ts(s)

        if not src:
            continue

        # Normalize Q-Trend variants that might slip in.
        if _is_qtrend_source(src):
            src = "Q-Trend Strong" if (str(s.get("strength") or "").lower() == "strong" or "strong" in str(src).lower()) else "Q-Trend"

        if src not in allowed_sources:
            continue

        allowed_events = allowed_events_by_source.get(src)
        if isinstance(allowed_events, set):
            if (evt or "").strip().lower() not in allowed_events:
                continue

        # Avoid duplicating the Lorentzian trigger itself in the window list.
        if src == "Lorentzian" and side == trig_side and abs(st - center) <= 1.0:
            continue

        compact = {
            "source": src,
            "event": evt,
            "side": side or None,
            "signal_type": s.get("signal_type"),
            "strength": s.get("strength"),
            "confirmed": s.get("confirmed"),
            "signal_time": st,
        }

        key = (compact.get("source"), compact.get("event"), compact.get("side"))
        prev = best_by_key.get(key)
        if (prev is None) or (float(compact.get("signal_time") or 0.0) >= float(prev.get("signal_time") or 0.0)):
            best_by_key[key] = compact

    # Split into aligned/opposed/neutral after de-dup.
    aligned: List[Dict[str, Any]] = []
    opposed: List[Dict[str, Any]] = []
    neutral: List[Dict[str, Any]] = []

    for compact in sorted(best_by_key.values(), key=lambda x: float(x.get("signal_time") or 0.0)):
        side = (compact.get("side") or "")
        if side in {"buy", "sell"} and trig_side in {"buy", "sell"}:
            (aligned if side == trig_side else opposed).append(compact)
        else:
            neutral.append(compact)

    # Hard cap payload sizes to keep prompts stable.
    aligned = aligned[-30:]
    opposed = opposed[-30:]
    neutral = neutral[-20:]

    return {
        "center_ts": float(center),
        "window_sec": float(w),
        "trigger_side": trig_side,
        "aligned": aligned,
        "opposed": opposed,
        "neutral": neutral,
        "counts": {"aligned": len(aligned), "opposed": len(opposed), "neutral": len(neutral)},
    }


def _weight_confirmed(confirmed: str) -> float:
    """bar_close の方が信頼度が高い想定。"""
    c = (confirmed or "").lower()
    if c == "bar_close":
        return 1.0
    if c == "intrabar":
        return 0.6
    return 0.8


def _load_cache():
    """起動時にファイルからシグナル履歴を復元する"""
    if not os.path.exists(CACHE_FILE):
        return
    try:
        with open(CACHE_FILE, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                recovered = 0
                with signals_lock:
                    for raw in data:
                        if not isinstance(raw, dict):
                            continue
                        # Ensure minimal fields
                        if not raw.get("receive_time"):
                            raw = dict(raw)
                            raw["receive_time"] = time.time()
                        if raw.get("symbol"):
                            raw = dict(raw)
                            raw["symbol"] = str(raw.get("symbol") or "").strip().upper()
                        normalized = _normalize_signal_fields(raw)
                        if _append_signal_dedup_locked(normalized):
                            recovered += 1

                    # prune immediately on boot to avoid stale context
                    _prune_signals_cache_locked(time.time())

                print(f"[FXAI] Cache loaded: {recovered} signals recovered.")
    except Exception as e:
        print(f"[FXAI][WARN] Failed to load cache: {e}")

    # After loading, treat cache as clean.
    global _cache_dirty, _cache_last_save_at, _cache_last_dirty_at
    _cache_dirty = False
    _cache_last_save_at = time.time()
    _cache_last_dirty_at = 0.0


def _mark_cache_dirty_locked(now: Optional[float] = None) -> None:
    """Mark cache as dirty (must be called with signals_lock held)."""
    global _cache_dirty, _cache_last_dirty_at
    if now is None:
        now = time.time()
    _cache_dirty = True
    _cache_last_dirty_at = float(now)


def _cache_flush_loop() -> None:
    """Background loop to flush cache to disk at a controlled interval."""
    global _cache_last_save_at, _cache_dirty
    # small sleep to avoid hot loop; ensure >= 0.5s
    sleep_sec = max(0.5, float(CACHE_FLUSH_INTERVAL_SEC or 2.0))
    while True:
        time.sleep(sleep_sec)
        if not CACHE_ASYNC_FLUSH_ENABLED:
            continue

        try:
            now = time.time()
            with signals_lock:
                if not _cache_dirty:
                    continue

                last_save = float(_cache_last_save_at or 0.0)
                last_dirty = float(_cache_last_dirty_at or 0.0)

                # Flush if we've been dirty long enough OR if a force window passed.
                should_flush = False
                if last_dirty > 0 and (now - last_dirty) >= float(CACHE_FLUSH_INTERVAL_SEC or 2.0):
                    should_flush = True
                if (now - last_save) >= float(CACHE_FLUSH_FORCE_SEC or 10.0):
                    should_flush = True

                if not should_flush:
                    continue

                _save_cache_locked()
                _cache_last_save_at = now
                _cache_dirty = False
        except Exception as e:
            print(f"[FXAI][WARN] Cache flush loop error: {e}")

def _save_cache_locked():
    """シグナルキャッシュをファイルに保存する (Lock保持中に呼ぶこと)"""
    try:
        # Atomic-ish write: write temp then replace.
        tmp = f"{CACHE_FILE}.tmp"
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(signals_cache, f, ensure_ascii=False)
        os.replace(tmp, CACHE_FILE)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to save cache: {e}")

def _prune_signals_cache_locked(now: float) -> None:
    """期限切れシグナルを削除。

    Zone情報は、
    - 「存在/構造（例: new_zone_confirmed）」は長く保持（デフォルト24h）
    - 「接触/touch（例: zone_retrace_touch）」は短く保持（デフォルト20m）
    に分離し、古いtouchを合流として誤認するバグを防ぐ。
    """
    keep_list = []

    for s in signals_cache:
        src_raw = (s.get("source") or "").strip().lower()
        src = src_raw
        event = (s.get("event") or "").strip().lower()
        sig_type = (s.get("signal_type") or "").strip().lower()

        src_norm = src_raw.replace(" ", "").replace("_", "")
        is_zones = (src_norm in {"zones", "zonesdetector"}) or ("zone" in src_norm)

        # Zoneの「構造/存在」イベント（長期保持）
        zone_presence_events = {
            "new_zone_confirmed",
            "zone_confirmed",
            "new_zone",
            "zone_created",
            "zone_breakout",
        }

        # Zoneの「接触」イベント（短期保持）
        zone_touch_markers = {
            "zone_retrace_touch",
            "zone_touch",
            "touch",
            "retrace",
            "bounce",
        }

        rt = float(s.get("receive_time", now) or now)
        age = now - rt

        if is_zones:
            if (sig_type in {"structure", "zones", "zone"} and event in zone_presence_events) or event in zone_presence_events:
                limit_sec = ZONE_LOOKBACK_SEC
                if age < float(limit_sec or ZONE_LOOKBACK_SEC):
                    keep_list.append(s)
                continue
            if any(m in event for m in zone_touch_markers):
                limit_sec = ZONE_TOUCH_LOOKBACK_SEC
                if age < float(limit_sec or ZONE_TOUCH_LOOKBACK_SEC):
                    keep_list.append(s)
                continue

            # Unknown Zones-like events: keep short to avoid false confluence.
            limit_sec = SIGNAL_LOOKBACK_SEC
            if age < float(limit_sec or SIGNAL_LOOKBACK_SEC):
                keep_list.append(s)
            continue

        # Non-Zones: simple time-based retention (no Q-Trend anchoring).
        # We keep recent evidence so Lorentzian triggers can reference it.
        limit_sec = float(SIGNAL_LOOKBACK_SEC or 1200)
        if age < limit_sec:
            keep_list.append(s)

    if len(keep_list) != len(signals_cache):
        signals_cache[:] = keep_list


def _is_zone_presence_signal(s: dict) -> bool:
    src = (s.get("source") or "").strip().lower()
    event = (s.get("event") or "").strip().lower()
    sig_type = (s.get("signal_type") or "").strip().lower()

    src_norm = src.replace(" ", "").replace("_", "")
    is_zones = (src_norm in {"zones", "zonesdetector"}) or ("zone" in src_norm)
    if not is_zones:
        return False

    zone_presence_events = {
        "new_zone_confirmed",
        "zone_confirmed",
        "new_zone",
        "zone_created",
        "zone_breakout",
    }
    return (event in zone_presence_events) or (sig_type == "structure" and event in zone_presence_events)


def _is_zone_touch_signal(s: dict) -> bool:
    src = (s.get("source") or "").strip().lower()
    event = (s.get("event") or "").strip().lower()
    src_norm = src.replace(" ", "").replace("_", "")
    is_zones = (src_norm in {"zones", "zonesdetector"}) or ("zone" in src_norm)
    if not is_zones:
        return False
    # match the canonical touch events used elsewhere
    return event in {"zone_retrace_touch", "zone_touch"} or ("touch" in event)

def _filter_fresh_signals(symbol: str, now: float) -> list:
    """v2.6の SignalMaxAgeSec 相当：signal_time が古すぎるものを落とす。"""
    with signals_lock:
        normalized = [_normalize_signal_fields(s) for s in signals_cache if s.get("symbol") == symbol]

    fresh = []
    for s in normalized:
        # Zones presence signals are intentionally long-lived (structure context).
        # Use receive_time-based retention rather than signal_time freshness.
        if _is_zone_presence_signal(s):
            rt = float(s.get("receive_time") or 0.0)
            if rt > 0 and (now - rt) <= float(ZONE_LOOKBACK_SEC):
                fresh.append(s)
            continue

        # Zones touch is a momentary event; keep it short-lived.
        if _is_zone_touch_signal(s):
            rt = float(s.get("receive_time") or 0.0)
            if rt <= 0 or (now - rt) > float(ZONE_TOUCH_LOOKBACK_SEC):
                continue

        st = float(s.get("signal_time") or 0.0)
        if st <= 0:
            continue
        age = now - st
        if abs(age) > SIGNAL_MAX_AGE_SEC:
            # future/old both drop（v2.6 DebugReplayCsv の代替はここでは省略）
            continue
        fresh.append(s)

    return fresh


def get_qtrend_anchor_stats(target_symbol: str):
    """fxChartAI v2.6 の『Q-Trend起点で合流を集計』を Python 側で再現。"""
    now = time.time()
    normalized = _filter_fresh_signals(target_symbol, now)
    if not normalized:
        return None

    # Allowed evidence sources under the new spec.
    # NOTE: Q-Trend itself is an anchor, not a confluence evidence.
    # Primary: normalized names used by _normalize_signal_fields.
    # Compatibility: keep older/raw template names to avoid accidental drops.
    allowed_evidence_sources = {"Zones", "FVG", "OSGFC", "ZonesDetector", "LuxAlgo_FVG"}

    # Q-Trend: Normal/Strong が混在する場合は Strong を優先
    q_candidates = []
    for s in normalized:
        if s.get("source") in {"Q-Trend Strong", "Q-Trend", "Q-Trend-Strong", "Q-Trend-Normal"} and s.get("side") in {"buy", "sell"}:
            st = float(s.get("signal_time") or s.get("receive_time") or 0.0)
            is_strong = (s.get("source") in {"Q-Trend Strong", "Q-Trend-Strong"}) or (s.get("strength") == "strong")
            q_candidates.append((st, 1 if is_strong else 0, s))

    latest_q = None
    if q_candidates:
        # sort by time then strong priority
        q_candidates.sort(key=lambda x: (x[0], x[1]))
        latest_q = q_candidates[-1][2]
    if not latest_q:
        return None

    q_time = float(latest_q.get("signal_time") or latest_q.get("receive_time") or now)
    q_side = (latest_q.get("side") or "").lower()
    q_source = (latest_q.get("source") or "")
    q_is_strong = (q_source in {"Q-Trend Strong", "Q-Trend-Strong"}) or (latest_q.get("strength") == "strong")
    q_trigger_type = "Strong" if q_is_strong else "Normal"
    momentum_factor = 1.5 if q_is_strong else 1.0
    is_strong_momentum = bool(q_is_strong)

    confirm_unique_sources = set([q_source or "Q-Trend"])
    opp_unique_sources = set([q_source or "Q-Trend"])
    confirm_signals = 0
    opp_signals = 0
    strong_after_q = q_is_strong

    # 旧EA互換: 反対側のbar_close(=entry/structure)が来たらトリガー無効化
    cancel_due_to_opposite_bar_close = False
    cancel_detail = None

    # richer context for AI (trend filter / S-R / event quality)
    osgfc_latest_side = ""
    osgfc_latest_time = None
    fvg_same = 0
    fvg_opp = 0
    zones_touch_same = 0
    zones_touch_opp = 0
    zones_confirmed_after_q = 0
    zones_confirmed_recent = 0
    weighted_confirm_score = 0.0
    weighted_oppose_score = 0.0

    dbg_rows = [] if CONFLUENCE_DEBUG else None

    # Zones presence is meaningful even if it happened before Q-Trend.
    # Count recent zone confirmations (within ZONE_LOOKBACK_SEC by receive_time).
    for s in normalized:
        if (s.get("source") == "Zones") and (s.get("signal_type") == "structure") and (s.get("event") == "new_zone_confirmed"):
            rt = float(s.get("receive_time") or 0.0)
            if rt > 0 and (now - rt) <= float(ZONE_LOOKBACK_SEC):
                zones_confirmed_recent += 1

    window = max(0, int(CONFLUENCE_WINDOW_SEC or 300))
    for s in normalized:
        st = float(s.get("signal_time") or s.get("receive_time") or 0)
        # Source of Truth: Q-Trend ± 5 minutes
        if st < (q_time - window):
            if dbg_rows is not None:
                dbg_rows.append({
                    "st": st,
                    "src": s.get("source"),
                    "side": s.get("side"),
                    "evt": s.get("event"),
                    "conf": s.get("confirmed"),
                    "sig_type": s.get("signal_type"),
                    "counted": False,
                    "bucket": "SKIP",
                    "reason": "before_pre_window",
                })
            continue
        if st > (q_time + window):
            if dbg_rows is not None:
                dbg_rows.append({
                    "st": st,
                    "src": s.get("source"),
                    "side": s.get("side"),
                    "evt": s.get("event"),
                    "conf": s.get("confirmed"),
                    "sig_type": s.get("signal_type"),
                    "counted": False,
                    "bucket": "SKIP",
                    "reason": "after_post_window",
                })
            continue

        src = s.get("source")
        side = s.get("side")
        event = s.get("event")
        sig_type = s.get("signal_type")
        confirmed = s.get("confirmed")
        w = _weight_confirmed(confirmed)
        # touch系は「瞬間イベント」なので合流/逆行の重みを落とす（ノイズ過剰反応防止）
        event_weight = 1.0
        if event in {"fvg_touch", "zone_retrace_touch", "zone_touch"}:
            event_weight = 0.7
        if src in {"Q-Trend Strong", "Q-Trend", "Q-Trend-Strong", "Q-Trend-Normal"}:
            # 同一トレンド内で Strong が追加で来るケースを拾う（後追い含む）
            if src in {"Q-Trend Strong", "Q-Trend-Strong"}:
                strong_after_q = True
                is_strong_momentum = True
                q_trigger_type = "Strong"
                momentum_factor = 1.5
            continue

        # Allowlist: ignore unknown sources to avoid unintended confluence votes.
        if src not in allowed_evidence_sources:
            if dbg_rows is not None:
                dbg_rows.append({
                    "st": st,
                    "src": src,
                    "side": side,
                    "evt": event,
                    "conf": confirmed,
                    "sig_type": sig_type,
                    "strength": s.get("strength"),
                    "counted": False,
                    "bucket": "SKIP",
                    "reason": "source_not_allowed",
                })
            continue

        # 旧EA互換: 反対側のbar_closeが来たらキャンセル（レンジ往復ビンタ回避）
        if (
            (not cancel_due_to_opposite_bar_close)
            and st >= q_time
            and (confirmed or "").lower() == "bar_close"
            and side in {"buy", "sell"}
            and side != q_side
            and sig_type in {"entry_trigger", "structure"}
        ):
            cancel_due_to_opposite_bar_close = True
            cancel_detail = {
                "source": src,
                "side": side,
                "signal_type": sig_type,
                "event": event,
                "signal_time": st,
            }

        # OSGFC trend filter is useful even when used as a filter (strength of context)
        if src == "OSGFC" and side in {"buy", "sell"}:
            # keep latest within window
            if osgfc_latest_time is None or st >= float(osgfc_latest_time):
                osgfc_latest_time = st
                osgfc_latest_side = side

        # Zones: neutral confirmations (no side) indicate S/R reliability
        if src == "Zones" and sig_type == "structure" and event == "new_zone_confirmed":
            zones_confirmed_after_q += 1

        # Directional confluence buckets by source/event
        if src == "FVG" and event == "fvg_touch" and side in {"buy", "sell"}:
            if side == q_side:
                fvg_same += 1
            else:
                fvg_opp += 1

        if src == "Zones" and event == "zone_retrace_touch" and side in {"buy", "sell"}:
            if side == q_side:
                zones_touch_same += 1
            else:
                zones_touch_opp += 1

        # Ignore unnamed sources to avoid counting malformed alerts as confluence.
        if not src:
            if dbg_rows is not None:
                dbg_rows.append({
                    "st": st,
                    "src": src,
                    "side": side,
                    "evt": event,
                    "conf": confirmed,
                    "sig_type": sig_type,
                    "counted": False,
                    "bucket": "SKIP",
                    "reason": "missing_source",
                })
            continue

        # 合流カウント
        # - bar_close は常に合流OK
        # - intrabar は原則 strength=strong のみ
        #   ただし「Zones/FVGのtouch系」は normal でも合流として数える（直近タッチ→反発→Qトリガーの再現）
        # - trend_filter は原則除外。ただし OSGFC は「1票」として合流に含める
        conf_l = (confirmed or "").lower()
        strength_l = (s.get("strength") or "").lower()
        is_touch_event = (event in {"fvg_touch", "zone_retrace_touch", "zone_touch"})
        is_zone_or_fvg_touch = (src in {"Zones", "FVG"}) and is_touch_event
        intrabar_ok = (conf_l == "intrabar") and (strength_l == "strong" or is_zone_or_fvg_touch)
        confluence_ok = (conf_l == "bar_close") or intrabar_ok
        exclude_from_confluence_count = (sig_type == "trend_filter") and (src != "OSGFC")

        counted = False
        bucket = "SKIP"
        reason = ""

        if side == q_side:
            if confluence_ok and (not exclude_from_confluence_count):
                confirm_signals += 1
                confirm_unique_sources.add(src)
                counted = True
                bucket = "CONFIRM"
                reason = "counted"
            weighted_confirm_score += (w * event_weight)
            if s.get("strength") == "strong":
                strong_after_q = True
            if dbg_rows is not None and (not counted):
                if exclude_from_confluence_count:
                    reason = "excluded_trend_filter"
                elif not confluence_ok:
                    reason = "not_confluence_ok"
                else:
                    reason = "same_side_not_counted"
        elif side in {"buy", "sell"}:
            if confluence_ok and (not exclude_from_confluence_count):
                opp_signals += 1
                opp_unique_sources.add(src)
                counted = True
                bucket = "OPPOSE"
                reason = "counted"
            weighted_oppose_score += (w * event_weight)
            if dbg_rows is not None and (not counted):
                if exclude_from_confluence_count:
                    reason = "excluded_trend_filter"
                elif not confluence_ok:
                    reason = "not_confluence_ok"
                else:
                    reason = "opp_side_not_counted"
        else:
            if dbg_rows is not None:
                reason = "no_side"

        if dbg_rows is not None:
            dbg_rows.append({
                "st": st,
                "src": src,
                "side": side,
                "evt": event,
                "conf": confirmed,
                "sig_type": sig_type,
                "strength": s.get("strength"),
                "counted": bool(counted),
                "bucket": bucket,
                "reason": reason,
            })

    if dbg_rows is not None:
        try:
            dbg_rows_sorted = sorted(dbg_rows, key=lambda r: float(r.get("st") or 0.0))
        except Exception:
            dbg_rows_sorted = dbg_rows
        print(
            "[FXAI][CONFLUENCE][DBG] "
            + json.dumps(
                {
                    "symbol": target_symbol,
                    "q_time": q_time,
                    "q_side": q_side,
                    "window_sec": int(window),
                    "min_other_needed": int(MIN_OTHER_SIGNALS_FOR_ENTRY or 0),
                    "confirm_unique_sources": max(0, len(confirm_unique_sources) - 1),
                    "confirm_signals": confirm_signals,
                    "opp_unique_sources": max(0, len(opp_unique_sources) - 1),
                    "opp_signals": opp_signals,
                    "lines": dbg_rows_sorted[: max(1, int(CONFLUENCE_DEBUG_MAX_LINES or 80))],
                    "truncated": len(dbg_rows_sorted) > max(1, int(CONFLUENCE_DEBUG_MAX_LINES or 80)),
                },
                ensure_ascii=False,
            )
        )

    return {
        "q_time": q_time,
        "q_side": q_side,
        "q_source": q_source,
        "q_is_strong": q_is_strong,
        "q_trigger_type": q_trigger_type,
        "momentum_factor": momentum_factor,
        "is_strong_momentum": is_strong_momentum,
        "confirm_unique_sources": max(0, len(confirm_unique_sources) - 1),
        "confirm_signals": confirm_signals,
        "opp_unique_sources": max(0, len(opp_unique_sources) - 1),
        "opp_signals": opp_signals,
        "cancel_due_to_opposite_bar_close": cancel_due_to_opposite_bar_close,
        "cancel_detail": cancel_detail,
        "strong_after_q": strong_after_q,
        "osgfc_latest_side": osgfc_latest_side,
        "osgfc_latest_time": osgfc_latest_time,
        "fvg_touch_same": fvg_same,
        "fvg_touch_opp": fvg_opp,
        "zones_touch_same": zones_touch_same,
        "zones_touch_opp": zones_touch_opp,
        "zones_confirmed_after_q": zones_confirmed_after_q,
        "zones_confirmed_recent": zones_confirmed_recent,
        "weighted_confirm_score": round(weighted_confirm_score, 3),
        "weighted_oppose_score": round(weighted_oppose_score, 3),
    }


def get_mt5_market_data(symbol: str):
    """MT5のティック/レートが取れないケースでも落ちないように安全に取得する。"""
    tick = mt5.symbol_info_tick(symbol)

    def _rate_field(rate_row, key: str, default: float = 0.0) -> float:
        """MT5のrate行はdictの場合もnumpy.voidの場合もあるので両対応で取り出す。"""
        if rate_row is None:
            return default
        # dict-like
        if isinstance(rate_row, dict):
            try:
                return float(rate_row.get(key, default) or default)
            except Exception:
                return default
        # numpy.void / structured array row
        try:
            return float(rate_row[key])
        except Exception:
            pass
        # attribute fallback
        try:
            return float(getattr(rate_row, key, default) or default)
        except Exception:
            return default

    rates_m15 = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M15, 0, 20)
    if rates_m15 is not None and len(rates_m15) > 0:
        closes = [_rate_field(r, "close", 0.0) for r in rates_m15]
        ma15 = sum(closes) / max(1, len(closes))
    else:
        ma15 = 0.0

    # ATR: 旧EA(iATR)に寄せて True Range で近似
    rates_m5 = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M5, 0, 60)
    atr = 0.0
    if rates_m5 is not None and len(rates_m5) >= 2:
        period = 14
        highs = [_rate_field(r, "high", 0.0) for r in rates_m5]
        lows = [_rate_field(r, "low", 0.0) for r in rates_m5]
        closes = [_rate_field(r, "close", 0.0) for r in rates_m5]

        trs = []
        for i in range(1, len(rates_m5)):
            h = highs[i]
            l = lows[i]
            pc = closes[i - 1]
            tr = max(abs(h - l), abs(h - pc), abs(l - pc))
            if tr > 0:
                trs.append(tr)
        if trs:
            atr = sum(trs[:period]) / max(1, min(period, len(trs)))

    # If market data isn't available, reuse last known ATR for this symbol.
    if atr <= 0:
        atr = float(_last_atr_by_symbol.get(symbol, 0.0) or 0.0)

    info = mt5.symbol_info(symbol)
    point = float(getattr(info, "point", 0.0) or 0.0)

    bid = float(getattr(tick, "bid", 0.0) or 0.0)
    ask = float(getattr(tick, "ask", 0.0) or 0.0)
    spread = ((ask - bid) / point) if point > 0 else 0.0

    atr_points = (atr / point) if point > 0 else 0.0
    atr_to_spread = (atr_points / spread) if spread > 0 else None

    return {
        "bid": bid,
        "ask": ask,
        "m15_ma": ma15,
        "atr": atr,
        "point": point,
        "atr_points": atr_points,
        "atr_to_spread": atr_to_spread,
        "spread": spread,
    }


def get_mt5_position_state(symbol: str):
    try:
        positions = mt5.positions_get(symbol=symbol)
        if positions is None:
            return {"positions_open": 0}
        return {"positions_open": len(positions)}
    except Exception:
        return {"positions_open": 0}


def get_mt5_positions_summary(symbol: str) -> Dict[str, Any]:
    """MT5の保有ポジション概要（AI決済判断に必要な最小情報）を安全に返す。"""
    try:
        positions = mt5.positions_get(symbol=symbol)
    except Exception:
        positions = None

    if not positions:
        return {
            "positions_open": 0,
            "net_side": "flat",
            "net_volume": 0.0,
            "total_profit": 0.0,
            "oldest_open_time": None,
            "max_holding_sec": 0,
        }

    now = time.time()
    net_volume = 0.0
    total_profit = 0.0
    oldest_time = None
    max_holding = 0
    buy_vol = 0.0
    sell_vol = 0.0
    buy_profit = 0.0
    sell_profit = 0.0
    buy_open_px_sum = 0.0
    sell_open_px_sum = 0.0
    buy_count = 0
    sell_count = 0

    for p in positions:
        try:
            vol = float(getattr(p, "volume", 0.0) or 0.0)
        except Exception:
            vol = 0.0
        try:
            open_px = float(getattr(p, "price_open", 0.0) or 0.0)
        except Exception:
            open_px = 0.0
        try:
            profit = float(getattr(p, "profit", 0.0) or 0.0)
        except Exception:
            profit = 0.0
        try:
            t_open = float(getattr(p, "time", 0.0) or 0.0)
        except Exception:
            t_open = 0.0

        ptype = getattr(p, "type", None)
        if ptype == mt5.POSITION_TYPE_BUY:
            buy_vol += vol
            net_volume += vol
            buy_profit += profit
            if vol > 0 and open_px > 0:
                buy_open_px_sum += (open_px * vol)
            buy_count += 1
        elif ptype == mt5.POSITION_TYPE_SELL:
            sell_vol += vol
            net_volume -= vol
            sell_profit += profit
            if vol > 0 and open_px > 0:
                sell_open_px_sum += (open_px * vol)
            sell_count += 1

        total_profit += profit

        if t_open and (oldest_time is None or t_open < float(oldest_time)):
            oldest_time = t_open
        if t_open:
            max_holding = max(max_holding, int(max(0.0, now - t_open)))

    net_side = "flat"
    if net_volume > 0:
        net_side = "buy"
    elif net_volume < 0:
        net_side = "sell"

    buy_avg_open = (buy_open_px_sum / buy_vol) if buy_vol > 0 else 0.0
    sell_avg_open = (sell_open_px_sum / sell_vol) if sell_vol > 0 else 0.0

    return {
        "positions_open": len(positions),
        "net_side": net_side,
        "net_volume": round(abs(net_volume), 4),
        "total_profit": round(total_profit, 2),
        "oldest_open_time": oldest_time,
        "max_holding_sec": int(max_holding),
        "buy_volume": round(buy_vol, 4),
        "sell_volume": round(sell_vol, 4),
        "buy_profit": round(buy_profit, 2),
        "sell_profit": round(sell_profit, 2),
        "buy_count": int(buy_count),
        "sell_count": int(sell_count),
        "buy_avg_open": round(buy_avg_open, 6),
        "sell_avg_open": round(sell_avg_open, 6),
    }


def _compute_entry_multiplier(confirm_unique_sources: int, strong_after_q: bool) -> float:
    # pasadの TryDirectEntryFromSignals と同じ段階
    mult = 1.0
    if confirm_unique_sources >= 2:
        mult = 2.0
    elif confirm_unique_sources >= 1:
        mult = 1.5

    if strong_after_q:
        if mult == 1.0:
            mult = 1.5
        elif mult == 1.5:
            mult = 2.0

    return mult


def _safe_int(v, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _safe_float(v, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _call_openai_with_retry(prompt: str) -> Optional[Dict[str, Any]]:
    if not client:
        return None

    last_err = None
    for i in range(API_RETRY_COUNT):
        try:
            res = client.chat.completions.create(
                model=OPENAI_MODEL,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": "You are a strict trading engine. Output ONLY JSON."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                timeout=API_TIMEOUT_SEC,
            )
            raw_content = (res.choices[0].message.content or "").strip()
            if raw_content.startswith("```"):
                raw_content = raw_content.replace("```json", "").replace("```", "").strip()
            return json.loads(raw_content)
        except Exception as e:
            last_err = e
            if i < API_RETRY_COUNT - 1:
                time.sleep(API_RETRY_WAIT_SEC)

    print(f"[FXAI][AI] failed: {last_err}")
    return None


def _validate_ai_entry_score(decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """期待するJSON: {confluence_score: 1-100, lot_multiplier: 0.5-2.0, reason: str(optional)}"""
    if not isinstance(decision, dict):
        return None

    # accept multiple field names to be robust
    score = decision.get("confluence_score")
    if score is None:
        score = decision.get("score")
    if score is None:
        score = decision.get("confidence")
    score = _safe_int(score, AI_ENTRY_DEFAULT_SCORE)
    score = int(_clamp(score, 1, 100))

    lot_mult = decision.get("lot_multiplier")
    if lot_mult is None:
        lot_mult = decision.get("multiplier")
    lot_mult = _safe_float(lot_mult, AI_ENTRY_DEFAULT_LOT_MULTIPLIER)
    lot_mult = float(_clamp(lot_mult, 0.5, 2.0))

    reason = decision.get("reason")
    if not isinstance(reason, str):
        reason = ""
    reason = reason.strip()

    return {"confluence_score": score, "lot_multiplier": lot_mult, "reason": reason}


def _build_entry_logic_prompt(
    symbol: str,
    market: dict,
    stats: dict,
    action: str,
    normalized_trigger: Optional[Dict[str, Any]] = None,
    qtrend_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Entry context prompt (new spec).

    - Entry is triggered by Lorentzian (entry_trigger).
    - Q-Trend is environment context only (direction + strength).
    - Zones/FVG are additional evidence context.
    """

    if not stats:
        minimal_payload = {
            "symbol": symbol,
            "proposed_action": action,
            "trigger": {
                "source": (normalized_trigger or {}).get("source"),
                "side": (normalized_trigger or {}).get("side"),
                "signal_type": (normalized_trigger or {}).get("signal_type"),
                "event": (normalized_trigger or {}).get("event"),
                "confirmed": (normalized_trigger or {}).get("confirmed"),
                "signal_time": (normalized_trigger or {}).get("signal_time"),
            },
            "qtrend_context": qtrend_context,
            "mt5_positions_summary": (stats or {}).get("mt5_positions_summary"),
            "signals_window": (stats or {}).get("window_signals"),
            "market": {
                "bid": market.get("bid"),
                "ask": market.get("ask"),
                "m15_sma20": market.get("m15_ma"),
                "atr_m5_approx": market.get("atr"),
                "spread_points": market.get("spread"),
            },
            "note": "No aggregated evidence stats available yet. Score conservatively.",
        }
        return (
            "You are a strict confluence scoring engine for algorithmic trading.\n"
            "Trigger: Lorentzian fired the proposed_action.\n"
            "Environment: Q-Trend provides direction/strength context only (not a trigger).\n"
            "Return ONLY strict JSON with this schema:\n"
            '{"confluence_score": 1-100, "lot_multiplier": 0.5-2.0, "reason": "short"}\n\n'
            "ContextJSON:\n"
            + json.dumps(minimal_payload, ensure_ascii=False)
        )

    base = _build_entry_filter_prompt(symbol, market, stats, action, normalized_trigger=normalized_trigger, qtrend_context=qtrend_context)
    return (
        "You are a strict confluence scoring engine for algorithmic trading.\n"
        "Given the technical context, output ONLY strict JSON with this schema:\n"
        '{"confluence_score": 1-100, "lot_multiplier": 0.5-2.0, "reason": "short"}\n\n'
        "--- CONTEXT BELOW (do not output it) ---\n"
        + base
    )


def _build_entry_filter_prompt(
    symbol: str,
    market: dict,
    stats: dict,
    action: str,
    normalized_trigger: Optional[Dict[str, Any]] = None,
    qtrend_context: Optional[Dict[str, Any]] = None,
) -> str:
    """ENTRY最終判断のためのコンテキストを構築。

    - BUY/SELL自体はローカルで確定済み（action）。
    - AIは action/confidence/multiplier のみを返す。
    """
    now = time.time()
    stats = stats or {}

    # New spec does not anchor on Q-Trend time; keep legacy stats if present.
    q_age_sec = int(now - stats.get("q_time", 0)) if stats.get("q_time") else -1

    bid = float(market.get("bid") or 0.0)
    m15_ma = float(market.get("m15_ma") or 0.0)
    m15_trend = "UP" if bid > m15_ma else "DOWN"
    trend_align = "ALIGNED" if (action == "BUY" and m15_trend == "UP") or (action == "SELL" and m15_trend == "DOWN") else "MISALIGNED"

    # 文脈スコア（AIへ“判断の軸”として提供。AIにBUY/SELLはさせない）
    confirm_u = int(stats.get("confirm_unique_sources") or 0)
    opp_u = int(stats.get("opp_unique_sources") or 0)
    confirm_n = int(stats.get("confirm_signals") or 0)
    opp_n = int(stats.get("opp_signals") or 0)

    osgfc_side = (stats.get("osgfc_latest_side") or "").lower()
    osgfc_align = (
        "UNKNOWN"
        if not osgfc_side
        else "ALIGNED"
        if ((action == "BUY" and osgfc_side == "buy") or (action == "SELL" and osgfc_side == "sell"))
        else "MISALIGNED"
    )

    fvg_same = int(stats.get("fvg_touch_same") or 0)
    fvg_opp = int(stats.get("fvg_touch_opp") or 0)
    zones_same = int(stats.get("zones_touch_same") or 0)
    zones_opp = int(stats.get("zones_touch_opp") or 0)
    zones_confirmed_recent = int(stats.get("zones_confirmed_recent") or stats.get("zones_confirmed") or 0)
    w_confirm = float(stats.get("weighted_confirm_score") or 0.0)
    w_oppose = float(stats.get("weighted_oppose_score") or 0.0)

    # In the new spec, Q-Trend strength is taken from qtrend_context (Normal/Strong).
    qt_side = (qtrend_context or {}).get("side")
    qt_strength = (qtrend_context or {}).get("strength")
    qt_dir = _dir_from_side(str(qt_side or ""))
    qt_strength_norm = "Strong" if str(qt_strength or "").lower() == "strong" else "Normal" if qt_side else "Unknown"
    trigger_side = ((normalized_trigger or {}).get("side") or "").lower()
    trigger_dir = _dir_from_side(trigger_side)
    alignment = "UNKNOWN"
    if trigger_dir in {"UP", "DOWN"} and qt_dir in {"UP", "DOWN"}:
        alignment = "ALIGNED" if trigger_dir == qt_dir else "MISALIGNED"

    is_strong_momentum = (qt_strength_norm == "Strong")
    momentum_factor = 1.5 if is_strong_momentum else 1.0
    strong_bonus = 2 if is_strong_momentum else 0
    q_trigger_type = qt_strength_norm

    confluence_score_base = (confirm_u * 2) + min(confirm_n, 6)
    confluence_score = confluence_score_base + strong_bonus
    opposition_score = (opp_u * 3) + min(opp_n, 6)
    strong_flag = bool(stats.get("strong_after_q"))

    local_multiplier = _compute_entry_multiplier(confirm_u, strong_flag)

    # スプレッドが大きすぎる局面はEVが落ちやすい（簡易目安）
    spread_points = float(market.get("spread") or 0.0)
    spread_flag = "WIDE" if spread_points >= 80 else "NORMAL"

    payload = {
        "symbol": symbol,
        "proposed_action": action,
        "trigger": {
            "source": (normalized_trigger or {}).get("source"),
            "side": (normalized_trigger or {}).get("side"),
            "signal_type": (normalized_trigger or {}).get("signal_type"),
            "event": (normalized_trigger or {}).get("event"),
            "confirmed": (normalized_trigger or {}).get("confirmed"),
            "signal_time": (normalized_trigger or {}).get("signal_time"),
        },
        "signals_window": stats.get("window_signals"),
        "mt5_positions_summary": stats.get("mt5_positions_summary"),
        "qtrend_context": {
            "side": qt_side,
            "direction": qt_dir,
            "strength": qt_strength_norm,
            "age_sec": int(now - float((qtrend_context or {}).get("updated_at") or 0.0)) if (qtrend_context or {}).get("updated_at") else None,
            "alignment_vs_trigger": alignment,
        },
        "qtrend": {
            "side": qt_side,
            "age_sec": q_age_sec,
            "is_strong": bool(is_strong_momentum),
            "is_strong_momentum": is_strong_momentum,
            "trigger_type": q_trigger_type,
            "momentum_factor": momentum_factor,
            "source": (qtrend_context or {}).get("source"),
            "strong_after_q": strong_flag,
        },
        "confluence": {
            "confirm_unique_sources": confirm_u,
            "confirm_signals": confirm_n,
            "oppose_unique_sources": opp_u,
            "oppose_signals": opp_n,
            "confluence_score": confluence_score,
            "confluence_score_base": confluence_score_base,
            "qtrend_strong_bonus": strong_bonus,
            "opposition_score": opposition_score,
            "weighted_confirm_score": w_confirm,
            "weighted_oppose_score": w_oppose,
            "fvg_touch_same": fvg_same,
            "fvg_touch_opp": fvg_opp,
            "zones_touch_same": zones_same,
            "zones_touch_opp": zones_opp,
            "zones_confirmed_recent": zones_confirmed_recent,
            "osgfc_latest_side": osgfc_side,
            "osgfc_alignment": osgfc_align,
            "note": "touch系(FVG/Zones)は瞬間イベント。Opposition評価は過剰反応せず、構造(confirmed)と区別する。",
        },
        "market": {
            "bid": market.get("bid"),
            "ask": market.get("ask"),
            "m15_sma20": market.get("m15_ma"),
            "m15_trend": m15_trend,
            "trend_alignment": trend_align,
            "atr_m5_approx": market.get("atr"),
            "atr_points_approx": market.get("atr_points"),
            "atr_to_spread_approx": market.get("atr_to_spread"),
            "spread_points": spread_points,
            "spread_flag": spread_flag,
        },
        "constraints": {
            "confluence_score_range": [1, 100],
            "lot_multiplier_range": [0.5, 2.0],
            "local_multiplier": local_multiplier,
            "final_multiplier_max": 2.0,
            "note": "Return JSON only. Score 70+ only when confluence is truly strong AND market conditions (ATR vs spread, trend alignment) are acceptable. Be conservative on uncertainty.",
        },
    }

    return (
        "You are a strict XAUUSD/GOLD DAY TRADING entry gate (not scalping).\n"
        "Core principle: Minimize loss, Maximize profit (cut losers fast; let winners run when EV is positive).\n"
        "Trigger: Lorentzian fired the proposed_action (entry_trigger).\n"
        "Environment: Q-Trend is context only (direction+strength), NOT a trigger.\n"
        "You MUST NOT suggest BUY/SELL; the proposed_action is already decided locally.\n"
        "Use these decision principles:\n"
        "- Prefer Q-Trend ALIGNED with Lorentzian trigger direction (trend-following).\n"
        "- If Q-Trend strength is Strong, rate ALIGNED entries even higher; be more willing to approve.\n"
        "- If MISALIGNED, treat it as counter-trend: require strong structural evidence (Zones confirmations, clean space/EV). If evidence is weak, score low/skip.\n"
        "- Also consider higher-timeframe trend_alignment from M15 as a secondary filter.\n"
        "- Prioritize current price action and momentum over simple MA position.\n"
        "- Prefer stronger confluence: higher confirm_unique_sources, higher weighted_confirm_score.\n"
        "- Evaluate opposition with nuance: confirmed/structural opposition matters most; touch-based opposition can be noise.\n"
        "- If Q-Trend strength is Strong, treat it as higher breakout/trend-continuation probability: tolerate some opposite touch noise if EV/space (ATR vs spread) remains attractive.\n"
        "- If some opposite FVG/Zones exist BUT trend is aligned and ATR-to-spread is healthy and confluence is decent, you MAY still approve ENTRY (EV can remain positive).\n"
        "- If structural Zones context exists (zones_confirmed_recent > 0) and confluence is weak, be conservative unless other evidence strongly improves EV.\n"
        "- Penalize wide spread.\n"
        "Return ONLY strict JSON schema:\n"
        '{"confluence_score": 1-100, "lot_multiplier": 0.5-2.0, "reason": "short"}\n\n'
        "ContextJSON:\n"
        + json.dumps(payload, ensure_ascii=False)
    )



def _validate_ai_close_decision(decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """期待するJSON: {action: "HOLD"|"CLOSE", confidence: 0-100, reason: str}"""
    if not isinstance(decision, dict):
        return None
    action = decision.get("action")
    if not isinstance(action, str):
        return None
    action = action.strip().upper()
    if action not in {"HOLD", "CLOSE"}:
        return None
    confidence = _safe_int(decision.get("confidence"), AI_CLOSE_DEFAULT_CONFIDENCE)
    confidence = int(_clamp(confidence, 0, 100))
    reason = decision.get("reason")
    if not isinstance(reason, str):
        reason = ""
    reason = reason.strip()

    # 追加: trail_modeの検証
    trail_mode = decision.get("trail_mode", "NORMAL").upper()
    if trail_mode not in {"WIDE", "NORMAL", "TIGHT"}:
        trail_mode = "NORMAL"

    return {"action": action, "confidence": confidence, "reason": reason, "trail_mode": trail_mode}  # 追加


def _build_close_logic_prompt(symbol: str, market: dict, stats: Optional[dict], pos_summary: dict, latest_signal: dict) -> str:
    """保有中のCLOSE/HOLD判断用プロンプト（Day Trading・損小利大）。"""
    now = time.time()
    stats = stats or {}

    q_age_sec = int(now - stats.get("q_time", 0)) if stats.get("q_time") else -1

    bid = float(market.get("bid") or 0.0)
    ask = float(market.get("ask") or 0.0)
    mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else max(bid, ask)

    net_side = (pos_summary.get("net_side") or "flat").lower()
    net_avg_open = 0.0
    if net_side == "buy":
        net_avg_open = float(pos_summary.get("buy_avg_open") or 0.0)
    elif net_side == "sell":
        net_avg_open = float(pos_summary.get("sell_avg_open") or 0.0)

    # directional price move (approx) from avg open to now, for intuition only
    move_points = 0.0
    if net_side == "buy" and net_avg_open > 0 and mid > 0:
        move_points = mid - net_avg_open
    elif net_side == "sell" and net_avg_open > 0 and mid > 0:
        move_points = net_avg_open - mid

    payload = {
        "symbol": symbol,
        "mode": "POSITION_MANAGEMENT",
        "position": {
            "positions_open": int(pos_summary.get("positions_open") or 0),
            "net_side": pos_summary.get("net_side"),
            "net_volume": pos_summary.get("net_volume"),
            "total_profit": pos_summary.get("total_profit"),
            "max_holding_sec": pos_summary.get("max_holding_sec"),
            "buy_count": int(pos_summary.get("buy_count") or 0),
            "sell_count": int(pos_summary.get("sell_count") or 0),
            "buy_avg_open": pos_summary.get("buy_avg_open"),
            "sell_avg_open": pos_summary.get("sell_avg_open"),
            "buy_profit": pos_summary.get("buy_profit"),
            "sell_profit": pos_summary.get("sell_profit"),
            "net_avg_open": round(net_avg_open, 6),
            "net_move_points_approx": round(move_points, 6),
        },
        "qtrend_context": {
            "latest_q_side": stats.get("q_side"),
            "latest_q_age_sec": q_age_sec,
            "latest_q_source": stats.get("q_source"),
            "trigger_type": stats.get("q_trigger_type"),
            "is_strong_momentum": bool(stats.get("is_strong_momentum")),
            "momentum_factor": stats.get("momentum_factor"),
            "confirm_unique_sources": int(stats.get("confirm_unique_sources") or 0),
            "oppose_unique_sources": int(stats.get("opp_unique_sources") or 0),
            "weighted_confirm_score": float(stats.get("weighted_confirm_score") or 0.0),
            "weighted_oppose_score": float(stats.get("weighted_oppose_score") or 0.0),
            "fvg_touch_opp": int(stats.get("fvg_touch_opp") or 0),
            "zones_touch_opp": int(stats.get("zones_touch_opp") or 0),
            "zones_confirmed_recent": int(stats.get("zones_confirmed_recent") or stats.get("zones_confirmed") or 0),
            "zones_confirmed_after_q": int(stats.get("zones_confirmed_after_q") or 0),
        },
        "market": {
            "bid": market.get("bid"),
            "ask": market.get("ask"),
            "m15_sma20": market.get("m15_ma"),
            "atr_m5_approx": market.get("atr"),
            "spread_points": market.get("spread"),
        },
        "latest_signal": {
            "source": latest_signal.get("source"),
            "side": latest_signal.get("side"),
            "event": latest_signal.get("event"),
            "signal_type": latest_signal.get("signal_type"),
            "confirmed": latest_signal.get("confirmed"),
            "signal_time": latest_signal.get("signal_time"),
        },
        "notes": {
            "opposition_handling": "Opposition signals can be noise. Prioritize confirmed/structural reversal signs (e.g., strong opposite Zones context) over single touch events.",
            "day_trading_goal": "Loss-cut small, Profit-target large. If profit is available and reversal risk rises, take profit. If loss grows and reversal evidence increases, exit early.",
        },
    }

    return (
    "You are an elite XAUUSD/GOLD DAY TRADER focused on maximizing run-up profits while securing gains.\n"
    "1. Decide ACTION: HOLD or CLOSE.\n"
    "2. Decide TRAIL_MODE (Dynamic Trailing):\n"
    "   - 'WIDE': Momentum is VERY STRONG. Use wide trailing to catch big waves (avoid noise stop-out).\n"
    "   - 'NORMAL': Standard trend strength.\n"
    "   - 'TIGHT': Momentum is weakening or chopping. Tighten stop to protect profits.\n"
    "Return ONLY strict JSON with this schema:\n"
    '{"action": "HOLD"|"CLOSE", "confidence": 0-100, "trail_mode": "WIDE"|"NORMAL"|"TIGHT", "reason": "short"}\n\n'
    "ContextJSON:\n"
    + json.dumps(payload, ensure_ascii=False)
    )


def _ai_close_hold_decision(symbol: str, market: dict, stats: Optional[dict], pos_summary: dict, latest_signal: dict) -> Optional[Dict[str, Any]]:
    if not client:
        return None
    prompt = _build_close_logic_prompt(symbol, market, stats, pos_summary, latest_signal)
    decision = _call_openai_with_retry(prompt)
    if not decision:
        print("[FXAI][AI] No response from AI (close/hold). Using fallback.")
        return None
    validated = _validate_ai_close_decision(decision)
    if not validated:
        print("[FXAI][AI] Invalid AI response (close/hold). Using fallback.")
        return None
    return validated


def _ai_entry_score(
    symbol: str,
    market: dict,
    stats: dict,
    action: str,
    normalized_trigger: Optional[Dict[str, Any]] = None,
    qtrend_context: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """AIに Confluence Score(1-100) と Lot Multiplier を出させる。"""
    if not client:
        return None
    prompt = _build_entry_logic_prompt(
        symbol,
        market,
        stats,
        action,
        normalized_trigger=normalized_trigger,
        qtrend_context=qtrend_context,
    )
    decision = _call_openai_with_retry(prompt)
    if not decision:
        print("[FXAI][AI] No response from AI (entry score).")
        return None

    validated = _validate_ai_entry_score(decision)
    if not validated:
        print("[FXAI][AI] Invalid AI response (entry score).")
        return None

    return validated


def _attempt_entry_from_lorentzian(
    symbol: str,
    normalized_trigger: dict,
    now: float,
    pos_summary: Optional[dict] = None,
) -> tuple[str, int]:
    """New spec entry flow.

    - Triggered ONLY by Lorentzian entry_trigger.
    - Q-Trend is context-only and read from in-memory cache.
    - Zones/FVG context read from signals_cache.
    """
    trig_side = (normalized_trigger.get("side") or "").strip().lower()
    action = "BUY" if trig_side == "buy" else "SELL" if trig_side == "sell" else ""
    if action not in {"BUY", "SELL"}:
        _set_status(last_result="Invalid Lorentzian side", last_result_at=time.time())
        return "Invalid trigger", 400

    # Safety: block entries when EA heartbeat is stale/missing.
    if not _heartbeat_is_fresh(now_ts=now):
        _set_status(last_result="Blocked by heartbeat", last_result_at=time.time())
        return "Blocked by heartbeat", 503

    positions_open = int((pos_summary or {}).get("positions_open") or 0)
    net_side = ((pos_summary or {}).get("net_side") or "flat").lower()

    is_addon = False

    # Add-on policy (user requirement): allow add-ons ONLY when position side == trigger side,
    # and cap total entries per open-position session.
    if positions_open <= 0:
        # reset session tracking when flat
        with _addon_lock:
            _addon_state_by_symbol.pop(symbol, None)
    else:
        # When holding positions, add-ons are strictly controlled.
        if net_side not in {"buy", "sell"}:
            _set_status(last_result="Skip entry (net_side unknown)", last_result_at=time.time())
            return "Skip (net_side unknown)", 200

        if (not ALLOW_ADD_ON_ENTRIES) or (net_side != trig_side):
            _set_status(last_result="Skip entry (position open)", last_result_at=time.time())
            return "Skip (position open)", 200

        is_addon = True

        max_entries = max(1, int(ADDON_MAX_ENTRIES_PER_POSITION or 5))
        with _addon_lock:
            st = _addon_state_by_symbol.get(symbol)
            # start/refresh session if side changed (safety)
            if not st or (st.get("side") != net_side):
                st = {"side": net_side, "count": 0, "updated_at": now}
            count = int(st.get("count") or 0)
            if count >= max_entries:
                _addon_state_by_symbol[symbol] = st
                _set_status(
                    last_result="Skip entry (add-on limit)",
                    last_result_at=time.time(),
                    last_addon_limit={"max": max_entries, "count": count, "side": net_side},
                )
                return "Skip (add-on limit)", 200
            _addon_state_by_symbol[symbol] = st

    market = get_mt5_market_data(symbol)
    try:
        if float(market.get("atr") or 0.0) > 0:
            _last_atr_by_symbol[symbol] = float(market.get("atr") or 0.0)
    except Exception:
        pass

    # --- Local safety guards (do not rely on AI for these) ---
    spread_points = float(market.get("spread") or 0.0)
    atr_to_spread = market.get("atr_to_spread")
    try:
        atr_to_spread_v = float(atr_to_spread) if atr_to_spread is not None else None
    except Exception:
        atr_to_spread_v = None

    if spread_points <= 0:
        _set_status(last_result="Blocked (no spread)", last_result_at=time.time())
        return "Blocked (no spread)", 503

    if float(ENTRY_MAX_SPREAD_POINTS or 0.0) > 0 and spread_points >= float(ENTRY_MAX_SPREAD_POINTS):
        _set_status(
            last_result="Blocked (spread too wide)",
            last_result_at=time.time(),
            last_entry_guard={"spread_points": spread_points, "max": float(ENTRY_MAX_SPREAD_POINTS)},
        )
        return "Blocked (spread too wide)", 200

    if float(ENTRY_MIN_ATR_TO_SPREAD or 0.0) > 0 and (atr_to_spread_v is not None):
        if atr_to_spread_v < float(ENTRY_MIN_ATR_TO_SPREAD):
            _set_status(
                last_result="Blocked (ATR too small vs spread)",
                last_result_at=time.time(),
                last_entry_guard={"atr_to_spread": atr_to_spread_v, "min": float(ENTRY_MIN_ATR_TO_SPREAD)},
            )
            return "Blocked (ATR too small vs spread)", 200

    if float(ENTRY_COOLDOWN_SEC or 0.0) > 0:
        with _entry_lock:
            last_sent = float(_last_order_sent_at_by_symbol.get(symbol, 0.0) or 0.0)
        if last_sent > 0 and (now - last_sent) < float(ENTRY_COOLDOWN_SEC):
            _set_status(
                last_result="Blocked (cooldown)",
                last_result_at=time.time(),
                last_entry_guard={
                    "cooldown_sec": float(ENTRY_COOLDOWN_SEC),
                    "since_last_order_sec": round(float(now - last_sent), 3),
                },
            )
            return "Blocked (cooldown)", 200

    qtrend_ctx = _get_qtrend_context(symbol, now=now)

    trig_st = float(normalized_trigger.get("signal_time") or normalized_trigger.get("receive_time") or now)
    window_signals = _collect_window_signals_around_trigger(symbol, trig_st, trig_side, window_sec=300.0)
    evidence_ctx = _collect_recent_context_signals(symbol, now)

    # Assemble a compact stats dict for the existing prompt structure.
    # Keep legacy keys used in the prompt where they still add value.
    stats = {
        "q_time": None,
        "q_side": (qtrend_ctx or {}).get("side"),
        "q_source": (qtrend_ctx or {}).get("source"),
        "q_is_strong": (qtrend_ctx or {}).get("strength") == "strong",
        "q_trigger_type": "Strong" if (qtrend_ctx or {}).get("strength") == "strong" else "Normal",
        "is_strong_momentum": (qtrend_ctx or {}).get("strength") == "strong",
        "momentum_factor": 1.5 if (qtrend_ctx or {}).get("strength") == "strong" else 1.0,
        "strong_after_q": (qtrend_ctx or {}).get("strength") == "strong",
        "zones_confirmed_recent": int((evidence_ctx or {}).get("zones_confirmed_recent") or 0),
        "window_signals": window_signals,
        "mt5_positions_summary": pos_summary,
    }

    # Optional: light-weight derived confluence counts for AI context.
    # We do not hard-gate by these counts under the new spec.
    aligned = (window_signals or {}).get("aligned") or []
    opposed = (window_signals or {}).get("opposed") or []

    confirm_sources = {ev.get("source") for ev in aligned if ev.get("source")}
    oppose_sources = {ev.get("source") for ev in opposed if ev.get("source")}
    stats["confirm_unique_sources"] = int(len(confirm_sources))
    stats["opp_unique_sources"] = int(len(oppose_sources))
    stats["confirm_signals"] = int(len(aligned))
    stats["opp_signals"] = int(len(opposed))

    # AI scoring is mandatory; throttle identical attempts.
    global _last_ai_attempt_key, _last_ai_attempt_at
    trig_src = (normalized_trigger.get("source") or "")
    trig_evt = (normalized_trigger.get("event") or "")
    attempt_key = f"LZ:{symbol}:{action}:{trig_src}:{trig_evt}:{trig_st:.3f}"

    now_mono = time.time()
    if _last_ai_attempt_key == attempt_key and (now_mono - float(_last_ai_attempt_at or 0.0)) < AI_ENTRY_THROTTLE_SEC:
        _set_status(last_result="AI throttled", last_result_at=time.time())
        return "AI throttled", 200
    _last_ai_attempt_key = attempt_key
    _last_ai_attempt_at = now_mono

    ai_decision = _ai_entry_score(
        symbol,
        market,
        stats,
        action,
        normalized_trigger=normalized_trigger,
        qtrend_context=qtrend_ctx,
    )
    if not ai_decision:
        _set_status(last_result="Blocked by AI (no score)", last_result_at=time.time())
        return "Blocked by AI", 503

    ai_score = int(ai_decision.get("confluence_score") or 0)
    ai_reason = (ai_decision.get("reason") or "").strip()
    lot_mult = float(ai_decision.get("lot_multiplier") or 1.0)

    if is_addon:
        min_addon_score = int(ADDON_MIN_AI_SCORE or AI_ENTRY_MIN_SCORE)
        if ai_score < min_addon_score:
            _set_status(
                last_result="Blocked add-on by AI",
                last_result_at=time.time(),
                last_entry_guard={"addon": True, "ai_score": ai_score, "min_required": min_addon_score},
            )
            return "Blocked add-on by AI", 403
    else:
        if ai_score < AI_ENTRY_MIN_SCORE:
            _set_status(last_result="Blocked by AI", last_result_at=time.time())
            return "Blocked by AI", 403

    # Final multiplier: start from 1.0, modulate by AI.
    final_multiplier = float(_clamp(1.0 * lot_mult, 0.5, 2.0))

    # Re-check heartbeat just before sending the order (avoid race).
    if not _heartbeat_is_fresh(now_ts=time.time()):
        _set_status(last_result="Blocked by heartbeat", last_result_at=time.time())
        return "Blocked by heartbeat", 503

    reason = ai_reason or "lorentzian_entry"
    payload = {
        "type": "ORDER",
        "action": action,
        "symbol": symbol,
        "atr": float(market.get("atr") or 0.0),
        "multiplier": final_multiplier,
        "reason": reason,
        "ai_confidence": ai_score,
        "ai_reason": ai_reason,
    }
    zmq_socket.send_json({**payload})

    # Local cooldown timestamp
    try:
        with _entry_lock:
            _last_order_sent_at_by_symbol[symbol] = float(time.time())
    except Exception:
        pass

    # Update add-on session counter after successful send.
    try:
        with _addon_lock:
            st = _addon_state_by_symbol.get(symbol)
            if not st or (st.get("side") != trig_side):
                st = {"side": trig_side, "count": 0}
            st["count"] = int(st.get("count") or 0) + 1
            st["updated_at"] = time.time()
            _addon_state_by_symbol[symbol] = st
    except Exception:
        pass

    _set_status(
        last_result="OK",
        last_result_at=time.time(),
        last_order={
            "action": action,
            "symbol": symbol,
            "atr": float(market.get("atr") or 0.0),
            "multiplier": final_multiplier,
            "ai_confidence": ai_score,
            "ai_reason": ai_reason,
            "trigger": {
                "source": normalized_trigger.get("source"),
                "event": normalized_trigger.get("event"),
                "signal_time": normalized_trigger.get("signal_time"),
                "side": normalized_trigger.get("side"),
                "signal_type": normalized_trigger.get("signal_type"),
            },
            "qtrend_context": qtrend_ctx,
            "evidence": {
                "zones_confirmed_recent": stats.get("zones_confirmed_recent"),
                "latest_zone_touch": (evidence_ctx or {}).get("latest_zone_touch"),
                "latest_fvg_touch": (evidence_ctx or {}).get("latest_fvg_touch"),
                "window_signals": window_signals,
            },
        },
    )

    print(f"[FXAI][ZMQ] Order sent: {payload}")
    return "OK", 200


@app.route('/webhook', methods=['POST'])
def webhook():
    """TradingViewからのシグナルを受信し、AIフィルターを適用してエントリーを決定。"""
    # Optional shared-secret authentication
    if WEBHOOK_TOKEN:
        header_token = (request.headers.get("X-Webhook-Token") or "").strip()
        data_peek = request.get_json(silent=True)
        body_token = (data_peek.get("token") if isinstance(data_peek, dict) else "") or ""
        if header_token != WEBHOOK_TOKEN and str(body_token).strip() != WEBHOOK_TOKEN:
            return "Unauthorized", 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return "Invalid data", 400

    now = time.time()

    # Lazy init (for WSGI / import-time safety)
    if not ensure_runtime_initialized():
        return "Runtime init failed", 503

    requested_symbol = _extract_symbol_from_webhook(data)
    symbol, sym_ok = _ensure_mt5_symbol_selected(requested_symbol)
    if not sym_ok:
        print(f"[FXAI][WARN] MT5 symbol_select failed for '{requested_symbol}'. Using '{symbol}' (may still be unavailable).")

    _set_status(
        last_webhook_at=now,
        last_webhook_symbol=symbol,
        last_result=None,
        last_result_at=None,
        last_webhook_source=(data.get("source") or ""),
        last_webhook_side=(data.get("side") or data.get("action") or ""),
    )

    # 受信シグナルをキャッシュに保存（合流判定のため）
    # NOTE: デフォルトでは action=BUY/SELL だけで Q-Trend 扱いにしない（誤発火防止）。
    # 旧テンプレ互換が必要なら ASSUME_ACTION_IS_QTREND=true を設定。
    raw_action = data.get("action") or data.get("side")
    raw_action_upper = (raw_action or "").strip().upper()
    inferred_side = ""
    if raw_action_upper in {"BUY", "SELL"}:
        inferred_side = raw_action_upper.lower()
    elif isinstance(raw_action, str) and raw_action.strip().lower() in {"buy", "sell"}:
        inferred_side = raw_action.strip().lower()

    source_in = (data.get("source") or "").strip()
    source_for_cache = source_in
    if not source_for_cache and ASSUME_ACTION_IS_QTREND and inferred_side:
        source_for_cache = "Q-Trend"

    signal = {
        "symbol": symbol,
        "source": source_for_cache,
        "side": data.get("side") or inferred_side,
        "strength": data.get("strength"),
        "signal_type": data.get("signal_type"),
        "event": data.get("event"),
        "confirmed": data.get("confirmed"),
        # TradingViewで time/timenow を渡している場合に備える
        "time": data.get("time") or data.get("timenow") or data.get("timestamp"),
        "receive_time": now,
    }

    normalized = _normalize_signal_fields(signal)

    with signals_lock:
        appended = _append_signal_dedup_locked(normalized)
        _prune_signals_cache_locked(now)
        _mark_cache_dirty_locked(now)
        if not CACHE_ASYNC_FLUSH_ENABLED:
            _save_cache_locked()

    if not appended:
        _set_status(last_result="Duplicate webhook", last_result_at=time.time())
        return "Duplicate", 200

    print(
        "[FXAI][WEBHOOK] recv "
        + json.dumps(
            {
                "symbol": symbol,
                "source": normalized.get("source"),
                "side": normalized.get("side"),
                "signal_type": normalized.get("signal_type"),
                "event": normalized.get("event"),
                "confirmed": normalized.get("confirmed"),
                "strength": normalized.get("strength"),
                "time": normalized.get("signal_time"),
                "raw_source": source_in,
                "raw_action": raw_action,
            },
            ensure_ascii=False,
        )
    )

    # Determine routing by signal_type
    sig_type = (normalized.get("signal_type") or "").strip().lower()

    pending_entry_trigger = False
    normalized_trigger: Optional[Dict[str, Any]] = None

    # Context-only signals (Q-Trend): always update in-memory context.
    # IMPORTANT: Do not early-return here because if we are holding positions we still
    # want to run position management AI on any incoming signal (user requirement).
    if sig_type == "context":
        if _is_qtrend_source(str(normalized.get("source") or "")):
            _update_qtrend_context_from_signal(normalized)

    # Entry trigger: Lorentzian starts the AI decision flow, but we may still run
    # management first if positions are open.
    if sig_type == "entry_trigger":
        src = (normalized.get("source") or "").strip()
        if src == "Lorentzian":
            pending_entry_trigger = True
            normalized_trigger = dict(normalized)
        else:
            # Unknown entry_trigger: store for audit only.
            _set_status(last_result="Stored (unknown entry_trigger)", last_result_at=time.time())
            return "Stored", 200

    # --- HEARTBEAT STALE POLICY ---
    # Under freeze mode: do not send any management (HOLD/CLOSE) nor entries while heartbeat is stale.
    if (HEARTBEAT_STALE_MODE == "freeze") and (not _heartbeat_is_fresh(now_ts=now)):
        _set_status(last_result="Frozen by heartbeat", last_result_at=time.time())
        return "Frozen by heartbeat", 200

    # --- POSITION MANAGEMENT MODE (CLOSE/HOLD) ---
    pos_summary = get_mt5_positions_summary(symbol)
    if int(pos_summary.get("positions_open") or 0) > 0:
        net_side = (pos_summary.get("net_side") or "flat").lower()
        market = get_mt5_market_data(symbol)
        stats = get_qtrend_anchor_stats(symbol)

        # Always attempt management AI when positions are open (recommended timing, cost not a concern).
        if AI_CLOSE_ENABLED:
            global _last_close_attempt_key, _last_close_attempt_at
            now_mono = time.time()

            src = (normalized.get("source") or "")
            evt = (normalized.get("event") or "")
            sig_side = (normalized.get("side") or "").lower()

            is_reversal_like = (
                (net_side in {"buy", "sell"} and sig_side in {"buy", "sell"} and sig_side != net_side)
                and (
                    (src in {"Q-Trend Strong", "Q-Trend", "Q-Trend-Strong", "Q-Trend-Normal"})
                    or (src == "FVG" and evt == "fvg_touch")
                    or (src == "Zones" and evt in {"zone_retrace_touch", "zone_touch"})
                )
            )

            last_attempt_age = now_mono - float(_last_close_attempt_at or 0.0)
            if (last_attempt_age < AI_CLOSE_THROTTLE_SEC) and (not is_reversal_like):
                _set_status(
                    last_result="AI close throttled",
                    last_result_at=time.time(),
                    last_mgmt_throttled={
                        "cooldown_sec": float(AI_CLOSE_THROTTLE_SEC),
                        "since_last_call_sec": round(float(last_attempt_age), 3),
                        "reason": "cooldown",
                        "bypass": False,
                        "incoming": {"source": src, "event": evt, "side": sig_side},
                    },
                )
            else:
                attempt_key = f"{symbol}:{pos_summary.get('net_side')}:{int(pos_summary.get('positions_open') or 0)}:{src}:{evt}:{sig_side}"
                _last_close_attempt_key = attempt_key
                _last_close_attempt_at = now_mono

                ai_decision = _ai_close_hold_decision(symbol, market, stats, pos_summary, normalized)
                if (not ai_decision) or ai_decision["confidence"] < AI_CLOSE_MIN_CONFIDENCE:
                    if (not ai_decision) and AI_CLOSE_FALLBACK == "default_close":
                        ai_decision = {
                            "action": "CLOSE",
                            "confidence": int(_clamp(AI_CLOSE_DEFAULT_CONFIDENCE, 0, 100)),
                            "reason": "fallback_default_close",
                        }
                    else:
                        _set_status(
                            last_result="HOLD (AI fallback)",
                            last_result_at=time.time(),
                            last_mgmt_action="HOLD",
                            last_mgmt_confidence=None,
                            last_mgmt_reason="ai_fallback_hold",
                            last_mgmt_at=time.time(),
                        )
                        zmq_socket.send_json({"type": "HOLD", "reason": "ai_fallback_hold"})
                        # even on fallback HOLD, we can still consider add-on entries below
                        ai_decision = {"action": "HOLD", "confidence": 0, "reason": "ai_fallback_hold", "trail_mode": "NORMAL"}

                decision_action = (ai_decision or {}).get("action")
                decision_reason = (ai_decision or {}).get("reason") or ""
                if decision_action == "CLOSE":
                    zmq_socket.send_json({"type": "CLOSE", "reason": decision_reason})
                    _set_status(
                        last_result="CLOSE",
                        last_result_at=time.time(),
                        last_mgmt_action="CLOSE",
                        last_mgmt_confidence=int((ai_decision or {}).get("confidence") or 0),
                        last_mgmt_reason=decision_reason,
                        last_mgmt_at=time.time(),
                        last_mgmt_throttled=None,
                    )
                    return "CLOSE", 200
                else:
                    t_mode = (ai_decision or {}).get("trail_mode", "NORMAL")
                    zmq_socket.send_json({"type": "HOLD", "reason": decision_reason, "trail_mode": t_mode})
                    _set_status(
                        last_result="HOLD",
                        last_result_at=time.time(),
                        last_mgmt_action="HOLD",
                        last_mgmt_confidence=int((ai_decision or {}).get("confidence") or 0),
                        last_mgmt_reason=decision_reason,
                        last_mgmt_at=time.time(),
                        last_mgmt_throttled=None,
                    )

        # After management decision (unless CLOSED), allow add-on entries by falling through to ENTRY section.

    # --- ENTRY (Lorentzian) ---
    if pending_entry_trigger and normalized_trigger:
        return _attempt_entry_from_lorentzian(symbol, normalized_trigger, now, pos_summary=pos_summary)

    if sig_type == "context":
        _set_status(last_result="Context stored", last_result_at=time.time())
        return "Context stored", 200

    _set_status(last_result="Stored", last_result_at=time.time())
    return "Stored", 200


@app.route('/ping', methods=['GET'])
def ping():
    return {"ok": True, "ts": time.time()}, 200


@app.route('/status', methods=['GET'])
def status():
    # Optional shared-secret authentication (same rule as /webhook)
    if WEBHOOK_TOKEN:
        header_token = (request.headers.get("X-Webhook-Token") or "").strip()
        if header_token != WEBHOOK_TOKEN:
            return "Unauthorized", 401
    return _get_status_snapshot(), 200


if __name__ == '__main__':
    print(f"[FXAI] webhook http://0.0.0.0:{WEBHOOK_PORT}/webhook")
    print(f"[FXAI] ZMQ bind: {ZMQ_BIND}")
    if ZMQ_HEARTBEAT_ENABLED:
        print(f"[FXAI] ZMQ heartbeat bind: {ZMQ_HEARTBEAT_BIND} (timeout={ZMQ_HEARTBEAT_TIMEOUT_SEC}s)")
    print(f"[FXAI] heartbeat stale mode: {HEARTBEAT_STALE_MODE}")
    if WEEKEND_CLOSE_ENABLED:
        print(
            f"[FXAI] weekend close enabled: tz={WEEKEND_CLOSE_TZ} "
            f"weekday={WEEKEND_CLOSE_WEEKDAY} time={WEEKEND_CLOSE_HOUR:02d}:{WEEKEND_CLOSE_MINUTE:02d} "
            f"window_min={WEEKEND_CLOSE_WINDOW_MIN} poll_sec={WEEKEND_CLOSE_POLL_SEC}"
        )
    print(f"[FXAI] symbol: {SYMBOL}")

    # Initialize external dependencies once at startup
    if not init_runtime():
        print(f"[FXAI][FATAL] Runtime init failed: {_runtime_init_error}")
        raise SystemExit(1)

    # Port pre-check behavior:
    # - "warn"   (default): warn if port seems unavailable, but still attempt to start Flask
    # - "strict": exit before starting when port is unavailable
    # - "skip":  do not pre-check (useful when running behind a supervisor / reverse proxy)
    PORT_PRECHECK_MODE = os.getenv("PORT_PRECHECK_MODE", "warn").strip().lower()

    if PORT_PRECHECK_MODE != "skip":
        bind_err = _check_port_bindable("0.0.0.0", int(WEBHOOK_PORT))
        if bind_err:
            msg = f"[FXAI] WEBHOOK_PORT={WEBHOOK_PORT} may be unavailable. OS error: {bind_err}"
            if PORT_PRECHECK_MODE == "strict":
                print("[FXAI][FATAL] " + msg)
                print("[FXAI][HINT] On Windows, port 80 is commonly used/reserved by IIS/HTTP.SYS.")
                print("[FXAI][HINT] Check: `netstat -ano | findstr :80` and stop the conflicting process.")
                raise SystemExit(2)
            else:
                print("[FXAI][WARN] " + msg)
                print("[FXAI][WARN] Continuing to start Flask anyway (PORT_PRECHECK_MODE=warn).")

    try:
        app.run(host='0.0.0.0', port=WEBHOOK_PORT)
    except OSError as e:
        print(f"[FXAI][FATAL] Failed to start web server on 0.0.0.0:{WEBHOOK_PORT}. OS error: {e}")
        print("[FXAI][HINT] If you must use port 80, ensure nothing else is listening on it (IIS/Apache/Nginx/another Python process).")
        print("[FXAI][HINT] Windows: `netstat -ano | findstr :80` then `tasklist /fi \"PID eq <pid>\"`.")
        raise
