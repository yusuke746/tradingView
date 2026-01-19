import os
import json
import time
import socket
from datetime import datetime, timezone, timedelta
from threading import Lock, Thread
from typing import Optional, Dict, Any

import zmq
import MetaTrader5 as mt5
from flask import Flask, request
from openai import OpenAI
from dotenv import load_dotenv

# fxChartAI v2.6思想寄せ：
# - エントリーは「Q-Trendトリガー + 合流」をローカルで決定（AIにBUY/SELLをさせない）
# - AIは基本「管理（CLOSE/HOLD）」用途。BUY/SELLは無視。
# - v2.6相当の期待値改善（鮮度・重複送信）をPython側で実装
# - 日次損失/最大ポジなどのハードガードは MT5(EA) 側で最終判断

app = Flask(__name__)
load_dotenv()

# --- Basic settings ---
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "80"))
ZMQ_PORT = int(os.getenv("ZMQ_PORT", "5555"))
ZMQ_BIND = os.getenv("ZMQ_BIND", f"tcp://*:{ZMQ_PORT}")

# --- ZMQ Heartbeat (EA -> Python) ---
ZMQ_HEARTBEAT_ENABLED = os.getenv("ZMQ_HEARTBEAT_ENABLED", "true").lower() == "true"
ZMQ_HEARTBEAT_PORT = int(os.getenv("ZMQ_HEARTBEAT_PORT", "5556"))
ZMQ_HEARTBEAT_BIND = os.getenv("ZMQ_HEARTBEAT_BIND", f"tcp://*:{ZMQ_HEARTBEAT_PORT}")
ZMQ_HEARTBEAT_TIMEOUT_SEC = float(os.getenv("ZMQ_HEARTBEAT_TIMEOUT_SEC", "5.0"))

# Heartbeat stale policy
# - freeze: block both ENTRY and management (HOLD/CLOSE) when heartbeat is stale
HEARTBEAT_STALE_MODE = os.getenv("HEARTBEAT_STALE_MODE", "freeze").strip().lower()

# Optional: discretionary close before weekend
WEEKEND_CLOSE_ENABLED = os.getenv("WEEKEND_CLOSE_ENABLED", "false").lower() == "true"
WEEKEND_CLOSE_TZ = os.getenv("WEEKEND_CLOSE_TZ", "local").strip().lower()  # local|utc|broker
WEEKEND_CLOSE_WEEKDAY = int(os.getenv("WEEKEND_CLOSE_WEEKDAY", "4"))  # 0=Mon ... 4=Fri
WEEKEND_CLOSE_HOUR = int(os.getenv("WEEKEND_CLOSE_HOUR", "23"))
WEEKEND_CLOSE_MINUTE = int(os.getenv("WEEKEND_CLOSE_MINUTE", "50"))
WEEKEND_CLOSE_WINDOW_MIN = int(os.getenv("WEEKEND_CLOSE_WINDOW_MIN", "20"))
WEEKEND_CLOSE_POLL_SEC = float(os.getenv("WEEKEND_CLOSE_POLL_SEC", "5.0"))
SYMBOL = os.getenv("SYMBOL", "GOLD")

# --- Confluence params (Source of Truth: Q-Trend ±5 minutes) ---
CONFLUENCE_WINDOW_SEC = int(os.getenv("CONFLUENCE_WINDOW_SEC", "300"))
Q_TREND_MAX_AGE_SEC = int(os.getenv("Q_TREND_MAX_AGE_SEC", "300"))
# 旧EA(QTrendMinOtherAlerts)のデフォルトに合わせる: 別アラート2つでENTRY
MIN_OTHER_SIGNALS_FOR_ENTRY = int(os.getenv("MIN_OTHER_SIGNALS_FOR_ENTRY", "2"))

# --- Debug (optional) ---
CONFLUENCE_DEBUG = os.getenv("CONFLUENCE_DEBUG", "false").lower() == "true"
CONFLUENCE_DEBUG_MAX_LINES = int(os.getenv("CONFLUENCE_DEBUG_MAX_LINES", "80"))

# --- Signal retention / freshness ---
SIGNAL_LOOKBACK_SEC = int(os.getenv("SIGNAL_LOOKBACK_SEC", "1200"))     # 通常シグナル: 20分
SIGNAL_MAX_AGE_SEC = int(os.getenv("SIGNAL_MAX_AGE_SEC", "300"))
# Zone「存在/確定」情報は長く保持し、touch系イベントは短く保持する
ZONE_LOOKBACK_SEC = int(os.getenv("ZONE_LOOKBACK_SEC", "86400"))        # デフォルト24時間
ZONE_TOUCH_LOOKBACK_SEC = int(os.getenv("ZONE_TOUCH_LOOKBACK_SEC", str(SIGNAL_LOOKBACK_SEC)))

# Signal (Q-Trend/FVG/OSGFC/ZoneDetector etc) retention around Q-Trend anchor.
# New spec: keep signals within +/- 5 minutes of Q-Trend.
SIGNAL_QTREND_WINDOW_SEC = int(os.getenv("SIGNAL_QTREND_WINDOW_SEC", "300"))

# Cache file name
CACHE_FILE = os.getenv("CACHE_FILE", "signals_cache.json")

# --- Debug / behavior ---
FORCE_AI_CALL_WHEN_FLAT = os.getenv("FORCE_AI_CALL_WHEN_FLAT", "false").lower() == "true"
AI_THROTTLE_SEC = int(os.getenv("AI_THROTTLE_SEC", "10"))

# --- Webhook auth (optional) ---
# If set, require the token to match either header `X-Webhook-Token` or JSON field `token`.
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

# --- AI entry scoring (Source of Truth) ---
# - Call OpenAI ONLY when local confluence exists
# - Execute only if Confluence Score >= 70
AI_ENTRY_MIN_SCORE = max(1, min(100, int(os.getenv("AI_ENTRY_MIN_SCORE", "70"))))
AI_ENTRY_THROTTLE_SEC = float(os.getenv("AI_ENTRY_THROTTLE_SEC", "4.0"))
# on AI error: always safest = do not enter
AI_ENTRY_FALLBACK = "skip"
AI_ENTRY_DEFAULT_SCORE = int(os.getenv("AI_ENTRY_DEFAULT_SCORE", "50"))
AI_ENTRY_DEFAULT_LOT_MULTIPLIER = float(os.getenv("AI_ENTRY_DEFAULT_LOT_MULTIPLIER", "1.0"))

# Add-on (pyramiding) behavior
ALLOW_ADD_ON_ENTRIES = os.getenv("ALLOW_ADD_ON_ENTRIES", "true").lower() == "true"
ADDON_MAX_ENTRIES_PER_QTREND = max(1, int(os.getenv("ADDON_MAX_ENTRIES_PER_QTREND", "2")))

# --- AI close/hold management (Day Trading) ---
AI_CLOSE_ENABLED = os.getenv("AI_CLOSE_ENABLED", "true").lower() == "true"
AI_CLOSE_MIN_CONFIDENCE = max(0, min(100, int(os.getenv("AI_CLOSE_MIN_CONFIDENCE", "70"))))
# Default to a conservative cadence; allow immediate evaluation on clear reversal-like signals.
AI_CLOSE_THROTTLE_SEC = float(os.getenv("AI_CLOSE_THROTTLE_SEC", "60.0"))
# on AI error: "hold" (safest) or "default_close"
AI_CLOSE_FALLBACK = os.getenv("AI_CLOSE_FALLBACK", "hold").strip().lower()
AI_CLOSE_DEFAULT_CONFIDENCE = int(os.getenv("AI_CLOSE_DEFAULT_CONFIDENCE", "50"))

# If true: allow multiple entries for the same qtrend anchor time (advanced).
ALLOW_MULTI_ENTRY_SAME_QTREND = os.getenv("ALLOW_MULTI_ENTRY_SAME_QTREND", "false").lower() == "true"

# --- Webhook compatibility / safety ---
# If true, an alert that only contains action=BUY/SELL (without source) is treated as Q-Trend.
# Default false to avoid accidental Q-Trend triggers.
ASSUME_ACTION_IS_QTREND = os.getenv("ASSUME_ACTION_IS_QTREND", "false").lower() == "true"

# --- OpenAI ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", os.getenv("MODEL", "gpt-4o-mini"))
API_RETRY_COUNT = int(os.getenv("API_RETRY_COUNT", "3"))
API_RETRY_WAIT_SEC = float(os.getenv("API_RETRY_WAIT_SEC", "1.0"))
API_TIMEOUT_SEC = float(os.getenv("API_TIMEOUT_SEC", "7.0"))

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Entry scoring is mandatory under the new spec. If no OpenAI client is available,
# the safest behavior is to never enter (we still run the server for visibility).
if not client:
    print("[FXAI][WARN] OPENAI_API_KEY is missing. ENTRY scoring is disabled -> entries will be blocked (safety).")

if AI_CLOSE_ENABLED and not client:
    print("[FXAI][WARN] AI_CLOSE_ENABLED=true but OPENAI_API_KEY is missing. Disabling AI close/hold management.")
    AI_CLOSE_ENABLED = False

# --- ZMQ ---
context = zmq.Context.instance()
zmq_socket = context.socket(zmq.PUSH)
zmq_socket.bind(ZMQ_BIND)

# --- MT5 ---
if not mt5.initialize():
    raise SystemExit("MT5初期化失敗")

signals_lock = Lock()
signals_cache = []  # list[dict]

# Market-data fallbacks (avoid ORDER blocked by atr=0)
_last_atr_by_symbol: Dict[str, float] = {}


def _extract_symbol_from_webhook(data: dict) -> str:
    """TradingView payload variations -> MT5 symbol.

    Accept keys like symbol/ticker and strip exchange prefixes (e.g. "OANDA:XAUUSD").
    """
    raw = None
    for k in ("symbol", "ticker", "tv_symbol", "instrument", "pair"):
        v = data.get(k)
        if isinstance(v, str) and v.strip():
            raw = v.strip()
            break

    if not raw:
        return SYMBOL

    # OANDA:XAUUSD / FX:GBPUSD
    if ":" in raw:
        raw = raw.split(":")[-1].strip()

    return raw.upper()


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
_executed_qtrend_times_by_symbol: Dict[str, list] = {}
_executed_qtrend_entry_counts_by_symbol: Dict[str, Dict[float, int]] = {}
EXECUTED_QTREND_RETENTION_SEC = int(os.getenv("EXECUTED_QTREND_RETENTION_SEC", "86400"))

_last_ai_attempt_key = None
_last_ai_attempt_at = 0.0

_last_close_attempt_key = None
_last_close_attempt_at = 0.0

_last_entry_ai_key = None
_last_entry_ai_at = 0.0


def _qtime_equal(a: float, b: float, eps: float = 1e-4) -> bool:
    try:
        return abs(float(a) - float(b)) < float(eps)
    except Exception:
        return False


def _was_qtrend_executed(symbol: str, q_time: float) -> bool:
    m = _executed_qtrend_entry_counts_by_symbol.get(symbol, {})
    for t, c in m.items():
        if c > 0 and _qtime_equal(t, q_time):
            return True
    return False


def _qtrend_entry_count(symbol: str, q_time: float) -> int:
    m = _executed_qtrend_entry_counts_by_symbol.get(symbol, {})
    for t, c in m.items():
        if _qtime_equal(t, q_time):
            return int(c or 0)
    return 0


def _mark_qtrend_executed(symbol: str, q_time: float) -> None:
    now = time.time()
    arr = _executed_qtrend_times_by_symbol.get(symbol, [])

    # prune old
    keep = []
    for t in arr:
        try:
            if (now - float(t)) <= float(EXECUTED_QTREND_RETENTION_SEC):
                keep.append(float(t))
        except Exception:
            continue

    if not any(_qtime_equal(t, q_time) for t in keep):
        keep.append(float(q_time))

    _executed_qtrend_times_by_symbol[symbol] = keep

    # maintain per-qtrend entry counts
    counts = _executed_qtrend_entry_counts_by_symbol.get(symbol, {})
    # prune counts that are no longer in keep
    keep_set = set()
    for t in keep:
        try:
            keep_set.add(float(t))
        except Exception:
            continue
    pruned = {}
    for t, c in counts.items():
        try:
            tt = float(t)
            if any(_qtime_equal(tt, k) for k in keep_set):
                pruned[tt] = int(c or 0)
        except Exception:
            continue

    # increment this q_time
    qt = float(q_time)
    found_key = None
    for t in pruned.keys():
        if _qtime_equal(t, qt):
            found_key = t
            break
    if found_key is None:
        found_key = qt
    pruned[found_key] = int(pruned.get(found_key, 0) or 0) + 1
    _executed_qtrend_entry_counts_by_symbol[symbol] = pruned


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
    # Q-Trend: Strong/Normal を source として完全分離する
    if src_lower in {"q-trend", "qtrend", "qtrendnormal", "q-trendnormal", "qtrend-normal", "q-trend-normal", "qtrendstrong", "q-trendstrong", "qtrend-strong", "q-trend-strong"}:
        is_strong = ("strong" in src_lower) or (strength == "strong")
        s["source"] = "Q-Trend-Strong" if is_strong else "Q-Trend-Normal"
        s["strength"] = "strong" if is_strong else "normal"
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

    # Collect Q-Trend anchors currently in cache (for Signal retention rule).
    qtrend_times = []
    for s in signals_cache:
        try:
            src = (s.get("source") or "").strip()
            side = (s.get("side") or "").strip().lower()
            if src in {"Q-Trend-Strong", "Q-Trend-Normal"} and side in {"buy", "sell"}:
                st = float(s.get("signal_time") or s.get("receive_time") or 0.0)
                if st > 0:
                    qtrend_times.append(st)
        except Exception:
            continue

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

        # Non-Zones: apply stricter Signal retention around Q-Trend.
        src_raw2 = (s.get("source") or "").strip()
        side2 = (s.get("side") or "").strip().lower()
        is_qtrend = (src_raw2 in {"Q-Trend-Strong", "Q-Trend-Normal"} and side2 in {"buy", "sell"})

        if is_qtrend:
            # Keep Q-Trend itself for a short period so add-on evidence can arrive.
            limit_sec = SIGNAL_LOOKBACK_SEC
            if age < float(limit_sec or SIGNAL_LOOKBACK_SEC):
                keep_list.append(s)
            continue

        # Other signals: keep only if they are within +/-5min of any cached Q-Trend.
        st = float(s.get("signal_time") or s.get("receive_time") or 0.0)
        if st <= 0:
            continue

        # Hard cap to avoid unbounded growth.
        if age >= float(SIGNAL_LOOKBACK_SEC or 1200):
            continue

        window = float(SIGNAL_QTREND_WINDOW_SEC or 300)
        if qtrend_times:
            keep = any(abs(st - qt) <= window for qt in qtrend_times)
            if keep:
                keep_list.append(s)
        else:
            # No Q-Trend anchor yet: keep briefly to allow pre-window evidence.
            if (now - st) <= window:
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
        if s.get("source") in {"Q-Trend-Strong", "Q-Trend-Normal"} and s.get("side") in {"buy", "sell"}:
            st = float(s.get("signal_time") or s.get("receive_time") or 0.0)
            is_strong = (s.get("source") == "Q-Trend-Strong") or (s.get("strength") == "strong")
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
    q_is_strong = (q_source == "Q-Trend-Strong") or (latest_q.get("strength") == "strong")
    q_trigger_type = "Strong" if q_is_strong else "Normal"
    momentum_factor = 1.5 if q_is_strong else 1.0
    is_strong_momentum = bool(q_is_strong)

    confirm_unique_sources = set([q_source or "Q-Trend-Normal"])
    opp_unique_sources = set([q_source or "Q-Trend-Normal"])
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
        if src in {"Q-Trend-Strong", "Q-Trend-Normal"}:
            # 同一トレンド内で Strong が追加で来るケースを拾う（後追い含む）
            if src == "Q-Trend-Strong":
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


def _build_entry_logic_prompt(symbol: str, market: dict, stats: dict, action: str) -> str:
    """Q-Trend発生時の環境を渡し、AIに Confluence Score(1-100) と Lot Multiplier を出させる。"""

    if not stats:
        minimal_payload = {
            "symbol": symbol,
            "proposed_action": action,
            "market": {
                "bid": market.get("bid"),
                "ask": market.get("ask"),
                "m15_sma20": market.get("m15_ma"),
                "atr_m5_approx": market.get("atr"),
                "spread_points": market.get("spread"),
            },
            "note": "No Q-Trend stats available yet. Score conservatively.",
        }
        return (
            "You are a strict confluence scoring engine for algorithmic trading.\n"
            "Return ONLY strict JSON with this schema:\n"
            '{"confluence_score": 1-100, "lot_multiplier": 0.5-2.0, "reason": "short"}\n\n'
            "ContextJSON:\n"
            + json.dumps(minimal_payload, ensure_ascii=False)
        )

    base = _build_entry_filter_prompt(symbol, market, stats, action)
    return (
        "You are a strict confluence scoring engine for algorithmic trading.\n"
        "Given the technical context, output ONLY strict JSON with this schema:\n"
        '{"confluence_score": 1-100, "lot_multiplier": 0.5-2.0, "reason": "short"}\n\n'
        "--- CONTEXT BELOW (do not output it) ---\n"
        + base
    )


def _build_entry_filter_prompt(symbol: str, market: dict, stats: dict, action: str) -> str:
    """ENTRY最終判断のためのコンテキストを構築。

    - BUY/SELL自体はローカルで確定済み（action）。
    - AIは action/confidence/multiplier のみを返す。
    """
    now = time.time()
    stats = stats or {}

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

    q_trigger_type = (stats.get("q_trigger_type") or ("Strong" if bool(stats.get("q_is_strong")) else "Normal"))
    is_strong_momentum = bool(stats.get("is_strong_momentum"))
    momentum_factor = float(stats.get("momentum_factor") or (1.5 if is_strong_momentum else 1.0))
    strong_bonus = 2 if is_strong_momentum else 0

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
        "qtrend": {
            "side": stats.get("q_side"),
            "age_sec": q_age_sec,
            "is_strong": bool(stats.get("q_is_strong")),
            "is_strong_momentum": is_strong_momentum,
            "trigger_type": q_trigger_type,
            "momentum_factor": momentum_factor,
            "source": stats.get("q_source"),
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
        "You MUST NOT suggest BUY/SELL; the proposed_action is already decided locally.\n"
        "Use these decision principles:\n"
        "- Prefer ALIGNED higher-timeframe trend_alignment. HOWEVER, even if MISALIGNED, you MAY approve ENTRY if qtrend.is_strong_momentum is true (viewed as high-reward mean-reversion or a new trend starting point).\n"
        "- Prioritize current price action and momentum over simple MA position.\n"
        "- Prefer stronger confluence: higher confirm_unique_sources, higher weighted_confirm_score.\n"
        "- Evaluate opposition with nuance: confirmed/structural opposition matters most; touch-based opposition can be noise.\n"
        "- If qtrend.trigger_type is Strong (strong momentum), treat it as higher breakout probability: be more willing to approve ENTRY even if there are some opposite touch signals (e.g., opposite FVG touch), as long as EV/space (ATR vs spread) remains attractive.\n"
        "- If some opposite FVG/Zones exist BUT trend is aligned and ATR-to-spread is healthy and confluence is decent, you MAY still approve ENTRY (EV can remain positive).\n"
        "- If structural Zones context exists (zones_confirmed_recent > 0) and confluence is weak, be conservative unless other evidence strongly improves EV.\n"
        "- Penalize wide spread.\n"
        "Return ONLY strict JSON schema:\n"
        '{"confluence_score": 1-100, "lot_multiplier": 0.5-2.0, "reason": "short"}\n\n'
        "ContextJSON:\n"
        + json.dumps(payload, ensure_ascii=False)
    )


def _attempt_entry_from_stats(
    symbol: str,
    stats: dict,
    normalized_trigger: dict,
    now: float,
    pos_summary: Optional[dict] = None,
) -> tuple[str, int]:
    """Try to place an order based on latest Q-Trend stats.

    Returns (message, http_status).
    """
    if bool((stats or {}).get("cancel_due_to_opposite_bar_close")):
        detail = (stats or {}).get("cancel_detail") or {}
        print(f"[FXAI][ENTRY] Cancel Q-Trend due to opposite bar_close: {detail}")
        _set_status(last_result="Canceled by opposite bar_close", last_result_at=time.time())
        return "Canceled by opposite bar_close", 200

    if not stats:
        _set_status(last_result="Stored (no qtrend stats)", last_result_at=time.time())
        return "Stored", 200

    q_time = float(stats.get("q_time") or now)
    q_age_sec = int(now - q_time)

    if Q_TREND_MAX_AGE_SEC > 0 and q_age_sec > Q_TREND_MAX_AGE_SEC:
        _set_status(last_result="Stale Q-Trend", last_result_at=time.time())
        return "Stale Q-Trend", 200

    # strict duplicate / add-on control per q_time
    entry_count = _qtrend_entry_count(symbol, q_time)
    positions_open = int((pos_summary or {}).get("positions_open") or 0)
    net_side = ((pos_summary or {}).get("net_side") or "flat").lower()

    # Determine max entries allowed for this q_time
    max_entries = 1
    if positions_open > 0 and ALLOW_ADD_ON_ENTRIES and net_side in {"buy", "sell"}:
        # add-on only for same direction
        if net_side == ((stats.get("q_side") or "").lower()):
            max_entries = int(ADDON_MAX_ENTRIES_PER_QTREND or 2)

    if ALLOW_MULTI_ENTRY_SAME_QTREND:
        # still keep a sane upper bound if configured; default is handled above
        max_entries = max(max_entries, int(ADDON_MAX_ENTRIES_PER_QTREND or 2))

    if entry_count >= int(max_entries):
        _set_status(last_result="Duplicate", last_result_at=time.time())
        return "Duplicate", 200

    # confluence requirement (local)
    have = int(stats.get("confirm_unique_sources") or 0)
    if have < MIN_OTHER_SIGNALS_FOR_ENTRY:
        print(f"[FXAI][ENTRY] Waiting confluence: have={have} need={MIN_OTHER_SIGNALS_FOR_ENTRY} q_time={stats.get('q_time')} q_side={stats.get('q_side')}")
        _set_status(last_result="Waiting confluence", last_result_at=time.time())
        return "Waiting confluence", 200

    q_side = (stats.get("q_side") or "").lower()
    action = "BUY" if q_side == "buy" else "SELL" if q_side == "sell" else ""
    if action not in {"BUY", "SELL"}:
        _set_status(last_result="Invalid action", last_result_at=time.time())
        return "Invalid action", 400

    # Safety: block entries when EA heartbeat is stale/missing.
    if not _heartbeat_is_fresh(now_ts=now):
        _set_status(last_result="Blocked by heartbeat", last_result_at=time.time())
        return "Blocked by heartbeat", 503

    market = get_mt5_market_data(symbol)
    try:
        if float(market.get("atr") or 0.0) > 0:
            _last_atr_by_symbol[symbol] = float(market.get("atr") or 0.0)
    except Exception:
        pass
    local_multiplier = _compute_entry_multiplier(
        int(stats.get("confirm_unique_sources") or 0),
        bool(stats.get("strong_after_q")),
    )

    final_multiplier = float(local_multiplier)
    ai_score = None
    ai_reason = ""

    # AI scoring is mandatory once local confluence exists.
    # Throttle AI calls but allow re-eval when NEW supporting signal arrives.
    global _last_ai_attempt_key, _last_ai_attempt_at
    trig_src = (normalized_trigger.get("source") or "")
    trig_evt = (normalized_trigger.get("event") or "")
    trig_st = float(normalized_trigger.get("signal_time") or normalized_trigger.get("receive_time") or now)
    attempt_key = f"{symbol}:{action}:{q_time:.3f}:{trig_src}:{trig_evt}:{trig_st:.3f}"

    now_mono = time.time()
    if _last_ai_attempt_key == attempt_key and (now_mono - float(_last_ai_attempt_at or 0.0)) < AI_ENTRY_THROTTLE_SEC:
        _set_status(last_result="AI throttled", last_result_at=time.time())
        return "AI throttled", 200
    _last_ai_attempt_key = attempt_key
    _last_ai_attempt_at = now_mono

    ai_decision = _ai_entry_score(symbol, market, stats, action)
    if not ai_decision:
        # Safety: do not enter on AI error.
        _set_status(last_result="Blocked by AI (no score)", last_result_at=time.time())
        return "Blocked by AI", 503

    ai_score = int(ai_decision.get("confluence_score") or 0)
    ai_reason = ai_decision.get("reason") or ""
    lot_mult = float(ai_decision.get("lot_multiplier") or 1.0)

    if ai_score < AI_ENTRY_MIN_SCORE:
        _set_status(last_result="Blocked by AI", last_result_at=time.time())
        return "Blocked by AI", 403

    final_multiplier = float(_clamp(local_multiplier * lot_mult, 0.5, 2.0))

    # Re-check heartbeat just before sending the order (avoid race).
    if not _heartbeat_is_fresh(now_ts=time.time()):
        _set_status(last_result="Blocked by heartbeat", last_result_at=time.time())
        return "Blocked by heartbeat", 503

    # ZMQでエントリー指示を送信
    reason = ai_reason or f"qtrend_entry other_sources={int(stats.get('confirm_unique_sources') or 0)}"
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
    zmq_socket.send_json(
        {
            **payload,
        }
    )

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
            "q_time": q_time,
            "trigger": {
                "source": normalized_trigger.get("source"),
                "event": normalized_trigger.get("event"),
                "signal_time": normalized_trigger.get("signal_time"),
            },
        },
    )

    _mark_qtrend_executed(symbol, q_time)
    print(f"[FXAI][ZMQ] Order sent: {payload}")
    return "OK", 200


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


def _ai_entry_score(symbol: str, market: dict, stats: dict, action: str) -> Optional[Dict[str, Any]]:
    """AIに Confluence Score(1-100) と Lot Multiplier を出させる。"""
    if not client:
        return None
    prompt = _build_entry_logic_prompt(symbol, market, stats, action)
    decision = _call_openai_with_retry(prompt)
    if not decision:
        print("[FXAI][AI] No response from AI (entry score).")
        return None

    validated = _validate_ai_entry_score(decision)
    if not validated:
        print("[FXAI][AI] Invalid AI response (entry score).")
        return None

    return validated


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
        source_for_cache = "Q-Trend-Normal"

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

    # Determine Q-Trend trigger early (used for both management and optional add-on entries)
    qtrend_trigger = (normalized.get("source") in {"Q-Trend-Strong", "Q-Trend-Normal"} and normalized.get("side") in {"buy", "sell"})

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
                    (src in {"Q-Trend-Strong", "Q-Trend-Normal"})
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

    # --- ENTRY MODE (Q-Trend reservation + follow-up entry) ---
    stats = get_qtrend_anchor_stats(symbol)
    if not stats:
        _set_status(last_result="Stored", last_result_at=time.time())
        return "Stored", 200

    # Debug summary for entry decision
    try:
        print(
            "[FXAI][ENTRY] stats "
            + json.dumps(
                {
                    "q_time": stats.get("q_time"),
                    "q_side": stats.get("q_side"),
                    "q_source": stats.get("q_source"),
                    "q_age_sec": int(now - float(stats.get("q_time") or now)),
                    "confirm_unique_sources": stats.get("confirm_unique_sources"),
                    "confirm_signals": stats.get("confirm_signals"),
                    "opp_unique_sources": stats.get("opp_unique_sources"),
                    "opp_signals": stats.get("opp_signals"),
                    "cancel_due_to_opposite_bar_close": stats.get("cancel_due_to_opposite_bar_close"),
                },
                ensure_ascii=False,
            )
        )
    except Exception:
        pass

    q_time = float(stats.get("q_time") or now)
    q_age_sec = int(now - q_time)
    has_pending_qtrend = (Q_TREND_MAX_AGE_SEC <= 0 or q_age_sec <= Q_TREND_MAX_AGE_SEC) and (
        ALLOW_MULTI_ENTRY_SAME_QTREND or (not _was_qtrend_executed(symbol, q_time))
    )

    # Trigger conditions:
    # - Q-Trend itself
    # - or any supporting signal (Zones/FVG/OSGFC) arriving while a pending Q-Trend exists
    src = (normalized.get("source") or "")
    supporting_sources = {"FVG", "Zones", "OSGFC"}

    if qtrend_trigger:
        return _attempt_entry_from_stats(symbol, stats, normalized, now, pos_summary)

    if has_pending_qtrend and src in supporting_sources:
        # this is the key upgrade: immediate re-eval on new evidence within Q-Trend TTL
        return _attempt_entry_from_stats(symbol, stats, normalized, now, pos_summary)

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
    # ★追加: 起動時にキャッシュを復元
    _load_cache()

    if ZMQ_HEARTBEAT_ENABLED:
        Thread(target=_heartbeat_receiver_loop, daemon=True).start()
    if WEEKEND_CLOSE_ENABLED:
        Thread(target=_weekend_close_loop, daemon=True).start()

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
