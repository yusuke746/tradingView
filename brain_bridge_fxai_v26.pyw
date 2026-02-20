import os
import json
import time
import socket
import statistics
from datetime import datetime, timezone, timedelta
import math
from threading import Lock, Thread
from typing import Optional, Dict, Any, List

import zmq
import MetaTrader5 as mt5
from flask import Flask, request, Response
from openai import OpenAI
from dotenv import load_dotenv

# Import path robustness: allow running as `python tradingView\brain_bridge_fxai_v26.pyw`
# (script-dir on sys.path) and also via package-style execution.
try:
    from fxai_hardening import (
        env_bool as _env_bool,
        env_int as _env_int,
        get_client_ip as _get_client_ip,
        rate_limit_allow as _rate_limit_allow,
        request_is_https as _request_is_https,
        sanitize_untrusted_text as _sanitize_untrusted_text,
    )
except Exception:
    from tradingView.fxai_hardening import (
        env_bool as _env_bool,
        env_int as _env_int,
        get_client_ip as _get_client_ip,
        rate_limit_allow as _rate_limit_allow,
        request_is_https as _request_is_https,
        sanitize_untrusted_text as _sanitize_untrusted_text,
    )

# Extracted modules (keep robust import paths)
try:
    from fxai_ai_client import call_openai_json_with_retry as _call_openai_json_with_retry
except Exception:
    from tradingView.fxai_ai_client import call_openai_json_with_retry as _call_openai_json_with_retry

try:
    from fxai_zmq_bridge import send_json_with_hooks as _zmq_send_json_with_hooks
except Exception:
    from tradingView.fxai_zmq_bridge import send_json_with_hooks as _zmq_send_json_with_hooks

try:
    import fxai_metrics as _fxai_metrics
except Exception:
    from tradingView import fxai_metrics as _fxai_metrics

try:
    import fxai_signal_cache as _fxai_signal_cache
    from fxai_signal_cache import is_zone_presence_signal
except Exception:
    from tradingView import fxai_signal_cache as _fxai_signal_cache
    from tradingView.fxai_signal_cache import is_zone_presence_signal

try:
    import fxai_persistence as _fxai_persist
except Exception:
    from tradingView import fxai_persistence as _fxai_persist

try:
    import fxai_flush as _fxai_flush
except Exception:
    from tradingView import fxai_flush as _fxai_flush

try:
    import fxai_qtrend as _fxai_qtrend
except Exception:
    from tradingView import fxai_qtrend as _fxai_qtrend

try:
    import fxai_window_signals as _fxai_window_signals
except Exception:
    from tradingView import fxai_window_signals as _fxai_window_signals

try:
    import fxai_recent_context as _fxai_recent_context
except Exception:
    from tradingView import fxai_recent_context as _fxai_recent_context

try:
    import fxai_entry_payload as _fxai_entry_payload
except Exception:
    from tradingView import fxai_entry_payload as _fxai_entry_payload

try:
    import fxai_close_payload as _fxai_close_payload
except Exception:
    from tradingView import fxai_close_payload as _fxai_close_payload

try:
    import fxai_prompts_text as _fxai_prompts_text
except Exception:
    from tradingView import fxai_prompts_text as _fxai_prompts_text

try:
    from werkzeug.exceptions import RequestEntityTooLarge
except Exception:
    RequestEntityTooLarge = None  # type: ignore

app = Flask(__name__)

# Load .env early
load_dotenv()


# --- Core config ---
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "80"))
WEBHOOK_TOKEN = str(os.getenv("WEBHOOK_TOKEN", "") or "").strip()

# Request size guard (mitigate accidental/hostile large payloads)
MAX_REQUEST_BYTES = _env_int("MAX_REQUEST_BYTES", "262144")  # default 256KB
if int(MAX_REQUEST_BYTES or 0) > 0:
    try:
        app.config["MAX_CONTENT_LENGTH"] = int(MAX_REQUEST_BYTES)
    except Exception:
        pass

# Auth hardening: avoid sending token in request body by default? (keep legacy behavior unless disabled)
ALLOW_BODY_TOKEN_AUTH = _env_bool("ALLOW_BODY_TOKEN_AUTH", "1")

# --- Basic API hardening (disabled by default to avoid breaking local setups) ---
REQUIRE_HTTPS = _env_bool("REQUIRE_HTTPS", "0")
WEBHOOK_RATE_LIMIT_RPM = _env_int("WEBHOOK_RATE_LIMIT_RPM", "0")
STATUS_RATE_LIMIT_RPM = _env_int("STATUS_RATE_LIMIT_RPM", "0")
METRICS_RATE_LIMIT_RPM = _env_int("METRICS_RATE_LIMIT_RPM", "0")

# Prompt payload compaction (OFF by default to avoid changing behavior)
PROMPT_COMPACT_ENABLED = _env_bool("PROMPT_COMPACT_ENABLED", "0")
PROMPT_MAX_LIST_ITEMS = int(os.getenv("PROMPT_MAX_LIST_ITEMS", "20"))
PROMPT_MAX_STR_LEN = int(os.getenv("PROMPT_MAX_STR_LEN", "600"))

SYMBOL = (os.getenv("SYMBOL", "GOLD") or "GOLD").strip().upper()

# Optional: normalize/alias incoming symbols to a canonical MT5 symbol.
# Example: SYMBOL_ALIASES="XAUUSD=GOLD,XAUUSDm=GOLD,OANDA:XAUUSD=GOLD"
SYMBOL_ALIASES_RAW = str(os.getenv("SYMBOL_ALIASES", "") or "").strip()


def _parse_symbol_aliases(raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    s = (raw or "").strip()
    if not s:
        return out
    # Split by comma/semicolon/newline
    parts: List[str] = []
    for chunk in s.replace("\n", ",").replace(";", ",").split(","):
        chunk = (chunk or "").strip()
        if chunk:
            parts.append(chunk)
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
        elif ":" in p:
            k, v = p.split(":", 1)
        else:
            continue
        k = (k or "").strip().upper()
        v = (v or "").strip().upper()
        if not k or not v:
            continue
        # Allow broker prefixes in keys like OANDA:XAUUSD
        if ":" in k:
            k = k.split(":")[-1].strip().upper()
        out[k] = v
    return out


SYMBOL_ALIASES = _parse_symbol_aliases(SYMBOL_ALIASES_RAW)

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
FVG_LOOKBACK_SEC = int(os.getenv("FVG_LOOKBACK_SEC", "1200"))

# Zombie signal TTL (hard prune): signals older than this are removed from cache
ZOMBIE_SIGNAL_TTL_SEC = int(os.getenv("ZOMBIE_SIGNAL_TTL_SEC", "1000"))

CACHE_ASYNC_FLUSH_ENABLED = _env_bool("CACHE_ASYNC_FLUSH_ENABLED", "3")
CACHE_FLUSH_INTERVAL_SEC = float(os.getenv("CACHE_FLUSH_INTERVAL_SEC", "5.0"))
# Force flush even if signals keep arriving frequently
CACHE_FLUSH_FORCE_SEC = float(os.getenv("CACHE_FLUSH_FORCE_SEC", "10.0"))

CONFLUENCE_LOOKBACK_SEC = int(os.getenv("CONFLUENCE_LOOKBACK_SEC", "600"))
Q_TREND_MAX_AGE_SEC = int(os.getenv("Q_TREND_MAX_AGE_SEC", "300"))
Q_TREND_TF_FALLBACK_ENABLED = _env_bool("Q_TREND_TF_FALLBACK_ENABLED", "0")

ZONE_LOOKBACK_SEC = int(os.getenv("ZONE_LOOKBACK_SEC", "1200"))
ZONE_TOUCH_LOOKBACK_SEC = int(os.getenv("ZONE_TOUCH_LOOKBACK_SEC", "1200"))

# Signal cache indexing (OFF by default to avoid any behavior risk)
SIGNAL_INDEX_ENABLED = _env_bool("SIGNAL_INDEX_ENABLED", "0")
SIGNAL_INDEX_BUCKET_SEC = int(os.getenv("SIGNAL_INDEX_BUCKET_SEC", "60"))

# Position time extraction debug (temporarily ON for diagnostics)
DEBUG_POSITION_TIME = _env_bool("DEBUG_POSITION_TIME", "1")

# Spread history (points) for stable average spread estimation
SPREAD_HISTORY_WINDOW_SEC = int(os.getenv("SPREAD_HISTORY_WINDOW_SEC", "86400"))
SPREAD_HISTORY_MAX = int(os.getenv("SPREAD_HISTORY_MAX", "2000"))


# --- Metrics / log aggregation (for fast tuning) ---
ENTRY_METRICS_ENABLED = _env_bool("ENTRY_METRICS_ENABLED", "1")
METRICS_FILE = str(os.getenv("METRICS_FILE", "entry_metrics.json") or "entry_metrics.json").strip()
METRICS_KEEP_DAYS = int(os.getenv("METRICS_KEEP_DAYS", "14"))
METRICS_MAX_EXAMPLES = int(os.getenv("METRICS_MAX_EXAMPLES", "80"))

# --- Auto-tuning (rolling) ---
AUTO_TUNE_ENABLED = _env_bool("AUTO_TUNE_ENABLED", "1")
AUTO_TUNE_INTERVAL_SEC = float(os.getenv("AUTO_TUNE_INTERVAL_SEC", "86400"))
AUTO_TUNE_PCTL = float(os.getenv("AUTO_TUNE_PCTL", "0.98"))
AUTO_TUNE_MIN_SAMPLES = int(os.getenv("AUTO_TUNE_MIN_SAMPLES", "80"))
AUTO_TUNE_ENV_PATH = str(os.getenv("AUTO_TUNE_ENV_PATH", ".env") or ".env").strip()
AUTO_TUNE_WRITE_ENV = _env_bool("AUTO_TUNE_WRITE_ENV", "1")
SPREAD_MAX_ATR_RATIO_MIN = float(os.getenv("SPREAD_MAX_ATR_RATIO_MIN", "0.03"))
SPREAD_MAX_ATR_RATIO_MAX = float(os.getenv("SPREAD_MAX_ATR_RATIO_MAX", "0.30"))
DRIFT_LIMIT_ATR_MULT_MIN = float(os.getenv("DRIFT_LIMIT_ATR_MULT_MIN", "0.05"))
DRIFT_LIMIT_ATR_MULT_MAX = float(os.getenv("DRIFT_LIMIT_ATR_MULT_MAX", "0.35"))


# --- Behavior toggles ---
ASSUME_ACTION_IS_QTREND = _env_bool("ASSUME_ACTION_IS_QTREND", "0")
ALLOW_ADD_ON_ENTRIES = _env_bool("ALLOW_ADD_ON_ENTRIES", "1")
ALLOW_MULTI_ENTRY_SAME_QTREND = _env_bool("ALLOW_MULTI_ENTRY_SAME_QTREND", "0")
ADDON_MAX_ENTRIES_PER_QTREND = int(os.getenv("ADDON_MAX_ENTRIES_PER_QTREND", "5"))
ADDON_MAX_ENTRIES_PER_POSITION = int(os.getenv("ADDON_MAX_ENTRIES_PER_POSITION", "5"))

# Phase control: DEVELOPMENT (Phase 1) duration limits for day trading.
# Default: 30 minutes (1800s). Positions that fail to reach breakeven by this time
# are transitioned to Phase 2 for stricter exit evaluation.
MAX_DEVELOPMENT_SEC = float(os.getenv("MAX_DEVELOPMENT_SEC", "1800"))
# Breakeven band multipliers: tighter bands = faster Phase 2 transition.
BREAKEVEN_BAND_ATR_MULTIPLIER = float(os.getenv("BREAKEVEN_BAND_ATR_MULTIPLIER", "0.10"))
BREAKEVEN_BAND_SPREAD_MULTIPLIER = float(os.getenv("BREAKEVEN_BAND_SPREAD_MULTIPLIER", "1.5"))


# --- AI config ---
OPENAI_API_KEY = str(os.getenv("OPENAI_API_KEY", "") or "").strip()
MODEL = str(os.getenv("OPENAI_MODEL", os.getenv("MODEL", "gpt-4o-mini")) or "gpt-4o-mini").strip()
OPENAI_MODEL = MODEL

API_TIMEOUT_SEC = float(os.getenv("API_TIMEOUT_SEC", "20"))
API_RETRY_COUNT = int(os.getenv("API_RETRY_COUNT", "3"))
API_RETRY_WAIT_SEC = float(os.getenv("API_RETRY_WAIT_SEC", "1.5"))

AI_ENTRY_DEFAULT_SCORE = int(os.getenv("AI_ENTRY_DEFAULT_SCORE", "50"))
AI_ENTRY_DEFAULT_LOT_MULTIPLIER = float(os.getenv("AI_ENTRY_DEFAULT_LOT_MULTIPLIER", "1.0"))
AI_ENTRY_MIN_SCORE = int(os.getenv("AI_ENTRY_MIN_SCORE", "75"))
# Optional: allow a lower minimum score only when Q-Trend is Strong AND aligned with Lorentzian.
# Default keeps behavior unchanged.
AI_ENTRY_MIN_SCORE_STRONG_ALIGNED = int(os.getenv("AI_ENTRY_MIN_SCORE_STRONG_ALIGNED", str(AI_ENTRY_MIN_SCORE)))
AI_ENTRY_THROTTLE_SEC = float(os.getenv("AI_ENTRY_THROTTLE_SEC", "15"))
ADDON_MIN_AI_SCORE = int(os.getenv("ADDON_MIN_AI_SCORE", str(AI_ENTRY_MIN_SCORE)))

# /status observability: keep last N entry outcomes (ring buffer)
ENTRY_STATUS_HISTORY_MAX = int(os.getenv("ENTRY_STATUS_HISTORY_MAX", "30"))

# /status observability: keep last N management outcomes (ring buffer)
MGMT_STATUS_HISTORY_MAX = int(os.getenv("MGMT_STATUS_HISTORY_MAX", "50"))

# Pending management: keep last N signals gathered during settle window
MGMT_PENDING_SIGNAL_HISTORY_MAX = int(os.getenv("MGMT_PENDING_SIGNAL_HISTORY_MAX", "12"))

MIN_OTHER_SIGNALS_FOR_ENTRY = int(os.getenv("MIN_OTHER_SIGNALS_FOR_ENTRY", "1"))

CONFLUENCE_WINDOW_SEC = int(os.getenv("CONFLUENCE_WINDOW_SEC", "600"))
CONFLUENCE_DEBUG = _env_bool("CONFLUENCE_DEBUG", "0")
CONFLUENCE_DEBUG_MAX_LINES = int(os.getenv("CONFLUENCE_DEBUG_MAX_LINES", "80"))

# --- Post-trigger settle window (simple) ---
# After receiving a Lorentzian entry_trigger, wait a short time to let near-immediate
# context signals (e.g., Q-Trend on bar close) arrive and be included in ContextJSON.
# Keep this small (e.g., 5-30s). Set 0 to disable.
POST_TRIGGER_WAIT_SEC = float(os.getenv("POST_TRIGGER_WAIT_SEC", "0"))

# --- Entry signal aggregation window (async) ---
# When a Lorentzian entry_trigger arrives, do NOT evaluate immediately; instead, wait a short
# time to let out-of-order context (Zones/FVG/Q-Trend) arrive, then evaluate once.
# This reduces "early entry" risk when TradingView alerts arrive slightly delayed.
ENTRY_POST_SIGNAL_WAIT_SEC = float(os.getenv("ENTRY_POST_SIGNAL_WAIT_SEC", "3"))
ENTRY_POST_SIGNAL_MAX_WAIT_SEC = float(os.getenv("ENTRY_POST_SIGNAL_MAX_WAIT_SEC", str(ENTRY_POST_SIGNAL_WAIT_SEC)))

# --- Delayed entry re-evaluation (minutes-level) ---
# If a Lorentzian trigger is blocked by AI due to missing/weak evidence,
# re-try the same trigger when supportive signals arrive later (e.g., Q-Trend / Zones / FVG).
# This keeps logic simple: we re-run the same entry prompt with fresher context.
DELAYED_ENTRY_ENABLED = _env_bool("DELAYED_ENTRY_ENABLED", "1")
# Maximum time to keep a pending Lorentzian trigger.
# Default: 5 minutes (300s) to prevent stale triggers from persisting.
DELAYED_ENTRY_MAX_WAIT_SEC = float(os.getenv("DELAYED_ENTRY_MAX_WAIT_SEC", "300"))
# Hard TTL: absolute maximum age for any Lorentzian trigger (prevents zombie signals).
# Default: 15 minutes (900s) = 3x DELAYED_ENTRY_MAX_WAIT_SEC.
DELAYED_ENTRY_HARD_TTL_SEC = float(os.getenv("DELAYED_ENTRY_HARD_TTL_SEC", "900"))
# Minimum interval between AI re-evaluations for the same pending trigger.
DELAYED_ENTRY_MIN_RETRY_INTERVAL_SEC = float(os.getenv("DELAYED_ENTRY_MIN_RETRY_INTERVAL_SEC", "1"))
# Safety cap: maximum number of attempts for one pending trigger (including the initial attempt).
DELAYED_ENTRY_MAX_ATTEMPTS = int(os.getenv("DELAYED_ENTRY_MAX_ATTEMPTS", "4"))

# Freshness policy (prompt-side): signals within this age are considered "fresh".
ENTRY_FRESHNESS_SEC = float(os.getenv("ENTRY_FRESHNESS_SEC", "15"))

AI_CLOSE_ENABLED = _env_bool("AI_CLOSE_ENABLED", "1")
AI_CLOSE_THROTTLE_SEC = float(os.getenv("AI_CLOSE_THROTTLE_SEC", "20"))
AI_CLOSE_MIN_CONFIDENCE = int(os.getenv("AI_CLOSE_MIN_CONFIDENCE", "70"))
AI_CLOSE_FALLBACK = str(os.getenv("AI_CLOSE_FALLBACK", "hold") or "hold").strip().lower()
AI_CLOSE_DEFAULT_CONFIDENCE = int(os.getenv("AI_CLOSE_DEFAULT_CONFIDENCE", "70"))

# Management settle window: when positions are open, wait briefly to let near-simultaneous
# context alerts (e.g., Q-Trend + Zones + FVG) arrive, then run ONE AI decision.
MGMT_POST_SIGNAL_WAIT_SEC = float(os.getenv("MGMT_POST_SIGNAL_WAIT_SEC", "3"))
MGMT_POST_SIGNAL_MAX_WAIT_SEC = float(os.getenv("MGMT_POST_SIGNAL_MAX_WAIT_SEC", "3"))


# --- Local entry safety guards (professional defaults) ---
# Gold day-trading (M5/M15) tends to suffer when spread is wide or ATR is too small.
ENTRY_MAX_SPREAD_POINTS = float(os.getenv("ENTRY_MAX_SPREAD_POINTS", "90"))
ENTRY_MIN_ATR_TO_SPREAD = float(os.getenv("ENTRY_MIN_ATR_TO_SPREAD", "2.5"))
ENTRY_COOLDOWN_SEC = float(os.getenv("ENTRY_COOLDOWN_SEC", "25"))

# Dynamic spread guards (ATR-relative). Set <= 0 to disable.
SPREAD_MAX_ATR_RATIO = float(os.getenv("SPREAD_MAX_ATR_RATIO", "0.10"))
# Soft override for spread-vs-ATR guard: when ATR/spread is still acceptable,
# skip hard block and let AI score decide. Set <=0 to disable override.
SPREAD_VS_ATR_SOFT_MIN = float(os.getenv("SPREAD_VS_ATR_SOFT_MIN", "10.0"))

# --- LRR Hard Guards (fxai_lrr_brain.py から移植) ---
# AI の呼び出し前に「絶対に見込みがない」相場を機械的に弾く 3 条件。
#   LRR_EV_HARD_MIN     : ATR/spread がこの値未満は EV ゼロと判断して即拒絶。
#                         lrr_brain では 10.0。v26 の既存 ENTRY_MIN_ATR_TO_SPREAD(2.5)
#                         より大幅に厳格。0 で無効化。
#   LRR_DIST_HARD_REJECT: M15 SMA20 乖離が ATR の N 倍超は伸び切り → 即拒絶。
#                         0 で無効化。
#   LRR_VOL_PANIC_RATIO : ATR_now / ATR_24h がこの値以上はパニック相場 → 即拒絶。
#                         0 で無効化。
#   LRR_SPREAD_MED_LR   : Robbins-Monro スプレッド中央値の学習率。
LRR_EV_HARD_MIN      = float(os.getenv("LRR_EV_HARD_MIN",      "10.0"))
LRR_DIST_HARD_REJECT = float(os.getenv("LRR_DIST_HARD_REJECT", "5.0"))
LRR_VOL_PANIC_RATIO  = float(os.getenv("LRR_VOL_PANIC_RATIO",  "2.0"))
LRR_SPREAD_MED_LR    = float(os.getenv("LRR_SPREAD_MED_LR",    "0.03"))

# Dynamic drift guard (ATR-relative). Set <= 0 to disable.
DRIFT_LIMIT_ATR_MULT = float(os.getenv("DRIFT_LIMIT_ATR_MULT", "0.15"))
DRIFT_LIMIT_MIN_POINTS = float(os.getenv("DRIFT_LIMIT_MIN_POINTS", "10"))
DRIFT_LIMIT_MAX_POINTS = float(os.getenv("DRIFT_LIMIT_MAX_POINTS", "120"))
ATR_SPIKE_CAP_MULT = float(os.getenv("ATR_SPIKE_CAP_MULT", "1.6"))
ATR_FLOOR_MULT = float(os.getenv("ATR_FLOOR_MULT", "0.7"))

# --- Entry race-condition guards ---
# Prevent duplicate orders when multiple near-simultaneous webhooks cause entry flow to run twice.
# 1) processing lock: blocks concurrent entry placements per symbol
# 2) processed trigger dedupe: blocks repeated processing of the same Lorentzian trigger (signal_time-based)
ENTRY_PROCESSING_LOCK_MAX_SEC = float(os.getenv("ENTRY_PROCESSING_LOCK_MAX_SEC", "20"))
ENTRY_TRIGGER_DEDUPE_TTL_SEC = float(os.getenv("ENTRY_TRIGGER_DEDUPE_TTL_SEC", "120"))

# --- Price drift safety (prompt-side by default) ---
# Prevent entering far away from the trigger price (avoid chasing highs/lows).
# Units are *points* (MT5 symbol_info.point based), consistent with spread_points.
# Set <= 0 to disable.
# NOTE: For XAUUSD, MT5 point is often 0.01 (1 point = $0.01).
# Default drift limit 20 points ≈ $0.20 with point=0.01, but we normalize drift using a
# larger "drift_point" for XAUUSD (see _drift_point_size) to avoid overly strict blocks.
DRIFT_LIMIT_POINTS = float(os.getenv("DRIFT_LIMIT_POINTS", os.getenv("MAX_SLIPPAGE_POINTS", "20.0")))
# If enabled, hard-block entry locally when drift exceeds the limit.
# Default is OFF to allow prompt-driven decisioning only.
DRIFT_HARD_BLOCK_ENABLED = _env_bool("DRIFT_HARD_BLOCK_ENABLED", "0")


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

# Optional indexes to reduce O(n) scans of signals_cache.
# Built/used only when SIGNAL_INDEX_ENABLED is True.
_signals_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
_signals_buckets_by_symbol: Dict[str, Dict[int, List[Dict[str, Any]]]] = {}

_cache_dirty = False
_cache_last_save_at = 0.0
_cache_last_dirty_at = 0.0
_cache_flush_thread_started = False

_spread_history_lock = Lock()
_spread_history_by_symbol: Dict[str, List[tuple]] = {}

# Robbins-Monro O(1) rolling spread median (from fxai_lrr_brain)
_spread_med_lock = Lock()
_spread_med_by_symbol: Dict[str, float] = {}

_auto_tune_lock = Lock()
_auto_tune_last_ts = 0.0

_metrics_lock = Lock()
_metrics: Dict[str, Any] = {
    "started_at": time.time(),
    "by_day": {},
}
_metrics_dirty = False
_metrics_last_save_at = 0.0

_qtrend_lock = Lock()
# Q-Trend context should be stored per timeframe to avoid mixing (e.g., M5 Q-Trend with H1 triggers).
# Structure: { SYMBOL: { tf_key: state_dict } }
_qtrend_state_by_symbol_tf: Dict[str, Dict[str, Dict[str, Any]]] = {}

_last_atr_by_symbol: Dict[str, float] = {}

_addon_lock = Lock()
_addon_state_by_symbol: Dict[str, Dict[str, Any]] = {}

_entry_lock = Lock()
_last_order_sent_at_by_symbol: Dict[str, float] = {}

_entry_processing_lock = Lock()
# Structure: { SYMBOL: { acquired_at: float, context: str|None } }
_entry_processing_by_symbol: Dict[str, Dict[str, Any]] = {}

_processed_entry_lock = Lock()
# Structure: { SYMBOL: { dedupe_key: processed_at_float } }
_processed_entry_triggers_by_symbol: Dict[str, Dict[str, float]] = {}

_mgmt_lock = Lock()
_mgmt_pending_lock = Lock()
_mgmt_pending_by_symbol: Dict[str, Dict[str, Any]] = {}
_mgmt_worker_running_by_symbol: Dict[str, bool] = {}

_pending_entry_lock = Lock()
# Structure: { SYMBOL: { trigger: dict, created_at: float, expires_at: float, attempts: int,
#                        last_attempt_at: float, last_retry_signal: dict|None } }
_pending_entry_by_symbol: Dict[str, Dict[str, Any]] = {}

_entry_agg_lock = Lock()
_entry_agg_by_symbol: Dict[str, Dict[str, Any]] = {}
_entry_agg_worker_running_by_symbol: Dict[str, bool] = {}


def _is_entry_agg_pending(symbol: str) -> bool:
    with _entry_agg_lock:
        return bool(isinstance(_entry_agg_by_symbol.get(symbol), dict))


def _entry_agg_trigger_compact(trigger: dict) -> Dict[str, Any]:
    if not isinstance(trigger, dict):
        return {}
    return {
        "source": trigger.get("source"),
        "event": trigger.get("event"),
        "side": trigger.get("side"),
        "tf": trigger.get("tf"),
        "signal_type": trigger.get("signal_type"),
        "confirmed": trigger.get("confirmed"),
        "strength": trigger.get("strength"),
        "signal_time": trigger.get("signal_time"),
        "receive_time": trigger.get("receive_time"),
        "price": trigger.get("price"),
    }


def _entry_agg_deferred_worker(symbol: str) -> None:
    """Wait for the entry aggregation window, then run one entry evaluation."""
    try:
        while True:
            with _entry_agg_lock:
                st = _entry_agg_by_symbol.get(symbol)
                if not isinstance(st, dict):
                    _entry_agg_worker_running_by_symbol[symbol] = False
                    return
                due_at = float(st.get("due_at") or 0.0)

            now = time.time()
            if due_at > 0 and now < due_at:
                time.sleep(min(0.2, max(0.01, due_at - now)))
                continue

            with _entry_agg_lock:
                st2 = _entry_agg_by_symbol.pop(symbol, None)
                _entry_agg_worker_running_by_symbol[symbol] = False

            if not isinstance(st2, dict):
                return
            trigger2 = st2.get("trigger") if isinstance(st2.get("trigger"), dict) else {}
            created_at = float(st2.get("created_at") or 0.0)
            trig_count = int(st2.get("trigger_count") or 1)

            # Reserve the initial pending-entry attempt right before running the actual entry attempt.
            try:
                if DELAYED_ENTRY_ENABLED:
                    _reserve_pending_entry_attempt(symbol, float(time.time()), retry_signal=trigger2)
            except Exception:
                pass

            with _entry_lock:
                last_sent_before = float(_last_order_sent_at_by_symbol.get(symbol, 0.0) or 0.0)

            attempt_ctx = f"AGG:{trig_count}:{int(created_at) if created_at > 0 else 0}"
            resp = _attempt_entry_from_lorentzian(
                symbol,
                trigger2,
                float(time.time()),
                pos_summary=get_mt5_positions_summary(symbol),
                bypass_ai_throttle=False,
                attempt_context=attempt_ctx,
            )

            with _entry_lock:
                last_sent_after = float(_last_order_sent_at_by_symbol.get(symbol, 0.0) or 0.0)
            if DELAYED_ENTRY_ENABLED and (last_sent_after > last_sent_before):
                _clear_pending_entry(symbol, reason="order_sent")
            return
    except Exception as e:
        try:
            with _entry_agg_lock:
                _entry_agg_worker_running_by_symbol[symbol] = False
        except Exception:
            pass
        print(f"[FXAI][ENTRY] Deferred worker error for {symbol}: {e}")


def _schedule_deferred_entry(symbol: str, normalized_trigger: dict, now: float) -> bool:
    """Schedule one entry evaluation after ENTRY_POST_SIGNAL_WAIT_SEC.

    Sliding window capped by ENTRY_POST_SIGNAL_MAX_WAIT_SEC.
    Returns True if scheduled/updated; False if disabled.
    """
    try:
        wait_sec = float(ENTRY_POST_SIGNAL_WAIT_SEC or 0.0)
        max_wait_sec = float(ENTRY_POST_SIGNAL_MAX_WAIT_SEC or 0.0)
    except Exception:
        wait_sec = 0.0
        max_wait_sec = 0.0
    if wait_sec <= 0:
        return False
    if max_wait_sec <= 0:
        max_wait_sec = wait_sec

    with _entry_agg_lock:
        st = _entry_agg_by_symbol.get(symbol)
        if not isinstance(st, dict):
            created_at = float(now)
            max_due_at = created_at + float(max_wait_sec)
            due_at = min(float(now) + float(wait_sec), max_due_at)
            st = {
                "created_at": created_at,
                "max_due_at": float(max_due_at),
                "due_at": float(due_at),
                "trigger": _entry_agg_trigger_compact(normalized_trigger),
                "trigger_count": 1,
            }
            _entry_agg_by_symbol[symbol] = st
        else:
            created_at = float(st.get("created_at") or now)
            max_due_at = float(st.get("max_due_at") or (created_at + float(max_wait_sec)))
            due_at = min(float(now) + float(wait_sec), max_due_at)
            st["due_at"] = float(due_at)
            st["trigger"] = _entry_agg_trigger_compact(normalized_trigger)
            st["trigger_count"] = int(st.get("trigger_count") or 0) + 1
            _entry_agg_by_symbol[symbol] = st

        running = bool(_entry_agg_worker_running_by_symbol.get(symbol))
        if not running:
            _entry_agg_worker_running_by_symbol[symbol] = True
            Thread(target=_entry_agg_deferred_worker, args=(symbol,), daemon=True).start()

    _set_status(
        last_result="Entry deferred",
        last_result_at=time.time(),
        last_entry_guard={
            "reason": "aggregation_window",
            "wait_sec": float(wait_sec),
            "max_wait_sec": float(max_wait_sec),
            "incoming": {
                "source": normalized_trigger.get("source"),
                "event": normalized_trigger.get("event"),
                "side": (normalized_trigger.get("side") or ""),
            },
        },
    )
    return True


def _entry_trigger_dedupe_key(symbol: str, action: str, trigger: Optional[dict]) -> str:
    """Build a stable key for a Lorentzian trigger to avoid duplicate order placement."""
    trig = trigger or {}
    src = (trig.get("source") or "")
    evt = (trig.get("event") or "")
    side = (trig.get("side") or "")
    tf = _normalize_tf(trig.get("tf") or trig.get("timeframe") or trig.get("interval"))
    st_raw = trig.get("signal_time") or trig.get("time") or trig.get("receive_time")
    try:
        st = float(st_raw) if st_raw is not None else 0.0
    except Exception:
        st = 0.0
    price = trig.get("price")
    # Use 3 decimals to make it robust to float noise but still precise enough.
    return f"LZTRIG:{symbol}:{action}:{side}:{tf}:{src}:{evt}:{st:.3f}:{price}"


def _prune_processed_entry_triggers_locked(now: float) -> None:
    try:
        ttl = float(ENTRY_TRIGGER_DEDUPE_TTL_SEC or 0.0)
    except Exception:
        ttl = 120.0
    if ttl <= 0:
        return
    cutoff = float(now - ttl)
    for sym, mp in list(_processed_entry_triggers_by_symbol.items()):
        if not isinstance(mp, dict):
            _processed_entry_triggers_by_symbol.pop(sym, None)
            continue
        for k, ts in list(mp.items()):
            try:
                if float(ts) <= cutoff:
                    mp.pop(k, None)
            except Exception:
                mp.pop(k, None)
        if not mp:
            _processed_entry_triggers_by_symbol.pop(sym, None)
        else:
            _processed_entry_triggers_by_symbol[sym] = mp


def _is_entry_processing_locked(symbol: str, now: Optional[float] = None) -> bool:
    if now is None:
        now = time.time()
    try:
        max_sec = float(ENTRY_PROCESSING_LOCK_MAX_SEC or 0.0)
    except Exception:
        max_sec = 20.0

    with _entry_processing_lock:
        st = _entry_processing_by_symbol.get(symbol)
        if not isinstance(st, dict):
            _entry_processing_by_symbol.pop(symbol, None)
            return False
        acquired_at = float(st.get("acquired_at") or 0.0)
        if acquired_at <= 0:
            _entry_processing_by_symbol.pop(symbol, None)
            return False
        if max_sec > 0 and (now - acquired_at) > max_sec:
            # Fail-safe: auto-unlock stale locks.
            _entry_processing_by_symbol.pop(symbol, None)
            return False
        return True


def _try_acquire_entry_processing_lock(symbol: str, *, context: Optional[str] = None, now: Optional[float] = None) -> bool:
    if now is None:
        now = time.time()
    try:
        max_sec = float(ENTRY_PROCESSING_LOCK_MAX_SEC or 0.0)
    except Exception:
        max_sec = 20.0
    with _entry_processing_lock:
        st = _entry_processing_by_symbol.get(symbol)
        if isinstance(st, dict):
            acquired_at = float(st.get("acquired_at") or 0.0)
            if acquired_at > 0 and (max_sec <= 0 or (now - acquired_at) <= max_sec):
                return False
            # stale lock
            _entry_processing_by_symbol.pop(symbol, None)
        _entry_processing_by_symbol[symbol] = {"acquired_at": float(now), "context": (str(context)[:160] if context else None)}
        return True


def _release_entry_processing_lock(symbol: str) -> None:
    with _entry_processing_lock:
        _entry_processing_by_symbol.pop(symbol, None)


def _is_trigger_already_processed(symbol: str, dedupe_key: str, now: Optional[float] = None) -> bool:
    if now is None:
        now = time.time()
    with _processed_entry_lock:
        _prune_processed_entry_triggers_locked(float(now))
        mp = _processed_entry_triggers_by_symbol.get(symbol)
        if not isinstance(mp, dict):
            return False
        return dedupe_key in mp


def _mark_trigger_processed(symbol: str, dedupe_key: str, now: Optional[float] = None) -> None:
    if now is None:
        now = time.time()
    with _processed_entry_lock:
        _prune_processed_entry_triggers_locked(float(now))
        mp = _processed_entry_triggers_by_symbol.get(symbol)
        if not isinstance(mp, dict):
            mp = {}
        mp[dedupe_key] = float(now)
        _processed_entry_triggers_by_symbol[symbol] = mp


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

        # Restore metrics
        if ENTRY_METRICS_ENABLED:
            try:
                _load_metrics()
            except Exception as e:
                print(f"[FXAI][WARN] Metrics load failed: {e}")
            try:
                _maybe_autotune_from_metrics(symbol=SYMBOL)
            except Exception as e:
                print(f"[FXAI][WARN] Auto-tune on startup failed: {e}")

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
    sym = raw.upper() if raw else (SYMBOL or "").strip().upper()
    # Apply aliases (e.g., XAUUSD -> GOLD)
    if sym and isinstance(SYMBOL_ALIASES, dict) and sym in SYMBOL_ALIASES:
        return str(SYMBOL_ALIASES.get(sym) or sym).strip().upper()
    return sym


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

    # Entry attempt observability
    "last_entry_attempt_at": None,
    "last_entry_attempt_context": None,
    "last_entry_bypass_ai_throttle": None,

    # Recent entry outcomes (ring buffer)
    "recent_entry_events": [],

    # Recent management outcomes (ring buffer)
    "recent_mgmt_events": [],

    # Heartbeat (EA -> Python)
    "heartbeat_enabled": bool(ZMQ_HEARTBEAT_ENABLED),
    "heartbeat_timeout_sec": float(ZMQ_HEARTBEAT_TIMEOUT_SEC),
    "last_heartbeat_at": None,
    "last_heartbeat_payload": None,
}


def _set_status(**kwargs) -> None:
    with _status_lock:
        _last_status.update(kwargs)


def _status_append_recent_entry_event(ev: Dict[str, Any]) -> None:
    """Append an entry event to the in-memory /status ring buffer."""
    try:
        max_n = int(ENTRY_STATUS_HISTORY_MAX or 0)
    except Exception:
        max_n = 30
    if max_n <= 0:
        return
    if not isinstance(ev, dict):
        return
    with _status_lock:
        arr = _last_status.get("recent_entry_events")
        if not isinstance(arr, list):
            arr = []
        arr.append(ev)
        if len(arr) > max_n:
            del arr[: max(0, len(arr) - max_n)]
        _last_status["recent_entry_events"] = arr


def _status_append_recent_mgmt_event(ev: Dict[str, Any]) -> None:
    """Append a management event to the in-memory /status ring buffer."""
    try:
        max_n = int(MGMT_STATUS_HISTORY_MAX or 0)
    except Exception:
        max_n = 50
    if max_n <= 0:
        return
    if not isinstance(ev, dict):
        return
    with _status_lock:
        arr = _last_status.get("recent_mgmt_events")
        if not isinstance(arr, list):
            arr = []
        arr.append(ev)
        if len(arr) > max_n:
            del arr[: max(0, len(arr) - max_n)]
        _last_status["recent_mgmt_events"] = arr


def _get_status_snapshot() -> Dict[str, Any]:
    with _status_lock:
        snap = dict(_last_status)
    # Copy list-like fields to avoid mutation during serialization.
    if isinstance(snap.get("recent_entry_events"), list):
        snap["recent_entry_events"] = list(snap.get("recent_entry_events") or [])
    if isinstance(snap.get("recent_mgmt_events"), list):
        snap["recent_mgmt_events"] = list(snap.get("recent_mgmt_events") or [])
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

    # Pending entry snapshot (for delayed re-evaluation observability)
    now2 = time.time()
    with _pending_entry_lock:
        pending_items = {}
        for sym, st in _pending_entry_by_symbol.items():
            if not isinstance(st, dict):
                continue
            trig = st.get("trigger") if isinstance(st.get("trigger"), dict) else {}
            created_at = float(st.get("created_at") or 0.0)
            expires_at = float(st.get("expires_at") or 0.0)
            last_attempt_at = float(st.get("last_attempt_at") or 0.0)
            attempts = int(st.get("attempts") or 0)
            pending_items[sym] = {
                "side": (trig.get("side") or "").lower(),
                "tf": trig.get("tf"),
                "trigger_time": trig.get("signal_time") or trig.get("receive_time"),
                "trigger_price": trig.get("price"),
                "age_sec": round(now2 - created_at, 3) if created_at > 0 else None,
                "expires_in_sec": round(expires_at - now2, 3) if expires_at > 0 else None,
                "attempts": attempts,
                "last_attempt_age_sec": round(now2 - last_attempt_at, 3) if last_attempt_at > 0 else None,
                "last_attempt_context": st.get("last_attempt_context"),
                "last_retry_signal": st.get("last_retry_signal"),
            }
        snap["pending_entries"] = pending_items

    # Pending management snapshot (for settle-window observability)
    now3 = time.time()
    with _mgmt_pending_lock:
        pending_mgmt = {}
        for sym, st in _mgmt_pending_by_symbol.items():
            if not isinstance(st, dict):
                continue
            created_at = float(st.get("created_at") or 0.0)
            due_at = float(st.get("due_at") or 0.0)
            max_due_at = float(st.get("max_due_at") or 0.0)
            pending_mgmt[sym] = {
                "age_sec": round(now3 - created_at, 3) if created_at > 0 else None,
                "due_in_sec": round(due_at - now3, 3) if due_at > 0 else None,
                "max_due_in_sec": round(max_due_at - now3, 3) if max_due_at > 0 else None,
                "last_signal": st.get("last_signal"),
                "last_signals": list(st.get("last_signals") or []) if isinstance(st.get("last_signals"), list) else None,
            }
        snap["pending_mgmt"] = pending_mgmt

    # Pending entry aggregation snapshot
    now4 = time.time()
    with _entry_agg_lock:
        pending_entry_agg = {}
        for sym, st in _entry_agg_by_symbol.items():
            if not isinstance(st, dict):
                continue
            created_at = float(st.get("created_at") or 0.0)
            due_at = float(st.get("due_at") or 0.0)
            max_due_at = float(st.get("max_due_at") or 0.0)
            pending_entry_agg[sym] = {
                "age_sec": round(now4 - created_at, 3) if created_at > 0 else None,
                "due_in_sec": round(due_at - now4, 3) if due_at > 0 else None,
                "max_due_in_sec": round(max_due_at - now4, 3) if max_due_at > 0 else None,
                "trigger_count": int(st.get("trigger_count") or 0),
                "trigger": st.get("trigger"),
            }
        snap["pending_entry_agg"] = pending_entry_agg
    return snap


def _run_position_management_once(
    symbol: str,
    normalized_signal: dict,
    now: float,
    recent_signals: Optional[List[dict]] = None,
) -> Optional[tuple[str, int]]:
    """Run one CLOSE/HOLD decision (synchronously) for the current position state."""
    # Under freeze mode: do not send any management while heartbeat is stale.
    if (HEARTBEAT_STALE_MODE == "freeze") and (not _heartbeat_is_fresh(now_ts=now)):
        _set_status(last_result="Frozen by heartbeat", last_result_at=time.time())
        return "Frozen by heartbeat", 200

    pos_summary = get_mt5_positions_summary(symbol)
    if int(pos_summary.get("positions_open") or 0) <= 0:
        return None

    net_side = (pos_summary.get("net_side") or "flat").lower()
    market = get_mt5_market_data(symbol)
    stats = get_qtrend_anchor_stats(symbol)

    if not AI_CLOSE_ENABLED:
        return None

    global _last_close_attempt_key, _last_close_attempt_at
    now_mono = time.time()

    src = (normalized_signal.get("source") or "")
    evt = (normalized_signal.get("event") or "")
    sig_side = (normalized_signal.get("side") or "").lower()

    is_reversal_like = (
        (net_side in {"buy", "sell"} and sig_side in {"buy", "sell"} and sig_side != net_side)
        and (
            (src in {"Q-Trend Strong", "Q-Trend", "Q-Trend-Strong", "Q-Trend-Normal"})
            or (src == "FVG" and evt == "fvg_touch")
            or (src == "Zones" and evt in {"zone_retrace_touch", "zone_touch"})
        )
    )

    # REVIEW: settle window経由（recent_signals渡された場合）はスロットルをバイパス
    # 理由: settle window後の評価は複数シグナルを集約した結果なので、スキップすると tp_mode等の更新が失われる
    is_settle_window_eval = (recent_signals is not None and len(recent_signals) > 0)

    last_attempt_age = now_mono - float(_last_close_attempt_at or 0.0)
    if (last_attempt_age < AI_CLOSE_THROTTLE_SEC) and (not is_reversal_like) and (not is_settle_window_eval):
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
        return None

    # Log throttle bypass reason (for observability)
    if is_settle_window_eval:
        print(f"[FXAI][MGMT] Throttle BYPASS: settle_window_eval (signals_count={len(recent_signals)})")
    elif is_reversal_like:
        print(f"[FXAI][MGMT] Throttle BYPASS: reversal_signal (src={src}, evt={evt}, sig_side={sig_side}, net_side={net_side})")

    attempt_key = f"{symbol}:{pos_summary.get('net_side')}:{int(pos_summary.get('positions_open') or 0)}:{src}:{evt}:{sig_side}"
    _last_close_attempt_key = attempt_key
    _last_close_attempt_at = now_mono

    used_signals = None
    if isinstance(recent_signals, list) and recent_signals:
        used_signals = [s for s in recent_signals if isinstance(s, dict)]
    if not used_signals:
        used_signals = [normalized_signal] if isinstance(normalized_signal, dict) else []

    ai_decision = _ai_close_hold_decision(symbol, market, stats, pos_summary, normalized_signal, recent_signals=used_signals)
    if (not ai_decision) or ai_decision["confidence"] < AI_CLOSE_MIN_CONFIDENCE:
        if (not ai_decision) and AI_CLOSE_FALLBACK == "default_close":
            ai_decision = {
                "confidence": int(_clamp(AI_CLOSE_DEFAULT_CONFIDENCE, 0, 100)),
                "reason": "fallback_default_close",
                "trail_mode": "NORMAL",
                "tp_mode": "NORMAL",  # REVIEW: fallback時もtp_modeを追加
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
            _zmq_send_json_with_metrics(
                {"type": "HOLD", "reason": "ai_fallback_hold", "trail_mode": "NORMAL", "tp_mode": "NORMAL"},
                symbol=symbol,
                kind="mgmt_hold_fallback"
            )
            ai_decision = {"confidence": 0, "reason": "ai_fallback_hold", "trail_mode": "NORMAL", "tp_mode": "NORMAL"}

    # _validate_ai_close_decision() が既に型検証済みなので、直接取得可能
    decision_reason = ai_decision.get("reason", "")
    decision_conf = ai_decision.get("confidence", 0)  # 既に int 型
    decision_trail = ai_decision.get("trail_mode", "NORMAL")
    decision_tp = ai_decision.get("tp_mode", "NORMAL")  # REVIEW: tp_mode を取得
    openai_rid = ai_decision.get("_openai_response_id")
    ai_ms = ai_decision.get("_ai_latency_ms")
    
    # 型安全な閾値取得 (空文字列や不正値への耐性)
    try:
        close_threshold = int(AI_CLOSE_MIN_CONFIDENCE or 0)
    except (ValueError, TypeError):
        close_threshold = 65  # デフォルト値

    final_action = "CLOSE" if decision_conf >= close_threshold else "HOLD"
    try:
        _record_mgmt_outcome(
            symbol=symbol,
            action=final_action,
            confidence=decision_conf,
            min_close_confidence=close_threshold,
            reason=decision_reason,
            trail_mode=decision_trail,
            tp_mode=decision_tp,
            pos_summary=pos_summary,
            market=market,
            used_signals=used_signals,
            openai_response_id=openai_rid,
            ai_latency_ms=ai_ms,
        )
    except Exception as e:
        print(f"[FXAI][WARN] Failed to record mgmt metrics: {e}")
    
    if decision_conf >= close_threshold:
        _zmq_send_json_with_metrics(
            {"type": "CLOSE", "reason": decision_reason, "trail_mode": decision_trail, "tp_mode": decision_tp},
            symbol=symbol,
            kind="mgmt_close"
        )
        _set_status(
            last_result="CLOSE",
            last_result_at=time.time(),
            last_mgmt_action="CLOSE",
            last_mgmt_confidence=decision_conf,
            last_mgmt_reason=decision_reason,
            last_mgmt_at=time.time(),
            last_mgmt_throttled=None,
        )
        _status_append_recent_mgmt_event(
            {
                "ts": time.time(),
                "symbol": symbol,
                "action": "CLOSE",
                "confidence": decision_conf,
                "trail_mode": decision_trail,
                "reason": decision_reason,
                "openai_response_id": openai_rid,
                "ai_latency_ms": ai_ms,
                "signals_used": used_signals,
            }
        )
        return "CLOSE", 200

    _zmq_send_json_with_metrics(
        {"type": "HOLD", "reason": decision_reason, "trail_mode": decision_trail, "tp_mode": decision_tp},
        symbol=symbol,
        kind="mgmt_hold",
    )
    _set_status(
        last_result="HOLD",
        last_result_at=time.time(),
        last_mgmt_action="HOLD",
        last_mgmt_confidence=decision_conf,
        last_mgmt_reason=decision_reason,
        last_mgmt_at=time.time(),
        last_mgmt_throttled=None,
    )
    _status_append_recent_mgmt_event(
        {
            "ts": time.time(),
            "symbol": symbol,
            "action": "HOLD",
            "confidence": decision_conf,
            "trail_mode": decision_trail,
            "reason": decision_reason,
            "openai_response_id": openai_rid,
            "ai_latency_ms": ai_ms,
            "signals_used": used_signals,
        }
    )
    return "HOLD", 200


def _mgmt_deferred_worker(symbol: str) -> None:
    """Wait for settle window, then run one management decision."""
    try:
        while True:
            with _mgmt_pending_lock:
                st = _mgmt_pending_by_symbol.get(symbol)
                if not isinstance(st, dict):
                    _mgmt_worker_running_by_symbol[symbol] = False
                    return
                due_at = float(st.get("due_at") or 0.0)
                last_signal = st.get("last_signal")

            now = time.time()
            if due_at > 0 and now < due_at:
                time.sleep(min(0.2, max(0.01, due_at - now)))
                continue

            # Time to run the decision.
            with _mgmt_pending_lock:
                st2 = _mgmt_pending_by_symbol.pop(symbol, None)
                _mgmt_worker_running_by_symbol[symbol] = False
            if not isinstance(st2, dict):
                return
            last_signal2 = st2.get("last_signal") or {}
            last_signals2 = st2.get("last_signals")

            used_signals = None
            if isinstance(last_signals2, list) and last_signals2:
                used_signals = [s for s in last_signals2 if isinstance(s, dict)]
            if not used_signals:
                used_signals = [dict(last_signal2)] if isinstance(last_signal2, dict) else []

            with _mgmt_lock:
                _run_position_management_once(
                    symbol,
                    dict(last_signal2) if isinstance(last_signal2, dict) else {},
                    time.time(),
                    recent_signals=used_signals,
                )
            return
    except Exception as e:
        try:
            with _mgmt_pending_lock:
                _mgmt_worker_running_by_symbol[symbol] = False
        except Exception:
            pass
        print(f"[FXAI][MGMT] Deferred worker error for {symbol}: {e}")


def _try_schedule_pyramid_entry(
    symbol: str,
    pending_entry_trigger: bool,
    normalized_trigger: Optional[dict],
    pos_summary: dict,
    now: float,
) -> None:
    """Try to schedule a PYRAMID deferred entry when conditions are met.
    
    PYRAMID-DEFER: Allow same-direction deferred entry during management defer.
    Safety gates: same direction only + profit gate (50% of profit_protect threshold).
    """
    if not (pending_entry_trigger and normalized_trigger):
        return
    
    trig_side = (normalized_trigger.get("side") or "").strip().lower()
    net_side = (pos_summary.get("net_side") or "").strip().lower()
    
    # Gate 1: Same direction only
    if trig_side not in {"buy", "sell"} or net_side not in {"buy", "sell"}:
        return
    if net_side != trig_side:
        return
    
    # Gate 2: Profit gate (must have unrealized profit)
    try:
        market = get_mt5_market_data(symbol)
        cur = _current_price_from_market(market)
        point = float((market or {}).get("point") or 0.0)
        
        if cur <= 0 or point <= 0:
            return
        
        if net_side == "buy":
            net_avg_open = float(pos_summary.get("buy_avg_open") or 0.0)
            move_points = cur - net_avg_open
        else:
            net_avg_open = float(pos_summary.get("sell_avg_open") or 0.0)
            move_points = net_avg_open - cur
        
        if net_avg_open <= 0:
            return
        
        spread_points = float((market or {}).get("spread") or 0.0)
        atr_points = float((market or {}).get("atr_points") or 0.0)
        profit_protect_threshold_points = _compute_profit_protect_threshold_points(spread_points, atr_points)
        
        if profit_protect_threshold_points <= 0:
            return
        
        net_move_points = float(move_points) / float(point)
        min_profit_gate = float(profit_protect_threshold_points) * 0.5
        
        if net_move_points < min_profit_gate:
            return
        
        # All gates passed: schedule PYRAMID entry
        normalized_trigger["entry_mode"] = "PYRAMID"
        if DELAYED_ENTRY_ENABLED:
            _upsert_pending_entry(symbol, normalized_trigger, float(now))
        if _schedule_deferred_entry(symbol, normalized_trigger, float(now)):
            print(
                f"[FXAI][PYRAMID] Deferred entry scheduled: "
                f"net_move={net_move_points:.1f}pts >= gate={min_profit_gate:.1f}pts"
            )
    except Exception as e:
        print(f"[FXAI][PYRAMID] Failed to schedule: {e}")


def _schedule_deferred_mgmt(symbol: str, normalized_signal: dict, now: float) -> bool:
    """Schedule a deferred management AI run after a short settle window.

    Uses a sliding window capped by MGMT_POST_SIGNAL_MAX_WAIT_SEC.
    Returns True if scheduled/updated; False if disabled.
    """
    try:
        wait_sec = float(MGMT_POST_SIGNAL_WAIT_SEC or 0.0)
        max_wait_sec = float(MGMT_POST_SIGNAL_MAX_WAIT_SEC or 0.0)
    except Exception:
        wait_sec = 0.0
        max_wait_sec = 0.0

    if wait_sec <= 0:
        return False
    if max_wait_sec <= 0:
        max_wait_sec = wait_sec

    with _mgmt_pending_lock:
        def _mk_sig(sig: dict) -> Dict[str, Any]:
            return {
                "source": sig.get("source"),
                "event": sig.get("event"),
                "side": sig.get("side"),
                "tf": sig.get("tf"),
                "signal_type": sig.get("signal_type"),
                "confirmed": sig.get("confirmed"),
                "signal_time": sig.get("signal_time"),
                "receive_time": sig.get("receive_time"),
            }

        def _dedupe_append(arr: list, item: dict) -> list:
            if not isinstance(arr, list):
                arr = []
            if arr and isinstance(arr[-1], dict):
                last = arr[-1]
                if (
                    last.get("source") == item.get("source")
                    and last.get("event") == item.get("event")
                    and last.get("side") == item.get("side")
                    and last.get("tf") == item.get("tf")
                    and last.get("signal_time") == item.get("signal_time")
                ):
                    arr[-1] = item
                    return arr
            arr.append(item)
            return arr

        try:
            keep_n = int(MGMT_PENDING_SIGNAL_HISTORY_MAX or 0)
        except Exception:
            keep_n = 12
        if keep_n <= 0:
            keep_n = 1

        st = _mgmt_pending_by_symbol.get(symbol)
        if not isinstance(st, dict):
            created_at = float(now)
            max_due_at = created_at + float(max_wait_sec)
            due_at = min(float(now) + float(wait_sec), max_due_at)
            sig_item = _mk_sig(normalized_signal if isinstance(normalized_signal, dict) else {})
            st = {
                "created_at": created_at,
                "max_due_at": float(max_due_at),
                "due_at": float(due_at),
                "last_signal": dict(sig_item),
                "last_signals": [dict(sig_item)],
            }
            _mgmt_pending_by_symbol[symbol] = st
        else:
            created_at = float(st.get("created_at") or now)
            max_due_at = float(st.get("max_due_at") or (created_at + float(max_wait_sec)))
            due_at = min(float(now) + float(wait_sec), max_due_at)
            st["due_at"] = float(due_at)
            sig_item = _mk_sig(normalized_signal if isinstance(normalized_signal, dict) else {})
            st["last_signal"] = dict(sig_item)
            st["last_signals"] = _dedupe_append(st.get("last_signals"), dict(sig_item))
            if isinstance(st.get("last_signals"), list) and len(st["last_signals"]) > keep_n:
                st["last_signals"] = list(st["last_signals"])[-keep_n:]
            _mgmt_pending_by_symbol[symbol] = st

        running = bool(_mgmt_worker_running_by_symbol.get(symbol))
        if not running:
            _mgmt_worker_running_by_symbol[symbol] = True
            Thread(target=_mgmt_deferred_worker, args=(symbol,), daemon=True).start()

    _set_status(
        last_result="Mgmt deferred",
        last_result_at=time.time(),
        last_mgmt_throttled={
            "reason": "settle_window",
            "wait_sec": float(wait_sec),
            "max_wait_sec": float(max_wait_sec),
            "incoming": {"source": normalized_signal.get("source"), "event": normalized_signal.get("event"), "side": (normalized_signal.get("side") or "")},
        },
    )
    return True


def _prune_pending_entries_locked(now: float) -> None:
    """Remove expired pending entries.
    
    CRITICAL: Lorentzian triggers older than DELAYED_ENTRY_HARD_TTL_SEC are removed
    to prevent stale signals from being re-evaluated with incorrect timestamps.
    """
    remove = []
    for sym, st in _pending_entry_by_symbol.items():
        try:
            expires_at = float((st or {}).get("expires_at") or 0.0)
            attempts = int((st or {}).get("attempts") or 0)
            created_at = float((st or {}).get("created_at") or 0.0)
            
            # Hard TTL: Remove Lorentzian triggers older than configured limit.
            # This prevents "zombie signals" from persisting with stale timestamps.
            if (created_at > 0 and (now - created_at) >= DELAYED_ENTRY_HARD_TTL_SEC) or \
               (expires_at > 0 and now >= expires_at) or \
               (int(DELAYED_ENTRY_MAX_ATTEMPTS or 0) > 0 and attempts >= int(DELAYED_ENTRY_MAX_ATTEMPTS)):
                remove.append(sym)
        except Exception:
            remove.append(sym)
    for sym in remove:
        _pending_entry_by_symbol.pop(sym, None)


def _upsert_pending_entry(symbol: str, trigger: dict, now: float) -> None:
    """Create or update a pending entry with proper expiration.
    
    Default expiration: 5 minutes (300s) to prevent stale triggers.
    """
    if not DELAYED_ENTRY_ENABLED:
        return
    try:
        max_wait = float(DELAYED_ENTRY_MAX_WAIT_SEC or 300.0)
    except Exception:
        max_wait = 300.0
    expires_at = now + max_wait

    with _pending_entry_lock:
        _prune_pending_entries_locked(now)
        _pending_entry_by_symbol[symbol] = {
            "trigger": dict(trigger),
            "created_at": float(now),
            "expires_at": float(expires_at),
            "attempts": 0,
            "last_attempt_at": 0.0,
            "last_retry_signal": None,
        }


def _clear_pending_entry(symbol: str, *, reason: str) -> None:
    with _pending_entry_lock:
        st = _pending_entry_by_symbol.pop(symbol, None)
    if st is not None:
        print(f"[FXAI][DELAYED_ENTRY] Cleared pending for {symbol}: {reason}")


def _reserve_pending_entry_attempt(
    symbol: str,
    now: float,
    *,
    retry_signal: Optional[dict] = None,
    attempt_context: Optional[str] = None,
) -> Optional[int]:
    """Reserve an attempt slot for a pending entry.

    Centralizes the attempt counters and retry throttling to avoid drift across call sites.
    Returns the (1-based) attempt number if reserved, else None.
    """
    try:
        min_interval = float(DELAYED_ENTRY_MIN_RETRY_INTERVAL_SEC or 0.0)
    except Exception:
        min_interval = 0.0

    with _pending_entry_lock:
        st = _pending_entry_by_symbol.get(symbol)
        if not isinstance(st, dict):
            return None

        last_attempt_at = float(st.get("last_attempt_at") or 0.0)
        attempts = int(st.get("attempts") or 0)

        if int(DELAYED_ENTRY_MAX_ATTEMPTS or 0) > 0 and attempts >= int(DELAYED_ENTRY_MAX_ATTEMPTS):
            _pending_entry_by_symbol.pop(symbol, None)
            return None

        if last_attempt_at > 0 and min_interval > 0 and (now - last_attempt_at) < min_interval:
            return None

        st["last_attempt_at"] = float(now)
        st["attempts"] = attempts + 1
        if attempt_context:
            st["last_attempt_context"] = str(attempt_context)[:160]

        if isinstance(retry_signal, dict):
            st["last_retry_signal"] = {
                "source": retry_signal.get("source"),
                "event": retry_signal.get("event"),
                "side": retry_signal.get("side"),
                "tf": retry_signal.get("tf"),
                "signal_time": retry_signal.get("signal_time"),
                "receive_time": retry_signal.get("receive_time"),
            }

        _pending_entry_by_symbol[symbol] = st
        return int(attempts + 1)


def _should_trigger_delayed_reeval(incoming: dict, pending_trigger: dict) -> bool:
    """Return True if incoming signal is the kind that should trigger a delayed re-evaluation."""
    try:
        sig_type = (incoming.get("signal_type") or "").strip().lower()
        src = (incoming.get("source") or "").strip()
        evt = (incoming.get("event") or "").strip().lower()
        inc_side = (incoming.get("side") or "").strip().lower()
        pend_side = (pending_trigger.get("side") or "").strip().lower()
        confirmed = (incoming.get("confirmed") or "").strip().lower()
        strength = (incoming.get("strength") or "").strip().lower()
    except Exception:
        return False

    # Tighten delayed re-evaluation triggers to reduce noisy repeated AI calls.
    # We only re-evaluate when the incoming signal is likely to add *structural* evidence.
    is_confirmed = confirmed in {"bar_close", "confirmed", "close"}
    is_strong = strength == "strong"

    # Directional signals must match the pending trigger direction.
    if inc_side in {"buy", "sell"} and pend_side in {"buy", "sell"} and inc_side != pend_side:
        return False

    # Q-Trend updates (context) are key.
    if _is_qtrend_source(src):
        return True

    # Zones structure confirmations (neutral is ok) are meaningful.
    if src == "Zones" and sig_type in {"structure", "zones", "zone"} and evt in {"new_zone_confirmed", "zone_confirmed", "new_zone", "zone_created", "zone_breakout"}:
        return True

    # Zones touch events are noisy; only trigger delayed re-eval when confirmed/strong.
    if src == "Zones" and (evt in {"zone_retrace_touch", "zone_touch"} or "touch" in evt):
        return bool(is_confirmed or is_strong)

    # FVG touch events: also require confirmed/strong.
    if src in {"FVG", "LuxAlgo_FVG"} and (evt == "fvg_touch" or "touch" in evt):
        return bool(is_confirmed or is_strong)

    # Trend filter evidence: only when confirmed (avoid intrabar churn).
    if src == "OSGFC" or sig_type == "trend_filter":
        return bool(is_confirmed)

    return False


def _maybe_attempt_delayed_entry(symbol: str, incoming_signal: dict, now: float) -> Optional[tuple[str, int]]:
    """Attempt delayed entry re-evaluation when supportive signals arrive.
    
    CRITICAL: Validates trigger age to prevent stale Lorentzian signals
    from being re-evaluated with incorrect timestamps.
    """
    if not DELAYED_ENTRY_ENABLED:
        return None

    # If an entry aggregation window is pending, do not run delayed-entry attempts yet.
    # This enforces "no immediate order" and avoids double evaluation during the 3s window.
    if _is_entry_agg_pending(symbol):
        return None

    # Do not run delayed entry while an entry placement is in progress.
    if _is_entry_processing_locked(symbol, now=now):
        print(f"[FXAI][DELAYED_ENTRY] Skip: entry processing locked for {symbol}")
        return None

    with _pending_entry_lock:
        _prune_pending_entries_locked(now)
        st = _pending_entry_by_symbol.get(symbol)
        if not st or not isinstance(st, dict):
            return None
        
        # CRITICAL: Validate trigger age while holding lock to prevent race conditions.
        created_at = float(st.get("created_at") or 0.0)
        if created_at > 0 and (now - created_at) >= DELAYED_ENTRY_HARD_TTL_SEC:
            _pending_entry_by_symbol.pop(symbol, None)
            print(f"[FXAI][DELAYED_ENTRY] Rejected stale trigger for {symbol}: age={(now - created_at):.1f}s")
            return None
        
        trigger = st.get("trigger") if isinstance(st.get("trigger"), dict) else None
        if not trigger:
            _pending_entry_by_symbol.pop(symbol, None)
            return None
        
        # Make a copy for processing outside lock
        st_copy = dict(st)

    # Continue processing with copy (lock released)

    # If we already processed this Lorentzian trigger, drop the pending state.
    try:
        trig_side = (trigger.get("side") or "").strip().lower()
        action = "BUY" if trig_side == "buy" else "SELL" if trig_side == "sell" else ""
        if action:
            dk = _entry_trigger_dedupe_key(symbol, action, trigger)
            if _is_trigger_already_processed(symbol, dk, now=now):
                _clear_pending_entry(symbol, reason="trigger_already_processed")
                return None
    except Exception:
        pass

    if not _should_trigger_delayed_reeval(incoming_signal, trigger):
        return None

    # Only attempt delayed entries when flat (do not create add-ons minutes later).
    pos_summary = get_mt5_positions_summary(symbol)
    # PYRAMID-DEFER: allow pyramiding deferred entries to continue even if positions are open.
    entry_mode = str((trigger or {}).get("entry_mode") or "").strip().upper()
    if int(pos_summary.get("positions_open") or 0) > 0:
        if entry_mode != "PYRAMID":
            _clear_pending_entry(symbol, reason="positions_open")
            return None

    retry_src = (incoming_signal.get("source") or "")
    retry_evt = (incoming_signal.get("event") or "")
    retry_tf = (incoming_signal.get("tf") or "")
    retry_st_raw = incoming_signal.get("signal_time") or incoming_signal.get("receive_time") or now
    try:
        retry_st = float(retry_st_raw)
    except Exception:
        retry_st = float(now)
    retry_strength = str(incoming_signal.get("strength") or "")
    retry_confirmed = 1 if bool(incoming_signal.get("confirmed")) else 0
    attempt_context = f"DE:{retry_src}:{retry_evt}:{retry_tf}:{retry_st:.0f}:{retry_strength}:{retry_confirmed}"

    attempt_n = _reserve_pending_entry_attempt(
        symbol,
        float(now),
        retry_signal=incoming_signal,
        attempt_context=attempt_context,
    )
    if attempt_n is None:
        return None

    with _entry_lock:
        last_sent_before = float(_last_order_sent_at_by_symbol.get(symbol, 0.0) or 0.0)

    print(
        "[FXAI][DELAYED_ENTRY] Re-evaluating pending entry due to incoming signal: "
        + json.dumps(
            {
                "symbol": symbol,
                "incoming": {
                    "source": incoming_signal.get("source"),
                    "event": incoming_signal.get("event"),
                    "side": incoming_signal.get("side"),
                    "tf": incoming_signal.get("tf"),
                },
                "attempt": int(attempt_n),
                "max_attempts": int(DELAYED_ENTRY_MAX_ATTEMPTS or 0),
                "attempt_context": attempt_context,
            },
            ensure_ascii=False,
        )
    )

    resp = _attempt_entry_from_lorentzian(
        symbol,
        trigger,
        now,
        pos_summary=pos_summary,
        bypass_ai_throttle=True,
        attempt_context=attempt_context,
    )

    with _entry_lock:
        last_sent_after = float(_last_order_sent_at_by_symbol.get(symbol, 0.0) or 0.0)
    if last_sent_after > last_sent_before:
        _clear_pending_entry(symbol, reason="order_sent")

    return resp


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
            "zmq_deser_fail",
            "hb_send_fail",
        ):
            if k in payload:
                keep[k] = payload.get(k)
        if keep:
            return keep
        return {"keys": list(payload.keys())[:20]}
    if isinstance(payload, str):
        return payload[:200]
    return payload


def _get_server_time_offset_sec() -> Optional[float]:
    """Return broker server offset vs UTC (seconds), if known."""
    with _status_lock:
        payload = _last_status.get("last_heartbeat_payload")
    if not isinstance(payload, dict):
        return None

    try:
        offset = payload.get("server_gmt_offset_sec")
        if offset is not None:
            return float(offset)
    except Exception:
        pass

    try:
        gmt_ts = payload.get("gmt_ts")
        srv_ts = payload.get("trade_server_ts")
        if gmt_ts is not None and srv_ts is not None:
            return float(srv_ts) - float(gmt_ts)
    except Exception:
        pass

    return None


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

            _zmq_send_json_with_metrics({"type": "CLOSE", "reason": "weekend_discretionary_close"}, symbol=sym, kind="weekend_close")
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


def _compact_for_prompt(obj: Any, *, max_list_items: int, max_str_len: int, depth: int = 0, max_depth: int = 4) -> Any:
    """Optionally compact a JSON-serializable object for prompt size control.

    Used ONLY when PROMPT_COMPACT_ENABLED is true.
    Goal: reduce token usage while keeping key semantics.
    """

    if depth >= max_depth:
        if isinstance(obj, (dict, list)):
            return {"_truncated": True, "_type": type(obj).__name__}
        return obj

    if isinstance(obj, str):
        if max_str_len > 0 and len(obj) > max_str_len:
            return obj[:max_str_len]
        return obj

    if isinstance(obj, (int, float, bool)) or obj is None:
        return obj

    if isinstance(obj, list):
        items = obj
        if max_list_items > 0 and len(items) > max_list_items:
            trimmed = items[-max_list_items:]
            out = [
                _compact_for_prompt(v, max_list_items=max_list_items, max_str_len=max_str_len, depth=depth + 1, max_depth=max_depth)
                for v in trimmed
            ]
            out.append({"_truncated": True, "_dropped": int(len(items) - len(trimmed))})
            return out
        return [_compact_for_prompt(v, max_list_items=max_list_items, max_str_len=max_str_len, depth=depth + 1, max_depth=max_depth) for v in items]

    if isinstance(obj, dict):
        out2: Dict[str, Any] = {}
        for k, v in obj.items():
            try:
                key = str(k)
            except Exception:
                key = ""
            out2[key] = _compact_for_prompt(v, max_list_items=max_list_items, max_str_len=max_str_len, depth=depth + 1, max_depth=max_depth)
        return out2

    try:
        s = str(obj)
    except Exception:
        s = ""
    if max_str_len > 0 and len(s) > max_str_len:
        s = s[:max_str_len]
    return s


def _drift_point_size(symbol: str, point: float) -> float:
    """Return the point size to use for drift checks.

    For XAUUSD/GOLD, MT5 often reports point=0.01 (1 point = $0.01).
    Drift checks are more intuitive in $0.10 increments (pip-like), so we
    normalize to 0.10 when point is very small.
    """
    try:
        sym = (symbol or "").strip().upper()
    except Exception:
        sym = ""
    try:
        p = float(point or 0.0)
    except Exception:
        p = 0.0
    if p <= 0:
        return 0.0
    if sym in {"XAUUSD", "GOLD", "XAU"} and p <= 0.01:
        return 0.1
    return p


def _effective_atr_price(market: Dict[str, Any]) -> float:
    try:
        atr = float((market or {}).get("atr") or 0.0)
        atr_avg = float((market or {}).get("atr_avg_24h") or 0.0)
    except Exception:
        return 0.0
    if atr <= 0:
        return 0.0
    if atr_avg > 0:
        cap_mult = float(ATR_SPIKE_CAP_MULT or 0.0)
        floor_mult = float(ATR_FLOOR_MULT or 0.0)
        if cap_mult > 0:
            atr = min(atr, atr_avg * cap_mult)
        if floor_mult > 0:
            atr = max(atr, atr_avg * floor_mult)
    return atr


def _dynamic_drift_limit_points(market: Dict[str, Any]) -> float:
    limit_points = float(DRIFT_LIMIT_POINTS or 0.0)
    atr_price = _effective_atr_price(market)
    try:
        drift_point = float((market or {}).get("drift_point") or (market or {}).get("point") or 0.0)
    except Exception:
        drift_point = 0.0
    if atr_price > 0 and drift_point > 0 and float(DRIFT_LIMIT_ATR_MULT or 0.0) > 0:
        limit_points = (atr_price * float(DRIFT_LIMIT_ATR_MULT)) / drift_point
        if float(DRIFT_LIMIT_MIN_POINTS or 0.0) > 0:
            limit_points = max(limit_points, float(DRIFT_LIMIT_MIN_POINTS))
        if float(DRIFT_LIMIT_MAX_POINTS or 0.0) > 0:
            limit_points = min(limit_points, float(DRIFT_LIMIT_MAX_POINTS))
    return limit_points


def _clamp(v: float, lo: Optional[float], hi: Optional[float]) -> float:
    try:
        out = float(v)
    except Exception:
        return 0.0
    if lo is not None:
        try:
            out = max(out, float(lo))
        except Exception:
            pass
    if hi is not None:
        try:
            out = min(out, float(hi))
        except Exception:
            pass
    return out


def _percentile(values: List[float], p: float) -> Optional[float]:
    """Compute percentile using linear interpolation (nearest-rank fallback)."""
    if not values:
        return None
    try:
        pct = float(p)
    except (TypeError, ValueError):
        return None
    pct = max(0.0, min(1.0, pct))
    vals = sorted(float(v) for v in values if v is not None)
    if not vals:
        return None
    # Use statistics.quantiles for Python 3.8+ (more accurate)
    try:
        return float(statistics.quantiles(vals, n=100, method='inclusive')[int(pct * 100)])
    except (AttributeError, IndexError):
        # Fallback for older Python or edge cases
        idx = int(round(pct * (len(vals) - 1)))
        return float(vals[max(0, min(idx, len(vals) - 1))])


def _format_env_value(v: Any) -> str:
    try:
        if isinstance(v, float):
            return f"{v:.6f}".rstrip("0").rstrip(".") or "0"
        if isinstance(v, int):
            return str(int(v))
        if isinstance(v, bool):
            return "1" if v else "0"
    except Exception:
        pass
    return str(v)


def _resolve_env_path(path_raw: str) -> str:
    path = str(path_raw or ".env").strip() or ".env"
    if os.path.isabs(path):
        return path
    return os.path.join(os.getcwd(), path)


def _update_env_file(path: str, updates: Dict[str, Any]) -> bool:
    """Atomically update .env file (existing keys replaced, new keys appended)."""
    if not updates:
        return False
    try:
        existing_lines: List[str] = []
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing_lines = f.readlines()
            except (IOError, OSError) as e:
                print(f"[FXAI][WARN] Failed to read .env: {e}")
                return False

        key_index: Dict[str, int] = {}
        for i, line in enumerate(existing_lines):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("export "):
                stripped = stripped[7:].lstrip()
            if "=" not in stripped:
                continue
            key = stripped.split("=", 1)[0].strip()
            if key:
                key_index[key] = i

        for key, value in updates.items():
            env_val = _format_env_value(value)
            new_line = f"{key}={env_val}\n"
            if key in key_index:
                existing_lines[key_index[key]] = new_line
            else:
                existing_lines.append(new_line)

        tmp_path = f"{path}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.writelines(existing_lines)
            os.replace(tmp_path, path)
        except (IOError, OSError) as e:
            print(f"[FXAI][ERROR] Failed to write .env: {e}")
            return False
        return True
    except Exception as e:
        print(f"[FXAI][ERROR] Unexpected error updating .env: {e}")
        return False


def _check_price_drift(
    signal_price: Any,
    current_market: Dict[str, Any],
    side: str,
    *,
    limit_points: Optional[float] = None,
) -> tuple[bool, str]:
    """Check price drift between signal price and current market.

    - BUY: block if current_ask > signal_price + limit
    - SELL: block if current_bid < signal_price - limit

    Returns (ok, reason).
    """
    if limit_points is None:
        limit_points = float(DRIFT_LIMIT_POINTS or 0.0)
    if float(limit_points or 0.0) <= 0:
        return True, "disabled"

    try:
        sp = float(signal_price)
    except Exception:
        return True, "no_signal_price"

    if not isinstance(current_market, dict):
        return True, "no_market"

    try:
        bid = float(current_market.get("bid") or 0.0)
        ask = float(current_market.get("ask") or 0.0)
        point = float(current_market.get("drift_point") or current_market.get("point") or 0.0)
    except Exception:
        bid, ask, point = 0.0, 0.0, 0.0

    if sp <= 0 or (bid <= 0 and ask <= 0):
        return True, "invalid_prices"
    if point <= 0:
        return True, "no_point"

    s = (side or "").strip().lower()
    if s in {"buy", "long"}:
        cur = ask if ask > 0 else bid
        if cur <= 0:
            return True, "no_current_ask"
        max_allowed = sp + (limit_points * point)
        if cur > max_allowed:
            drift_pts = (cur - sp) / point
            return False, f"drift_too_high buy signal={sp} ask={cur} limit_pts={limit_points} drift_pts={drift_pts:.2f}"
        return True, "ok"

    if s in {"sell", "short"}:
        cur = bid if bid > 0 else ask
        if cur <= 0:
            return True, "no_current_bid"
        min_allowed = sp - (limit_points * point)
        if cur < min_allowed:
            drift_pts = (sp - cur) / point
            return False, f"drift_too_low sell signal={sp} bid={cur} limit_pts={limit_points} drift_pts={drift_pts:.2f}"
        return True, "ok"

    return True, "unknown_side"


def _price_drift_snapshot(signal_price: Any, current_market: Dict[str, Any], side: str) -> Dict[str, Any]:
    """Return a compact snapshot for prompts/metrics (never raises)."""
    limit_points = _dynamic_drift_limit_points(current_market)
    ok, reason = _check_price_drift(signal_price, current_market, side, limit_points=limit_points)
    snap: Dict[str, Any] = {
        "enabled": bool(float(limit_points or 0.0) > 0),
        "hard_block_enabled": bool(DRIFT_HARD_BLOCK_ENABLED),
        "limit_points": limit_points,
        "ok": bool(ok),
        "reason": reason,
        "signal_price": signal_price,
        "bid": (current_market or {}).get("bid") if isinstance(current_market, dict) else None,
        "ask": (current_market or {}).get("ask") if isinstance(current_market, dict) else None,
        "point": (current_market or {}).get("point") if isinstance(current_market, dict) else None,
        "drift_point": (current_market or {}).get("drift_point") if isinstance(current_market, dict) else None,
    }
    try:
        sp = float(signal_price)
        bid = float((current_market or {}).get("bid") or 0.0)
        ask = float((current_market or {}).get("ask") or 0.0)
        point = float((current_market or {}).get("drift_point") or (current_market or {}).get("point") or 0.0)
        s = (side or "").strip().lower()
        if sp > 0 and point > 0 and (bid > 0 or ask > 0):
            if s == "buy":
                cur = ask if ask > 0 else bid
                snap["current_price"] = cur
                snap["drift_points"] = (cur - sp) / point
            elif s == "sell":
                cur = bid if bid > 0 else ask
                snap["current_price"] = cur
                snap["drift_points"] = (sp - cur) / point
    except Exception:
        pass
    return snap


def _current_price_from_market(market: Dict[str, Any]) -> float:
    try:
        bid = float((market or {}).get("bid") or 0.0)
        ask = float((market or {}).get("ask") or 0.0)
    except Exception:
        bid, ask = 0.0, 0.0
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return bid if bid > 0 else ask if ask > 0 else 0.0


def _build_sma_context(market: Dict[str, Any]) -> Dict[str, Any]:
    """Build SMA context with distance and slope information."""
    sma_price = float((market or {}).get("m15_ma") or 0.0)
    point = float((market or {}).get("point") or 0.0)
    atr_points = float((market or {}).get("atr_points") or 0.0)
    slope = (market or {}).get("m15_sma20_slope") or "FLAT"
    if slope not in {"UP", "DOWN", "FLAT"}:
        slope = "FLAT"
    
    cur = _current_price_from_market(market)
    if sma_price <= 0 or cur <= 0 or point <= 0:
        return {
            "price": float(sma_price) if sma_price > 0 else None,
            "distance_points": None,
            "distance_atr_ratio": None,
            "relationship": None,
            "slope": slope,
        }

    distance_points = abs(cur - sma_price) / point
    # When ATR=0, use 999.9 as explicit "extreme deviation" flag for AI prompt
    distance_atr_ratio = (distance_points / atr_points) if atr_points > 0 else 999.9
    relationship = "ABOVE" if cur >= sma_price else "BELOW"
    return {
        "price": float(sma_price),
        "distance_points": float(distance_points),
        "distance_atr_ratio": distance_atr_ratio,
        "relationship": relationship,
        "slope": slope,
    }


def _build_volatility_context(market: Dict[str, Any]) -> Dict[str, Any]:
    cur_atr = float((market or {}).get("atr") or 0.0)
    avg_atr = float((market or {}).get("atr_avg_24h") or 0.0)
    ratio = (cur_atr / avg_atr) if (avg_atr > 0) else None
    return {
        "current_atr": float(cur_atr),
        "average_atr_24h": float(avg_atr) if avg_atr > 0 else None,
        "volatility_ratio": float(ratio) if ratio is not None else None,
    }


def _build_spread_context(market: Dict[str, Any]) -> Dict[str, Any]:
    cur = float((market or {}).get("spread") or 0.0)
    avg = float((market or {}).get("spread_avg_24h") or 0.0)
    ratio = (cur / avg) if (avg > 0) else None
    return {
        "current_spread": float(cur),
        "average_spread": float(avg) if avg > 0 else None,
        "spread_ratio": float(ratio) if ratio is not None else None,
    }


def _compute_session_context(now_ts: Optional[float] = None) -> Dict[str, Any]:
    """Compute trading session context based on UTC hour.
    
    ASIAN: 0-7 UTC (Tokyo/Sydney)
    LONDON: 7-13 UTC (London open)
    NY: 13-21 UTC (NY session)
    YZ: 21-24 UTC (After-hours, low liquidity)
    """
    if now_ts is None:
        now_ts = time.time()
    try:
        dt = datetime.fromtimestamp(float(now_ts), tz=timezone.utc)
        hour = int(dt.hour)
    except Exception:
        hour = int(datetime.now(timezone.utc).hour)

    if 13 <= hour < 21:
        session = "NY"
    elif 7 <= hour < 13:
        session = "LONDON"
    elif 0 <= hour < 7:
        session = "ASIAN"
    else:  # 21-24 UTC
        session = "YZ"

    is_high = bool(session in {"LONDON", "NY"} and (7 <= hour < 21))
    return {
        "current_session": session,
        "is_high_activity": bool(is_high),
        "utc_hour": int(hour),
    }


def _extract_zone_level(signal: Dict[str, Any]) -> Optional[float]:
    def _num(v: Any) -> Optional[float]:
        try:
            fv = float(v)
            return fv if fv > 0 else None
        except Exception:
            return None

    low = _num(signal.get("zone_low") or signal.get("low") or signal.get("bottom") or signal.get("lower"))
    high = _num(signal.get("zone_high") or signal.get("high") or signal.get("top") or signal.get("upper"))
    if low is not None and high is not None:
        return (low + high) / 2.0

    for k in ("zone_price", "zone_mid", "mid", "price", "close", "c"):
        v = _num(signal.get(k))
        if v is not None:
            return v
    return None


def _build_zones_context(symbol: str, now: float, current_price: float) -> Dict[str, Any]:
    """Build zones_context: counts of zones above/below current price.
    
    IMPORTANT: Includes zones from multiple timeframes (m5, m15, h1) to capture
    all structural levels from TradingView alerts. 24h cache retention ensures
    strong zones remain visible for context.
    """
    if current_price <= 0:
        return {"total": 0, "support_count": 0, "resistance_count": 0}

    normalized = _filter_fresh_signals(symbol, float(now))
    support = 0
    resistance = 0

    for s in normalized:
        if not is_zone_presence_signal(s):
            continue
        # Accept zones from multiple timeframes (m5, m15, h1) for structural context
        tf = (s.get("tf") or "").strip().lower()
        if tf and tf not in ["m5", "m15", "h1"]:
            continue  # Skip zones from other timeframes (e.g., m1, h4)
        level = _extract_zone_level(s)
        if level is None:
            continue
        # Zones at current price are counted as support (conservative for entry)
        if level <= current_price:
            support += 1
        else:
            resistance += 1

    total = support + resistance
    return {
        "total": int(total),
        "support_count": int(support),
        "resistance_count": int(resistance),
    }


def _stable_round_time(t: Optional[float], resolution_sec: float = 1.0) -> Optional[float]:
    return _fxai_signal_cache.stable_round_time(t, resolution_sec)


def _signal_dedupe_key(s: Dict[str, Any]) -> str:
    return _fxai_signal_cache.signal_dedupe_key(s)


def _signal_ts_for_index(s: Dict[str, Any]) -> float:
    return _fxai_signal_cache.signal_ts_for_index(s)


def _rebuild_signal_indexes_locked() -> None:
    """Rebuild optional signal cache indexes (must be called with signals_lock held)."""
    global _signals_by_symbol, _signals_buckets_by_symbol
    by_symbol, buckets_by_symbol = _fxai_signal_cache.rebuild_signal_indexes(
        signals_cache=signals_cache,
        bucket_sec=int(SIGNAL_INDEX_BUCKET_SEC or 60),
    )
    _signals_by_symbol = by_symbol
    _signals_buckets_by_symbol = buckets_by_symbol


def _ensure_signal_indexes_locked() -> None:
    global _signals_by_symbol, _signals_buckets_by_symbol
    rebuilt = _fxai_signal_cache.ensure_signal_indexes(
        signal_index_enabled=bool(SIGNAL_INDEX_ENABLED),
        signals_cache=signals_cache,
        signals_by_symbol=_signals_by_symbol,
        bucket_sec=int(SIGNAL_INDEX_BUCKET_SEC or 60),
    )
    if rebuilt is None:
        return
    by_symbol, buckets_by_symbol = rebuilt
    _signals_by_symbol = by_symbol
    _signals_buckets_by_symbol = buckets_by_symbol


def _append_signal_dedup_locked(signal: Dict[str, Any], dedupe_window_sec: float = 120.0) -> bool:
    """Append a signal into cache with de-duplication.

    Returns True if appended, False if treated as a duplicate.
    """
    return _fxai_signal_cache.append_signal_dedup(
        signals_cache=signals_cache,
        signal=signal,
        dedupe_window_sec=float(dedupe_window_sec or 0.0),
        signal_index_enabled=bool(SIGNAL_INDEX_ENABLED),
        bucket_sec=int(SIGNAL_INDEX_BUCKET_SEC or 60),
        signals_by_symbol=_signals_by_symbol,
        signals_buckets_by_symbol=_signals_buckets_by_symbol,
    )


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

    # Sanitize untrusted strings early to reduce prompt-injection surface.
    src = _sanitize_untrusted_text((s.get("source") or "").strip(), max_len=80)
    strength = _sanitize_untrusted_text((s.get("strength") or "").strip(), max_len=24).lower()

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

    # Some alert templates may send direction as `action` instead of `side`.
    # Accept it as an alias (do not override an explicit `side`).
    if (not s.get("side")) and isinstance(s.get("action"), str):
        a = s.get("action")
        a_norm = str(a).strip().lower()
        if a_norm in {"buy", "sell"}:
            s["side"] = a_norm

    if isinstance(s.get("side"), str):
        side_norm = _sanitize_untrusted_text(s["side"], max_len=16).strip().lower()
        s["side"] = side_norm if side_norm else ""
    if isinstance(s.get("strength"), str):
        s["strength"] = _sanitize_untrusted_text(s["strength"], max_len=24).strip().lower()

    if isinstance(s.get("signal_type"), str):
        s["signal_type"] = _sanitize_untrusted_text(s["signal_type"], max_len=32).strip().lower()
    if isinstance(s.get("event"), str):
        s["event"] = _sanitize_untrusted_text(s["event"], max_len=64).strip().lower()
    if isinstance(s.get("confirmed"), str):
        s["confirmed"] = _sanitize_untrusted_text(s["confirmed"], max_len=24).strip().lower()

    # CRITICAL: signal_time must be immutable once set. Never overwrite existing value.
    if "signal_time" not in s or s.get("signal_time") is None:
        parsed = _parse_signal_time_to_epoch(s.get("time"))
        if parsed is not None:
            s["signal_time"] = parsed
        else:
            s["signal_time"] = s.get("receive_time")

    # Keep timeframe (tf) if provided by alert templates.
    tf_in = s.get("tf") or s.get("timeframe") or s.get("interval")
    s["tf"] = _normalize_tf(tf_in)

    # Keep a representative price if provided.
    if "price" not in s:
        s["price"] = s.get("close") or s.get("c")

    return s


def _normalize_tf(tf_value: Any) -> Optional[str]:
    if tf_value is None:
        return None
    try:
        if isinstance(tf_value, (int, float)):
            n = int(tf_value)
            if n <= 0:
                return None
            if n == 60:
                return "h1"
            if n == 240:
                return "h4"
            if n == 1440:
                return "d1"
            return f"m{n}"
        s = str(tf_value).strip().lower().replace(" ", "")
    except Exception:
        return None

    if not s:
        return None

    # Digits-only often mean minutes in TradingView alerts.
    if s.isdigit():
        n = int(s)
        if n == 60:
            return "h1"
        if n == 240:
            return "h4"
        if n == 1440:
            return "d1"
        return f"m{n}"

    if s.endswith("m") and s[:-1].isdigit():
        return f"m{int(s[:-1])}"
    if s.startswith("m") and s[1:].isdigit():
        return f"m{int(s[1:])}"

    if s in {"1h", "h1"}:
        return "h1"
    if s.endswith("h") and s[:-1].isdigit():
        return f"h{int(s[:-1])}"
    if s.startswith("h") and s[1:].isdigit():
        return f"h{int(s[1:])}"

    if s in {"d", "1d", "d1"}:
        return "d1"

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

    tf_key = _normalize_tf(normalized.get("tf")) or "unknown"

    now = float(normalized.get("receive_time") or time.time())
    with _qtrend_lock:
        bucket = _qtrend_state_by_symbol_tf.get(symbol)
        if not isinstance(bucket, dict):
            bucket = {}
            _qtrend_state_by_symbol_tf[symbol] = bucket
        bucket[tf_key] = {
            "side": side,
            "strength": strength,
            "updated_at": now,
            "tf": tf_key,
            "price": normalized.get("price"),
            "confirmed": normalized.get("confirmed"),
            "event": normalized.get("event"),
            "source": normalized.get("source"),
        }


def _get_qtrend_context(symbol: str, now: Optional[float] = None, tf: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    if now is None:
        now = time.time()

    tf_key = _normalize_tf(tf)
    with _qtrend_lock:
        bucket = _qtrend_state_by_symbol_tf.get(sym)

    if not isinstance(bucket, dict) or not bucket:
        return None

    st = None
    if tf_key:
        if tf_key in bucket:
            st = bucket.get(tf_key)
        else:
            # Some alert payloads omit timeframe; we store those under "unknown".
            # When a specific TF is requested but only "unknown" exists, prefer returning it
            # over dropping Q-Trend context entirely.
            if "unknown" in bucket:
                st = bucket.get("unknown")
            else:
                # If caller specifies a timeframe but we don't have that TF, do not mix TFs by default.
                if not Q_TREND_TF_FALLBACK_ENABLED:
                    return None

    if st is None:
        # Fallback (optional): choose the freshest timeframe context.
        best_ts = 0.0
        for cand in bucket.values():
            if not isinstance(cand, dict):
                continue
            ts = float(cand.get("updated_at") or 0.0)
            if ts > best_ts:
                best_ts = ts
                st = cand

    if not isinstance(st, dict):
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
    return _fxai_recent_context.compute_recent_context_signals(
        normalized=normalized,
        now=float(now),
        zone_lookback_sec=ZONE_LOOKBACK_SEC,
    )


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
        if SIGNAL_INDEX_ENABLED:
            _ensure_signal_indexes_locked()
            bucket_sec = int(SIGNAL_INDEX_BUCKET_SEC or 60)
            if bucket_sec <= 0:
                bucket_sec = 60

            start_b = int((center - w) // float(bucket_sec))
            end_b = int((center + w) // float(bucket_sec))

            sym_buckets = _signals_buckets_by_symbol.get(sym, {})
            candidates = []
            for b in range(start_b, end_b + 1):
                candidates.extend(sym_buckets.get(b, []))

            snapshot = [dict(s) for s in candidates if isinstance(s, dict)]
        else:
            snapshot = [dict(s) for s in signals_cache if isinstance(s, dict)]

    return _fxai_window_signals.build_window_signals_payload(
        snapshot=snapshot,
        symbol=sym,
        center_ts=float(center),
        trigger_side=trig_side,
        window_sec=float(w),
        is_qtrend_source=_is_qtrend_source,
    )


def _weight_confirmed(confirmed: str) -> float:
    """bar_close の方が信頼度が高い想定。"""
    c = (confirmed or "").lower()
    if c == "bar_close":
        return 1.0
    if c == "intrabar":
        return 0.6
    return 0.8


def _utc_day_key(ts: Optional[float] = None) -> str:
    return _fxai_metrics.utc_day_key(ts)


def _metrics_mark_dirty_locked() -> None:
    global _metrics_dirty
    _metrics_dirty = True


def _metrics_bucket_score(score: int) -> str:
    return _fxai_metrics.metrics_bucket_score(score)


def _metrics_prune_locked(now: Optional[float] = None) -> None:
    _fxai_metrics.metrics_prune(_metrics, keep_days=int(METRICS_KEEP_DAYS or 14), now=now)


def _metrics_get_bucket_locked(day_key: str, symbol: str) -> Dict[str, Any]:
    return _fxai_metrics.metrics_get_bucket(_metrics, day_key=day_key, symbol=symbol, default_symbol=(SYMBOL or "GOLD"))


def _is_timeout_exc(e: Exception) -> bool:
    name = type(e).__name__.lower()
    if "timeout" in name:
        return True
    try:
        if isinstance(e, TimeoutError):
            return True
    except Exception:
        pass
    return False


def _record_openai_call_metrics(
    *,
    symbol: str,
    kind: str,
    ok: bool,
    attempts: int,
    timeout_attempts: int,
    err_counts: Optional[Dict[str, int]] = None,
) -> None:
    if not ENTRY_METRICS_ENABLED:
        return
    if not symbol:
        symbol = (SYMBOL or "GOLD")

    now = time.time()
    day_key = _utc_day_key(now)
    with _metrics_lock:
        _metrics_prune_locked(now)
        b = _metrics_get_bucket_locked(day_key, symbol)
        _metrics_inc_locked(b, "openai_calls", 1)
        _metrics_inc_locked(b, "openai_success" if ok else "openai_fail", 1)
        _metrics_inc_locked(b, "openai_attempts", max(1, int(attempts)))
        _metrics_inc_locked(b, "openai_retries", max(0, int(attempts) - 1))
        _metrics_inc_locked(b, "openai_timeouts", max(0, int(timeout_attempts)))

        calls_by_kind = b.get("openai_calls_by_kind")
        if not isinstance(calls_by_kind, dict):
            calls_by_kind = {}
            b["openai_calls_by_kind"] = calls_by_kind
        _metrics_inc_map_locked(calls_by_kind, str(kind or "unknown"), 1)

        if not ok:
            fail_by_kind = b.get("openai_fail_by_kind")
            if not isinstance(fail_by_kind, dict):
                fail_by_kind = {}
                b["openai_fail_by_kind"] = fail_by_kind
            _metrics_inc_map_locked(fail_by_kind, str(kind or "unknown"), 1)

        if err_counts:
            fail_by_type = b.get("openai_fail_by_type")
            if not isinstance(fail_by_type, dict):
                fail_by_type = {}
                b["openai_fail_by_type"] = fail_by_type
            for k, v in err_counts.items():
                try:
                    _metrics_inc_map_locked(fail_by_type, str(k), int(v))
                except Exception:
                    continue

        _metrics_mark_dirty_locked()


def _record_ai_validation_failure(*, symbol: str, kind: str) -> None:
    if not ENTRY_METRICS_ENABLED:
        return
    if not symbol:
        symbol = (SYMBOL or "GOLD")

    now = time.time()
    day_key = _utc_day_key(now)
    with _metrics_lock:
        _metrics_prune_locked(now)
        b = _metrics_get_bucket_locked(day_key, symbol)
        _metrics_inc_locked(b, "ai_validation_fail", 1)

        m = b.get("ai_validation_fail_by_kind")
        if not isinstance(m, dict):
            m = {}
            b["ai_validation_fail_by_kind"] = m
        _metrics_inc_map_locked(m, str(kind or "unknown"), 1)
        _metrics_mark_dirty_locked()


def _record_zmq_send_metrics(*, symbol: str, kind: str, ok: bool, err_type: Optional[str] = None) -> None:
    if not ENTRY_METRICS_ENABLED:
        return
    if not symbol:
        symbol = (SYMBOL or "GOLD")

    now = time.time()
    day_key = _utc_day_key(now)
    with _metrics_lock:
        _metrics_prune_locked(now)
        b = _metrics_get_bucket_locked(day_key, symbol)
        _metrics_inc_locked(b, "zmq_send_ok" if ok else "zmq_send_fail", 1)

        by_kind = b.get("zmq_send_by_kind")
        if not isinstance(by_kind, dict):
            by_kind = {}
            b["zmq_send_by_kind"] = by_kind
        _metrics_inc_map_locked(by_kind, str(kind or "unknown"), 1)

        if (not ok) and err_type:
            by_type = b.get("zmq_send_fail_by_type")
            if not isinstance(by_type, dict):
                by_type = {}
                b["zmq_send_fail_by_type"] = by_type
            _metrics_inc_map_locked(by_type, str(err_type), 1)

        _metrics_mark_dirty_locked()


def _zmq_send_json_with_metrics(payload: Dict[str, Any], *, symbol: str, kind: str) -> None:
    def _ok() -> None:
        _record_zmq_send_metrics(symbol=symbol, kind=kind, ok=True)

    def _err(e: Exception) -> None:
        _record_zmq_send_metrics(symbol=symbol, kind=kind, ok=False, err_type=type(e).__name__)

    _zmq_send_json_with_hooks(zmq_socket, payload, on_ok=_ok, on_error=_err)


def _metrics_inc_locked(b: Dict[str, Any], key: str, n: int = 1) -> None:
    _fxai_metrics.inc(b, key, n)


def _metrics_inc_map_locked(m: Dict[str, Any], key: str, n: int = 1) -> None:
    _fxai_metrics.inc_map(m, key, n)


def _metrics_update_guard_stat_locked(guard_stats: Dict[str, Any], name: str, value: Optional[float]) -> None:
    _fxai_metrics.update_guard_stat(guard_stats, name, value)


def _metrics_append_example_locked(examples: List[Dict[str, Any]], ex: Dict[str, Any]) -> None:
    _fxai_metrics.append_example(examples, ex, max_n=int(METRICS_MAX_EXAMPLES or 80))


def _collect_autotune_samples(metrics: Dict[str, Any], *, symbol: Optional[str] = None) -> Dict[str, List[float]]:
    sym = (symbol or SYMBOL or "GOLD").strip().upper()
    by_day = metrics.get("by_day") if isinstance(metrics, dict) else None
    if not isinstance(by_day, dict):
        return {"spread_to_atr": [], "drift_ratio": []}

    spread_to_atr: List[float] = []
    drift_ratio: List[float] = []

    for day in by_day.values():
        if not isinstance(day, dict):
            continue
        # Prefer target symbol bucket; fallback to all buckets only if target missing
        bucket = day.get(sym)
        if isinstance(bucket, dict):
            buckets = [bucket]
        else:
            buckets = [v for v in day.values() if isinstance(v, dict)]

        for bucket in buckets:
            examples = bucket.get("examples")
            if not isinstance(examples, list):
                continue
            for ex in examples:
                if not isinstance(ex, dict):
                    continue
                try:
                    atr_to_spread = ex.get("atr_to_spread")
                    if atr_to_spread is not None and float(atr_to_spread) > 0:
                        spread_to_atr.append(1.0 / float(atr_to_spread))
                except (TypeError, ValueError, ZeroDivisionError):
                    pass

                try:
                    drift_pts = ex.get("drift_points")
                    atr_points = ex.get("atr_points")
                    if drift_pts is not None and atr_points is not None and float(atr_points) > 0:
                        drift_ratio.append(abs(float(drift_pts)) / float(atr_points))
                except (TypeError, ValueError, ZeroDivisionError):
                    pass

    return {"spread_to_atr": spread_to_atr, "drift_ratio": drift_ratio}


def _compute_autotune_settings(metrics: Dict[str, Any], *, symbol: Optional[str] = None) -> Dict[str, float]:
    samples = _collect_autotune_samples(metrics, symbol=symbol)
    spread_vals = samples.get("spread_to_atr") or []
    drift_vals = samples.get("drift_ratio") or []

    min_samples = max(10, int(AUTO_TUNE_MIN_SAMPLES or 0))
    pctl = float(AUTO_TUNE_PCTL or 0.90)

    settings: Dict[str, float] = {}

    if len(spread_vals) >= min_samples:
        s = _percentile(spread_vals, pctl)
        if s is not None:
            s = _clamp(s, SPREAD_MAX_ATR_RATIO_MIN, SPREAD_MAX_ATR_RATIO_MAX)
            settings["SPREAD_MAX_ATR_RATIO"] = float(s)

    if len(drift_vals) >= min_samples:
        d = _percentile(drift_vals, pctl)
        if d is not None:
            d = _clamp(d, DRIFT_LIMIT_ATR_MULT_MIN, DRIFT_LIMIT_ATR_MULT_MAX)
            settings["DRIFT_LIMIT_ATR_MULT"] = float(d)

    return settings


def _apply_autotune_settings(settings: Dict[str, float]) -> None:
    global SPREAD_MAX_ATR_RATIO, DRIFT_LIMIT_ATR_MULT
    now = time.time()
    if not settings:
        with _auto_tune_lock:
            _auto_tune_last_ts = now
        return
    if "SPREAD_MAX_ATR_RATIO" in settings:
        SPREAD_MAX_ATR_RATIO = float(settings["SPREAD_MAX_ATR_RATIO"])
        os.environ["SPREAD_MAX_ATR_RATIO"] = _format_env_value(SPREAD_MAX_ATR_RATIO)
    if "DRIFT_LIMIT_ATR_MULT" in settings:
        DRIFT_LIMIT_ATR_MULT = float(settings["DRIFT_LIMIT_ATR_MULT"])
        os.environ["DRIFT_LIMIT_ATR_MULT"] = _format_env_value(DRIFT_LIMIT_ATR_MULT)


def _maybe_autotune_from_metrics(*, symbol: Optional[str] = None) -> None:
    if not AUTO_TUNE_ENABLED:
        return
    now = time.time()
    with _auto_tune_lock:
        global _auto_tune_last_ts
        if _auto_tune_last_ts > 0 and (now - _auto_tune_last_ts) < float(AUTO_TUNE_INTERVAL_SEC or 0.0):
            return

    with _metrics_lock:
        settings = _compute_autotune_settings(_metrics, symbol=symbol)
    if not settings:
        return

    _apply_autotune_settings(settings)

    if AUTO_TUNE_WRITE_ENV:
        env_path = _resolve_env_path(AUTO_TUNE_ENV_PATH)
        _update_env_file(env_path, settings)

    with _auto_tune_lock:
        _auto_tune_last_ts = now

    print(f"[FXAI] Auto-tuned settings applied: {settings}")


def _record_webhook_metric(symbol: str, sig_type: str, appended: bool) -> None:
    if not ENTRY_METRICS_ENABLED:
        return
    now = time.time()
    day_key = _utc_day_key(now)
    with _metrics_lock:
        _metrics_prune_locked(now)
        b = _metrics_get_bucket_locked(day_key, symbol)
        _metrics_inc_locked(b, "webhooks", 1)
        if not appended:
            _metrics_inc_locked(b, "duplicates", 1)
        if (sig_type or "").strip().lower() == "context":
            _metrics_inc_locked(b, "context_updates", 1)
        if (sig_type or "").strip().lower() == "entry_trigger":
            _metrics_inc_locked(b, "entry_triggers", 1)
        _metrics_mark_dirty_locked()


def _record_entry_outcome(
    *,
    symbol: str,
    outcome: str,
    http_status: int,
    action: Optional[str] = None,
    is_addon: Optional[bool] = None,
    trigger: Optional[Dict[str, Any]] = None,
    qtrend: Optional[Dict[str, Any]] = None,
    market: Optional[Dict[str, Any]] = None,
    window_signals: Optional[Dict[str, Any]] = None,
    zones_confirmed_recent: Optional[int] = None,
    ai_score: Optional[int] = None,
    min_required: Optional[int] = None,
    ai_reason: Optional[str] = None,
    openai_response_id: Optional[str] = None,
    ai_latency_ms: Optional[int] = None,
    attempt_context: Optional[str] = None,
    bypass_ai_throttle: Optional[bool] = None,
) -> None:
    if not ENTRY_METRICS_ENABLED:
        return

    now = time.time()
    day_key = _utc_day_key(now)

    spread_points = None
    atr_to_spread = None
    drift_points = None
    atr_points = None
    if isinstance(market, dict):
        try:
            spread_points = float(market.get("spread") or 0.0)
        except (TypeError, ValueError):
            spread_points = None
        try:
            v = market.get("atr_to_spread")
            atr_to_spread = float(v) if v is not None else None
        except (TypeError, ValueError):
            atr_to_spread = None
        if spread_points is not None and atr_to_spread is not None:
            try:
                atr_points = float(spread_points) * float(atr_to_spread)
            except (TypeError, ValueError, ArithmeticError):
                atr_points = None

        if isinstance(trigger, dict) and trigger:
            try:
                side = str(trigger.get("side") or action or "").strip().lower()
                if side in {"buy", "sell"}:
                    snap = _price_drift_snapshot(trigger.get("price"), market, side)
                    drift_points = snap.get("drift_points")
            except Exception as e:
                print(f"[FXAI][DEBUG] Drift snapshot failed: {e}")
                drift_points = None

    q_side = (qtrend or {}).get("side") if isinstance(qtrend, dict) else None
    q_strength = (qtrend or {}).get("strength") if isinstance(qtrend, dict) else None
    w_counts = (window_signals or {}).get("counts") if isinstance(window_signals, dict) else None

    with _metrics_lock:
        _metrics_prune_locked(now)
        b = _metrics_get_bucket_locked(day_key, symbol)
        _metrics_inc_locked(b, "entry_attempts", 1)

        # Observability counters
        if attempt_context and str(attempt_context).startswith("DE:"):
            _metrics_inc_locked(b, "delayed_entry_attempts", 1)
        if bool(bypass_ai_throttle):
            _metrics_inc_locked(b, "ai_throttle_bypassed", 1)

        guard_stats = b.get("guard_stats")
        if not isinstance(guard_stats, dict):
            guard_stats = {}
            b["guard_stats"] = guard_stats
        _metrics_update_guard_stat_locked(guard_stats, "spread_points", spread_points)
        _metrics_update_guard_stat_locked(guard_stats, "atr_to_spread", atr_to_spread)
        if isinstance(w_counts, dict):
            _metrics_update_guard_stat_locked(guard_stats, "window_aligned", w_counts.get("aligned"))
            _metrics_update_guard_stat_locked(guard_stats, "window_opposed", w_counts.get("opposed"))

        if str(outcome) == "ok":
            _metrics_inc_locked(b, "entry_ok", 1)
        else:
            blocked = b.get("blocked")
            if not isinstance(blocked, dict):
                blocked = {}
                b["blocked"] = blocked
            _metrics_inc_map_locked(blocked, str(outcome), 1)

            if ai_score is not None:
                hist = b.get("ai_score_hist")
                if not isinstance(hist, dict):
                    hist = {}
                    b["ai_score_hist"] = hist
                _metrics_inc_map_locked(hist, _metrics_bucket_score(int(ai_score)), 1)

        examples = b.get("examples")
        if not isinstance(examples, list):
            examples = []
            b["examples"] = examples

        ex = {
            "ts": now,
            "outcome": str(outcome),
            "http": int(http_status),
            "action": action,
            "addon": bool(is_addon) if is_addon is not None else None,
            "attempt_context": (str(attempt_context)[:160] if attempt_context else None),
            "bypass_ai_throttle": bool(bypass_ai_throttle) if bypass_ai_throttle is not None else None,
            "ai_score": int(ai_score) if ai_score is not None else None,
            "min_required": int(min_required) if min_required is not None else None,
            "ai_reason": (str(ai_reason)[:220] if ai_reason else None),
            "openai_response_id": (str(openai_response_id)[:120] if openai_response_id else None),
            "ai_latency_ms": int(ai_latency_ms) if ai_latency_ms is not None else None,
            "spread_points": spread_points,
            "atr_to_spread": atr_to_spread,
            "atr_points": atr_points,
            "drift_points": drift_points,
            "qtrend": {"side": q_side, "strength": q_strength} if (q_side or q_strength) else None,
            "window_counts": w_counts,
            "zones_confirmed_recent": int(zones_confirmed_recent) if zones_confirmed_recent is not None else None,
            "trigger": {
                "source": (trigger or {}).get("source"),
                "event": (trigger or {}).get("event"),
                "side": (trigger or {}).get("side"),
                "signal_time": (trigger or {}).get("signal_time"),
            }
            if isinstance(trigger, dict)
            else None,
        }
        _metrics_append_example_locked(examples, ex)
        _metrics_mark_dirty_locked()

    try:
        _maybe_autotune_from_metrics(symbol=symbol)
    except Exception as e:
        print(f"[FXAI][WARN] Auto-tune update failed: {e}")


def _record_mgmt_outcome(
    *,
    symbol: str,
    action: str,
    confidence: Optional[int],
    min_close_confidence: Optional[int],
    reason: Optional[str] = None,
    trail_mode: Optional[str] = None,
    tp_mode: Optional[str] = None,
    pos_summary: Optional[Dict[str, Any]] = None,
    market: Optional[Dict[str, Any]] = None,
    used_signals: Optional[List[Dict[str, Any]]] = None,
    openai_response_id: Optional[str] = None,
    ai_latency_ms: Optional[int] = None,
) -> None:
    if not ENTRY_METRICS_ENABLED:
        return

    now = time.time()
    day_key = _utc_day_key(now)

    a = str(action or "").strip().upper()
    conf: Optional[int]
    try:
        conf = int(confidence) if confidence is not None else None
    except (TypeError, ValueError):
        conf = None

    min_conf: Optional[int]
    try:
        min_conf = int(min_close_confidence) if min_close_confidence is not None else None
    except (TypeError, ValueError):
        min_conf = None

    holding_sec = None
    move_points = None
    phase_name = "UNKNOWN"
    spread_ratio = None
    distance_atr_ratio = None

    try:
        if isinstance(pos_summary, dict):
            holding_sec = float(pos_summary.get("max_holding_sec") or 0.0)
            v = pos_summary.get("net_move_points")
            move_points = float(v) if v is not None else None
    except (TypeError, ValueError):
        holding_sec = None
        move_points = None

    try:
        if isinstance(market, dict):
            spread_ctx = _build_spread_context(market)
            if isinstance(spread_ctx, dict):
                v = spread_ctx.get("spread_ratio")
                spread_ratio = float(v) if v is not None else None
    except Exception:
        spread_ratio = None

    try:
        if isinstance(market, dict):
            sma_ctx = _build_sma_context(market)
            if isinstance(sma_ctx, dict):
                v = sma_ctx.get("distance_atr_ratio")
                distance_atr_ratio = float(v) if v is not None else None
    except Exception:
        distance_atr_ratio = None

    try:
        spread_points = float((market or {}).get("spread") or 0.0)
        atr_points = float((market or {}).get("atr_points") or 0.0)
        threshold_points = _compute_profit_protect_threshold_points(spread_points, atr_points)
        in_profit_protect = (move_points is not None) and (move_points >= threshold_points)
        in_phase2_by_time = (holding_sec is not None) and (holding_sec >= float(MAX_DEVELOPMENT_SEC or 0.0))
        phase_name = "PROFIT_PROTECT" if (in_profit_protect or in_phase2_by_time) else "DEVELOPMENT"
    except Exception:
        phase_name = "UNKNOWN"

    signal_count = 0
    if isinstance(used_signals, list):
        signal_count = len([s for s in used_signals if isinstance(s, dict)])

    with _metrics_lock:
        _metrics_prune_locked(now)
        b = _metrics_get_bucket_locked(day_key, symbol)

        mgmt = b.get("mgmt")
        if not isinstance(mgmt, dict):
            mgmt = {}
            b["mgmt"] = mgmt

        _metrics_inc_locked(mgmt, "decisions", 1)
        if a == "CLOSE":
            _metrics_inc_locked(mgmt, "close", 1)
        elif a == "HOLD":
            _metrics_inc_locked(mgmt, "hold", 1)

        phase_counts = mgmt.get("phase_counts")
        if not isinstance(phase_counts, dict):
            phase_counts = {}
            mgmt["phase_counts"] = phase_counts
        _metrics_inc_map_locked(phase_counts, phase_name, 1)

        if conf is not None:
            conf_hist = mgmt.get("confidence_hist")
            if not isinstance(conf_hist, dict):
                conf_hist = {}
                mgmt["confidence_hist"] = conf_hist
            _metrics_inc_map_locked(conf_hist, _metrics_bucket_score(conf), 1)

            if min_conf is not None and conf >= min_conf:
                _metrics_inc_locked(mgmt, "at_or_above_threshold", 1)
            if min_conf is not None and a == "CLOSE" and min_conf <= conf <= (min_conf + 5):
                _metrics_inc_locked(mgmt, "close_near_threshold", 1)

        if distance_atr_ratio is not None and a == "CLOSE" and distance_atr_ratio >= 4.0:
            _metrics_inc_locked(mgmt, "close_overextended_sma", 1)
        if spread_ratio is not None and a == "CLOSE" and spread_ratio > 1.5:
            _metrics_inc_locked(mgmt, "close_high_spread", 1)

        examples = mgmt.get("examples")
        if not isinstance(examples, list):
            examples = []
            mgmt["examples"] = examples

        ex = {
            "ts": now,
            "action": a,
            "confidence": conf,
            "min_required": min_conf,
            "reason": (str(reason)[:220] if reason else None),
            "trail_mode": (str(trail_mode).upper() if trail_mode else None),
            "tp_mode": (str(tp_mode).upper() if tp_mode else None),
            "phase": phase_name,
            "holding_sec": holding_sec,
            "move_points": move_points,
            "distance_atr_ratio": distance_atr_ratio,
            "spread_ratio": spread_ratio,
            "signals_used_count": signal_count,
            "openai_response_id": (str(openai_response_id)[:120] if openai_response_id else None),
            "ai_latency_ms": int(ai_latency_ms) if ai_latency_ms is not None else None,
        }
        _metrics_append_example_locked(examples, ex)
        _metrics_mark_dirty_locked()


def _load_metrics() -> None:
    if not METRICS_FILE:
        return
    try:
        data = _fxai_persist.read_json_if_exists(METRICS_FILE, default=None)
        if not isinstance(data, dict):
            return
        with _metrics_lock:
            _metrics.clear()
            _metrics.update(data)
            _metrics.setdefault("started_at", time.time())
            _metrics.setdefault("by_day", {})
            _metrics_prune_locked(time.time())
            global _metrics_dirty, _metrics_last_save_at
            _metrics_dirty = False
            _metrics_last_save_at = time.time()
    except Exception as e:
        print(f"[FXAI][WARN] Failed to load metrics: {e}")


def _save_metrics_locked() -> None:
    if not METRICS_FILE:
        return
    try:
        err = _fxai_persist.atomic_write_json(METRICS_FILE, _metrics, ensure_ascii=False)
        if err:
            raise RuntimeError(err)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to save metrics: {e}")


def _load_cache():
    """起動時にファイルからシグナル履歴を復元する"""
    try:
        data = _fxai_persist.read_json_if_exists(CACHE_FILE, default=None)
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
    """Background loop to flush cache/metrics to disk at a controlled interval."""
    global _cache_last_save_at, _cache_dirty, _metrics_last_save_at, _metrics_dirty

    sleep_sec = _fxai_flush.compute_sleep_sec(float(CACHE_FLUSH_INTERVAL_SEC or 2.0))

    def _enabled() -> bool:
        return bool(CACHE_ASYNC_FLUSH_ENABLED)

    def _warn(msg: str) -> None:
        print(f"[FXAI][WARN] {msg}")

    def _flush_cache_once() -> None:
        global _cache_last_save_at, _cache_dirty
        now = time.time()
        with signals_lock:
            if not _cache_dirty:
                return

            last_save = float(_cache_last_save_at or 0.0)
            last_dirty = float(_cache_last_dirty_at or 0.0)

            if not _fxai_flush.should_flush(
                now=float(now),
                last_save=float(last_save),
                last_dirty=float(last_dirty),
                interval_sec=float(CACHE_FLUSH_INTERVAL_SEC or 2.0),
                force_sec=float(CACHE_FLUSH_FORCE_SEC or 10.0),
            ):
                return

            _save_cache_locked()
            _cache_last_save_at = now
            _cache_dirty = False

    def _flush_metrics_once() -> None:
        global _metrics_last_save_at, _metrics_dirty
        if not ENTRY_METRICS_ENABLED:
            return
        now = time.time()
        with _metrics_lock:
            if not _metrics_dirty:
                return
            # Reuse cache flush cadence; metrics volume is tiny.
            if (now - float(_metrics_last_save_at or 0.0)) < float(CACHE_FLUSH_INTERVAL_SEC or 2.0):
                return
            _save_metrics_locked()
            _metrics_last_save_at = now
            _metrics_dirty = False

    _fxai_flush.run_flush_loop(
        sleep_sec=float(sleep_sec),
        is_enabled=_enabled,
        flush_cache_once=_flush_cache_once,
        flush_metrics_once=_flush_metrics_once,
        warn=_warn,
    )

def _save_cache_locked():
    """シグナルキャッシュをファイルに保存する (Lock保持中に呼ぶこと)"""
    try:
        err = _fxai_persist.atomic_write_json(CACHE_FILE, signals_cache, ensure_ascii=False)
        if err:
            raise RuntimeError(err)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to save cache: {e}")

def _prune_signals_cache_locked(now: float) -> None:
    """期限切れシグナルを削除。

    Zone情報は、
    - 「存在/構造（例: new_zone_confirmed）」は長く保持（デフォルト24h）
    - 「接触/touch（例: zone_retrace_touch）」は短く保持（デフォルト20m）
    - FVG は 15分足対応のため保持を延長（デフォルト20m）
    に分離し、古いtouchを合流として誤認するバグを防ぐ。
    """
    keep_list, changed = _fxai_signal_cache.prune_signals_cache(
        signals_cache=signals_cache,
        now=now,
        zone_lookback_sec=ZONE_LOOKBACK_SEC,
        zone_touch_lookback_sec=ZONE_TOUCH_LOOKBACK_SEC,
        fvg_lookback_sec=FVG_LOOKBACK_SEC,
        signal_lookback_sec=SIGNAL_LOOKBACK_SEC,
    )

    if changed:
        signals_cache[:] = keep_list

    if SIGNAL_INDEX_ENABLED and (changed or (not _signals_by_symbol and signals_cache)):
        _rebuild_signal_indexes_locked()


def _is_zone_presence_signal(s: dict) -> bool:
    return _fxai_signal_cache.is_zone_presence_signal(s)


def _is_zone_touch_signal(s: dict) -> bool:
    return _fxai_signal_cache.is_zone_touch_signal(s)


def _is_fvg_signal(s: dict) -> bool:
    """Return True if a signal represents an FVG (Fair Value Gap) event/presence."""
    try:
        fn = getattr(_fxai_signal_cache, "is_fvg_signal", None)
        if callable(fn):
            return bool(fn(s))
    except Exception:
        pass

    if not isinstance(s, dict):
        return False

    try:
        src = str(s.get("source") or "").strip()
        sig_type = str(s.get("signal_type") or "").strip().lower()
        evt = str(s.get("event") or "").strip().lower()
    except Exception:
        return False

    if src in {"FVG", "LuxAlgo_FVG"}:
        return True
    if "fvg" in sig_type:
        return True
    if "fvg" in evt:
        return True
    return False


def _filter_fresh_signals(symbol: str, now: float) -> list:
    """v2.6の SignalMaxAgeSec 相当：signal_time が古すぎるものを落とす。"""
    sym = (symbol or "").strip().upper()
    with signals_lock:
        if SIGNAL_INDEX_ENABLED:
            _ensure_signal_indexes_locked()
            base = _signals_by_symbol.get(sym, [])
            normalized = [_normalize_signal_fields(s) for s in base if isinstance(s, dict)]
        else:
            normalized = [_normalize_signal_fields(s) for s in signals_cache if s.get("symbol") == symbol]

    return _fxai_signal_cache.filter_fresh_signals_from_normalized(
        normalized=normalized,
        now=now,
        signal_max_age_sec=SIGNAL_MAX_AGE_SEC,
        zone_lookback_sec=ZONE_LOOKBACK_SEC,
        zone_touch_lookback_sec=ZONE_TOUCH_LOOKBACK_SEC,
        fvg_lookback_sec=FVG_LOOKBACK_SEC,
    )


def get_qtrend_anchor_stats(target_symbol: str):
    """fxChartAI v2.6 の『Q-Trend起点で合流を集計』を Python 側で再現。"""
    now = time.time()
    normalized = _filter_fresh_signals(target_symbol, now)
    if not normalized:
        return None

    return _fxai_qtrend.compute_qtrend_anchor_stats(
        target_symbol=target_symbol,
        normalized=normalized,
        now=now,
        confluence_window_sec=CONFLUENCE_WINDOW_SEC,
        min_other_signals_for_entry=MIN_OTHER_SIGNALS_FOR_ENTRY,
        zone_lookback_sec=ZONE_LOOKBACK_SEC,
        zone_touch_lookback_sec=ZONE_TOUCH_LOOKBACK_SEC,
        confluence_debug=bool(CONFLUENCE_DEBUG),
        confluence_debug_max_lines=CONFLUENCE_DEBUG_MAX_LINES,
        weight_confirmed=_weight_confirmed,
    )


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

    rates_m15 = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M15, 0, 30)
    ma15 = 0.0
    m15_slope = "FLAT"
    if rates_m15 is not None and len(rates_m15) > 0:
        # CRITICAL: MT5 returns bars in [newest, ..., oldest] order.
        # Index 0 is the current FORMING bar; skip it to avoid false slope signals.
        closes = [_rate_field(r, "close", 0.0) for r in rates_m15]
        if len(closes) >= 20:
            ma15 = sum(closes[:20]) / 20.0
        else:
            ma15 = sum(closes) / max(1, len(closes))

        if len(closes) >= 23:
            # Use CLOSED bars only (skip the current forming bar at index 0).
            sma1 = sum(closes[1:21]) / 20.0  # SMA(20) as of last closed bar
            sma3 = sum(closes[3:23]) / 20.0  # SMA(20) from 2 closed bars earlier
            delta = sma1 - sma3
        elif len(closes) >= 22:
            # Fallback: still skip current bar, compare last closed vs 1 bar earlier.
            sma1 = sum(closes[1:21]) / 20.0
            sma2 = sum(closes[2:22]) / 20.0
            delta = sma1 - sma2
            # Threshold: 0.01% of price or minimum $0.01 (for XAUUSD ~$2600, thresh ≈ $0.026)
            thresh = max(abs(sma1) * 1e-4, 0.01)
            if delta > thresh:
                m15_slope = "UP"
            elif delta < -thresh:
                m15_slope = "DOWN"
            else:
                m15_slope = "FLAT"

    # ATR: 旧EA(iATR)に寄せて True Range で近似
    rates_m5 = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M5, 0, 60)
    atr = 0.0
    swing_low_20m5  = 0.0   # [LRR COMPAT] recent 20-bar swing low  (→ BUY sweep_extreme)
    swing_high_20m5 = 0.0   # [LRR COMPAT] recent 20-bar swing high (→ SELL sweep_extreme)
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
        # [LRR COMPAT] Swing extreme: last 20 CLOSED M5 bars (skip bar[0] = still forming)
        # BUY → liquidity swept below → swing LOW acts as structure SL anchor
        # SELL → liquidity swept above → swing HIGH acts as structure SL anchor
        if len(rates_m5) >= 22:
            _lows20f  = [v for v in [_rate_field(r, "low",  0.0) for r in rates_m5[1:21]] if v > 0.0]
            _highs20f = [v for v in [_rate_field(r, "high", 0.0) for r in rates_m5[1:21]] if v > 0.0]
            if _lows20f:
                swing_low_20m5  = min(_lows20f)
            if _highs20f:
                swing_high_20m5 = max(_highs20f)

    # 24h average ATR: simplified using recent longer window
    # Full 288-bar rolling ATR is too expensive; use a 60-bar approximation
    atr_avg_24h = 0.0
    rates_m5_60 = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M5, 0, 62)
    if rates_m5_60 is not None and len(rates_m5_60) >= 15:
        highs_60 = [_rate_field(r, "high", 0.0) for r in rates_m5_60]
        lows_60 = [_rate_field(r, "low", 0.0) for r in rates_m5_60]
        closes_60 = [_rate_field(r, "close", 0.0) for r in rates_m5_60]
        trs_60 = []
        for i in range(1, len(rates_m5_60)):
            h = highs_60[i]
            l = lows_60[i]
            pc = closes_60[i - 1]
            tr = max(abs(h - l), abs(h - pc), abs(l - pc))
            if tr > 0:
                trs_60.append(tr)
        if len(trs_60) >= 14:
            # Simple average of TR over 14-period windows
            atr_avg_24h = sum(trs_60[:min(60, len(trs_60))]) / max(1, min(60, len(trs_60)))

    # If market data isn't available, reuse last known ATR for this symbol.
    if atr <= 0:
        atr = float(_last_atr_by_symbol.get(symbol, 0.0) or 0.0)

    info = mt5.symbol_info(symbol)
    point = float(getattr(info, "point", 0.0) or 0.0)

    bid = float(getattr(tick, "bid", 0.0) or 0.0)
    ask = float(getattr(tick, "ask", 0.0) or 0.0)
    spread = ((ask - bid) / point) if point > 0 else 0.0

    # Average spread: compute from actual tick spreads (points) history.
    # This avoids mixing price ranges into spread statistics.
    spread_avg_24h = 0.0
    now_ts = time.time()
    if spread > 0:
        sym = (symbol or "").strip().upper()
        with _spread_history_lock:
            hist = _spread_history_by_symbol.get(sym)
            if hist is None:
                hist = []
                _spread_history_by_symbol[sym] = hist
            
            hist.append((now_ts, float(spread)))
            
            # Prune by time window first (most effective for memory)
            win = float(SPREAD_HISTORY_WINDOW_SEC or 0)
            if win > 0:
                cutoff = now_ts - win
                hist[:] = [x for x in hist if float(x[0]) >= cutoff]
            
            # Then prune by max size (safety cap)
            max_n = int(SPREAD_HISTORY_MAX or 0)
            if max_n > 0 and len(hist) > max_n:
                hist[:] = hist[-max_n:]
            
            # Compute average from remaining samples
            if hist:
                spread_avg_24h = float(sum(v for _, v in hist) / len(hist))

    # Fallback: if bars unavailable or median failed, clamp to reasonable range around current.
    # Avoid setting avg=current when current is anomalous (e.g., spread spike).
    if spread_avg_24h <= 0:
        # Use a conservative default based on typical GOLD spread range (40-80 pts).
        spread_avg_24h = max(40.0, min(80.0, spread)) if spread > 0 else 50.0

    atr_points = (atr / point) if point > 0 else 0.0
    atr_to_spread = (atr_points / spread) if spread > 0 else None

    drift_point = _drift_point_size(symbol, point)
    return {
        "bid": bid,
        "ask": ask,
        "m15_ma": ma15,
        "m15_sma20_slope": m15_slope,
        "atr": atr,
        "atr_avg_24h": atr_avg_24h,
        "point": point,
        "drift_point": drift_point,
        "atr_points": atr_points,
        "atr_to_spread": atr_to_spread,
        "spread": spread,
        "spread_avg_24h": spread_avg_24h,
        "swing_low_20m5":  swing_low_20m5,
        "swing_high_20m5": swing_high_20m5,
    }


def _update_spread_med(symbol: str, spread: float) -> float:
    """Robbins-Monro O(1) rolling median update.  更新式: med += lr * sign(x - med)
    スパイク耐性があり O(n) ソート不要。lrr_brain §7 から移植。
    Returns: new median estimate (points)
    """
    if spread <= 0:
        return 0.0
    sym = (symbol or "").strip().upper()
    with _spread_med_lock:
        med = _spread_med_by_symbol.get(sym, spread)
        diff = spread - med
        med += LRR_SPREAD_MED_LR * (1.0 if diff > 0 else (-1.0 if diff < 0 else 0.0))
        _spread_med_by_symbol[sym] = med
    return med


def get_mt5_position_state(symbol: str):
    try:
        positions = mt5.positions_get(symbol=symbol)
        if positions is None:
            return {"positions_open": 0}
        return {"positions_open": len(positions)}
    except Exception:
        return {"positions_open": 0}


def _mt5_time_to_unix_seconds(v: Any) -> float:
    """Best-effort conversion of MT5 time fields to unix seconds.

    MT5 Python bindings may expose time fields as:
    - int seconds since epoch (time)
    - int milliseconds since epoch (time_msc)
    - datetime (depending on environment/version)
    """
    if v is None:
        return 0.0
    if isinstance(v, datetime):
        try:
            vv = v
            if vv.tzinfo is None:
                vv = vv.replace(tzinfo=timezone.utc)
            return float(vv.timestamp())
        except Exception:
            return 0.0
    try:
        fv = float(v)
    except Exception:
        return 0.0
    if (not math.isfinite(fv)) or fv <= 0:
        return 0.0
    # Heuristic: treat very large values as milliseconds.
    if fv >= 1.0e12:
        return fv / 1000.0
    return fv


def _mt5_position_open_time_seconds(p: Any) -> float:
    """Extract position open time from MT5 position object (best-effort)."""
    # Prefer explicit open/create time fields when available.
    for key in (
        "time",           # Most common: position open time (seconds)
        "time_msc",       # Millisecond variant
        "time_open",      # Explicit open time
        "time_open_msc",
        "time_create",
        "time_create_msc",
        "time_setup",
        "time_setup_msc",
        "time_update",    # Fallback: better than nothing
        "time_update_msc",
    ):
        try:
            raw = getattr(p, key, None)
            if raw is None:
                continue
            ts = _mt5_time_to_unix_seconds(raw)
            if ts > 0:
                if DEBUG_POSITION_TIME:
                    print(f"[DEBUG] _mt5_position_open_time_seconds: key={key}, ts={ts:.1f}")
                return ts
        except Exception:
            continue
    if DEBUG_POSITION_TIME:
        print(f"[DEBUG] _mt5_position_open_time_seconds: No valid time found")
    return 0.0


def get_current_broker_time(symbol: Optional[str] = None) -> float:
    """Return current MT5 broker time as a UNIX timestamp (seconds)."""
    sym = (symbol or SYMBOL or "").strip().upper() or "GOLD"
    try:
        tick = mt5.symbol_info_tick(sym)
        ts = float(getattr(tick, "time", 0.0) or 0.0)
        if ts > 0:
            return ts
    except Exception as e:
        print(f"[ERROR] get_current_broker_time failed for {sym}: {e}")
    # Emergency fallback: UTC time (may reintroduce DST issues).
    print(f"[WARN] Using UTC fallback for broker time (MT5 tick unavailable)")
    return float(datetime.now(timezone.utc).timestamp())


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

    now_ts_for_holding = get_current_broker_time(symbol)
    print(f"[DEBUG] get_mt5_positions_summary: symbol={symbol}, now={now_ts_for_holding}, positions_count={len(positions)}")
    net_volume = 0.0
    total_profit = 0.0
    oldest_open_time = float("inf")
    max_holding = 0.0
    buy_vol = 0.0
    sell_vol = 0.0
    buy_profit = 0.0
    sell_profit = 0.0
    buy_open_px_sum = 0.0
    sell_open_px_sum = 0.0
    buy_count = 0
    sell_count = 0

    for p in positions:
        # Extract basic position data
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
        
        ptype = getattr(p, "type", None)
        
        # Aggregate volume/profit (always, regardless of time validity)
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
        
        # Extract open time for holding duration calculation
        try:
            t_open = _mt5_position_open_time_seconds(p)
        except Exception:
            t_open = 0.0
        
        if t_open <= 0:
            # Position has no valid open time; skip holding time tracking
            if DEBUG_POSITION_TIME:
                print(f"[WARN] get_mt5_positions_summary: missing time for ticket={getattr(p, 'ticket', '?')}")
            continue
        
        # Track oldest position time (will compute max_holding after loop)
        if t_open < oldest_open_time:
            oldest_open_time = t_open

    # Finalize: compute max_holding from oldest position
    if oldest_open_time == float("inf"):
        oldest_open_time = None
        max_holding = 0.0
        if DEBUG_POSITION_TIME:
            print(f"[WARN] get_mt5_positions_summary: No valid times for {len(positions)} positions")
    else:
        raw_holding = now_ts_for_holding - oldest_open_time
        # MT5サーバー時刻のズレ対応（小さなズレのみ補正、大きなズレは異常として扱う）
        if raw_holding < 0:
            # 負の値: サーバー時刻が未来
            if raw_holding >= -300:  # 5分以内のズレは許容（abs補正）
                if DEBUG_POSITION_TIME:
                    print(f"[WARN] Small negative holding_sec={raw_holding:.1f}s (using abs)")
                max_holding = abs(raw_holding)
            else:  # 5分超のズレは異常（0として扱う）
                if DEBUG_POSITION_TIME:
                    print(f"[WARN] Large negative holding_sec={raw_holding:.1f}s (server time way ahead, treating as 0)")
                max_holding = 0.0
        else:
            max_holding = raw_holding
        if DEBUG_POSITION_TIME:
            print(f"[DEBUG] max_holding_sec={max_holding:.1f}s (oldest={oldest_open_time:.1f})")

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
        "oldest_open_time": oldest_open_time,
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


def check_trading_hours(symbol: str) -> bool:
    """Return True if trading is allowed based on broker server time.
    
    Guards:
    - 23:50-23:59: Market closing (prevent new entries, consider closing positions)
    - 00:00-00:30: Market opening (wait for stable spread)
    """
    broker_now = get_current_broker_time(symbol)
    dt = datetime.fromtimestamp(float(broker_now), tz=timezone.utc)
    h, m = dt.hour, dt.minute
    
    if (h == 23 and m >= 50) or (h == 0 and m <= 30):
        print(f"[Market Guard] Server Time: {h:02d}:{m:02d} - Market Closing/Opening Logic")
        return False
    return True


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


def _call_openai_with_retry(prompt: str, *, symbol: Optional[str] = None, kind: str = "unknown") -> Optional[Dict[str, Any]]:
    if not client:
        return None

    data, err_counts, timeout_attempts, attempts, last_err = _call_openai_json_with_retry(
        client=client,
        model=OPENAI_MODEL,
        prompt=prompt,
        timeout_sec=API_TIMEOUT_SEC,
        retry_count=API_RETRY_COUNT,
        retry_wait_sec=API_RETRY_WAIT_SEC,
    )

    ok = bool(isinstance(data, dict))
    try:
        _record_openai_call_metrics(
            symbol=(symbol or (SYMBOL or "GOLD")),
            kind=kind,
            ok=ok,
            attempts=max(1, int(attempts or 1)),
            timeout_attempts=int(timeout_attempts),
            err_counts=(err_counts or None),
        )
    except Exception:
        pass

    if ok:
        return data

    if last_err is not None:
        print(f"[FXAI][AI] failed: {last_err}")
    return None


@app.errorhandler(413)
def _handle_413(e):
    # Werkzeug raises RequestEntityTooLarge (413) when MAX_CONTENT_LENGTH is exceeded.
    return "Request too large", 413


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

    out: Dict[str, Any] = {"confluence_score": score, "lot_multiplier": lot_mult, "reason": reason}
    # optional meta for observability
    if "_openai_response_id" in decision:
        out["_openai_response_id"] = decision.get("_openai_response_id")
    if "_ai_latency_ms" in decision:
        out["_ai_latency_ms"] = decision.get("_ai_latency_ms")
    return out


def _build_entry_logic_prompt(
    symbol: str,
    market: dict,
    stats: dict,
    action: str,
    normalized_trigger: Optional[Dict[str, Any]] = None,
    qtrend_context: Optional[Dict[str, Any]] = None,
    attempt_context: Optional[str] = None,
) -> str:
    """Entry context prompt (new spec).

    - Entry is triggered by Lorentzian (entry_trigger).
    - Q-Trend is environment context only (direction + strength).
    - Zones/FVG are additional evidence context.
    """

    if not stats:
        now = time.time()
        current_price = _current_price_from_market(market)
        minimal_payload = {
            "symbol": symbol,
            "proposed_action": action,
            "attempt_context": (str(attempt_context)[:160] if attempt_context else None),
            "trigger": {
                "source": (normalized_trigger or {}).get("source"),
                "side": (normalized_trigger or {}).get("side"),
                "signal_type": (normalized_trigger or {}).get("signal_type"),
                "event": (normalized_trigger or {}).get("event"),
                "confirmed": (normalized_trigger or {}).get("confirmed"),
                "signal_time": (normalized_trigger or {}).get("signal_time"),
                "price": (normalized_trigger or {}).get("price"),
            },
            "qtrend_context": qtrend_context,
            "zones_context": _build_zones_context(symbol, now, current_price),
            "sma_context": _build_sma_context(market),
            "volatility_context": _build_volatility_context(market),
            "spread_context": _build_spread_context(market),
            "session_context": _compute_session_context(now),
            "mt5_positions_summary": (stats or {}).get("mt5_positions_summary"),
            "signals_window": (stats or {}).get("window_signals"),
            "market": {
                "bid": market.get("bid"),
                "ask": market.get("ask"),
                "m15_sma20": market.get("m15_ma"),
                "atr_m5_approx": market.get("atr"),
                "spread_points": market.get("spread"),
            },
            "price_drift": _price_drift_snapshot((normalized_trigger or {}).get("price"), market, (normalized_trigger or {}).get("side")),
            "note": "No aggregated evidence stats available yet. Score conservatively.",
        }
        if PROMPT_COMPACT_ENABLED:
            try:
                minimal_payload = _compact_for_prompt(
                    minimal_payload,
                    max_list_items=max(5, int(PROMPT_MAX_LIST_ITEMS or 20)),
                    max_str_len=max(120, int(PROMPT_MAX_STR_LEN or 600)),
                )
            except Exception:
                pass
        return (
            _fxai_prompts_text.ENTRY_LOGIC_MINIMAL_PREFIX
            + (f"AttemptContext: {str(attempt_context)[:160]}\n" if attempt_context else "")
            + _fxai_prompts_text.ENTRY_LOGIC_MINIMAL_SUFFIX
            + json.dumps(minimal_payload, ensure_ascii=False)
        )

    base = _build_entry_filter_prompt(symbol, market, stats, action, normalized_trigger=normalized_trigger, qtrend_context=qtrend_context)
    return (
        _fxai_prompts_text.ENTRY_LOGIC_FULL_PREFIX
        + (f"AttemptContext: {str(attempt_context)[:160]}\n" if attempt_context else "")
        + _fxai_prompts_text.ENTRY_LOGIC_FULL_SUFFIX_BEFORE_BASE
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

    trigger_age_sec = int(now - float((normalized_trigger or {}).get("signal_time") or 0.0)) if (normalized_trigger or {}).get("signal_time") else None
    qt_age_sec = int(now - float((qtrend_context or {}).get("updated_at") or 0.0)) if (qtrend_context or {}).get("updated_at") else None
    price_drift = _price_drift_snapshot((normalized_trigger or {}).get("price"), market, trigger_side)
    current_price = _current_price_from_market(market)
    
    # Compute additional market contexts with fallback handling
    try:
        zones_context = _build_zones_context(symbol, now, current_price)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build zones_context for entry: {e}")
        zones_context = {"total": 0, "support_count": 0, "resistance_count": 0}
    
    try:
        sma_context = _build_sma_context(market)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build sma_context for entry: {e}")
        sma_context = None
    
    try:
        volatility_context = _build_volatility_context(market)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build volatility_context for entry: {e}")
        volatility_context = None
    
    try:
        spread_context = _build_spread_context(market)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build spread_context for entry: {e}")
        spread_context = None
    
    try:
        session_context = _compute_session_context(now)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build session_context for entry: {e}")
        session_context = None

    payload = _fxai_entry_payload.build_entry_filter_payload(
        symbol=symbol,
        action=action,
        now_ts=float(now),
        normalized_trigger=(normalized_trigger or None),
        trigger_age_sec=trigger_age_sec,
        stats_window_signals=stats.get("window_signals"),
        mt5_positions_summary=stats.get("mt5_positions_summary"),
        zones_context=zones_context,
        sma_context=sma_context,
        volatility_context=volatility_context,
        spread_context=spread_context,
        session_context=session_context,
        qt_available=bool(qt_side),
        qt_side=qt_side,
        qt_dir=qt_dir,
        qt_strength_norm=qt_strength_norm,
        qt_age_sec=qt_age_sec,
        qt_alignment_vs_trigger=alignment,
        qt_source=(qtrend_context or {}).get("source"),
        q_age_sec_legacy=q_age_sec,
        strong_flag=bool(strong_flag),
        is_strong_momentum=bool(is_strong_momentum),
        q_trigger_type=q_trigger_type,
        momentum_factor=float(momentum_factor),
        confirm_u=confirm_u,
        confirm_n=confirm_n,
        opp_u=opp_u,
        opp_n=opp_n,
        confluence_score=int(confluence_score),
        confluence_score_base=int(confluence_score_base),
        strong_bonus=int(strong_bonus),
        opposition_score=int(opposition_score),
        w_confirm=float(w_confirm),
        w_oppose=float(w_oppose),
        fvg_same=int(fvg_same),
        fvg_opp=int(fvg_opp),
        zones_same=int(zones_same),
        zones_opp=int(zones_opp),
        osgfc_side=str(osgfc_side or ""),
        osgfc_align=str(osgfc_align or ""),
        bid=float(market.get("bid") or 0.0),
        ask=float(market.get("ask") or 0.0),
        m15_sma20=float(market.get("m15_ma") or 0.0),
        m15_trend=str(m15_trend),
        trend_alignment=str(trend_align),
        atr_m5_approx=market.get("atr"),
        atr_points_approx=market.get("atr_points"),
        atr_to_spread_approx=market.get("atr_to_spread"),
        spread_points=float(spread_points),
        spread_flag=str(spread_flag),
        price_drift=price_drift,
        local_multiplier=float(local_multiplier),
        entry_freshness_sec=float(ENTRY_FRESHNESS_SEC or 30.0),
    )

    if PROMPT_COMPACT_ENABLED:
        try:
            payload = _compact_for_prompt(
                payload,
                max_list_items=max(5, int(PROMPT_MAX_LIST_ITEMS or 20)),
                max_str_len=max(120, int(PROMPT_MAX_STR_LEN or 600)),
            )
        except Exception:
            pass

    return _fxai_prompts_text.ENTRY_FILTER_PROMPT_PREFIX + json.dumps(payload, ensure_ascii=False)



def _validate_ai_close_decision(decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """期待するJSON: {confidence: 0-100, reason: str, trail_mode: "WIDE"|"NORMAL"|"TIGHT", tp_mode: "WIDE"|"NORMAL"|"TIGHT"}
    
    NOTE: trail_mode は ZMQ 経由で MT5 に送信され、動的 SL トレーリングに使用。
          tp_mode は ZMQ 経由で MT5 に送信され、動的 TP トレーリングに使用。
    """
    if not isinstance(decision, dict):
        return None
    
    # _safe_int が既に int を返すため、_clamp の結果も int
    confidence = _safe_int(decision.get("confidence"), AI_CLOSE_DEFAULT_CONFIDENCE)
    confidence = _clamp(confidence, 0, 100)  # 既に int なので冗長な int() 不要
    
    reason = decision.get("reason")
    if not isinstance(reason, str):
        reason = ""
    reason = reason.strip()

    # trail_mode の検証（動的SLトレーリングストップ機能用）
    trail_mode = str(decision.get("trail_mode", "NORMAL")).upper()
    if trail_mode not in {"WIDE", "NORMAL", "TIGHT"}:
        trail_mode = "NORMAL"

    # REVIEW: tp_mode の検証（動的TPトレーリング機能用）
    tp_mode = str(decision.get("tp_mode", "NORMAL")).upper()
    if tp_mode not in {"WIDE", "NORMAL", "TIGHT"}:
        tp_mode = "NORMAL"

    out: Dict[str, Any] = {"confidence": confidence, "reason": reason, "trail_mode": trail_mode, "tp_mode": tp_mode}
    
    # オプショナルなメタデータを保持
    for key in ("_openai_response_id", "_ai_latency_ms"):
        if key in decision:
            out[key] = decision[key]
    
    return out


def _compute_profit_protect_threshold_points(spread_points: float, atr_points: float) -> float:
    """Compute profit protection threshold (shared logic for management and pyramiding)."""
    try:
        return max(float(spread_points) * 4.0, float(atr_points) * 0.9)
    except Exception:
        return float(spread_points) * 4.0 if spread_points > 0 else 0.0


def _build_close_logic_prompt(
    symbol: str,
    market: dict,
    stats: Optional[dict],
    pos_summary: dict,
    latest_signal: dict,
    recent_signals: Optional[List[dict]] = None,
) -> str:
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

    # Phase Management helpers (keep it lightweight; this only affects the prompt)
    holding_sec = float(pos_summary.get("max_holding_sec") or 0.0)
    spread_points = float(market.get("spread") or 0.0)
    atr_points = float(market.get("atr_points") or 0.0)
    point = float(market.get("point") or 0.0)

    move_points_pts = (move_points / point) if (point > 0 and move_points != 0.0) else (0.0 if point > 0 else None)

    # Breakeven-like band: within noise/spread/volatility. Used only as a hint to AI.
    # CRITICAL: Tightened from ATR*0.15 to ATR*0.10 to reduce false positives.
    breakeven_band_points = 0.0
    try:
        breakeven_band_points = max(
            0.0,
            float(spread_points) * BREAKEVEN_BAND_SPREAD_MULTIPLIER,
            float(atr_points) * BREAKEVEN_BAND_ATR_MULTIPLIER
        )
    except Exception:
        breakeven_band_points = max(0.0, float(spread_points) * BREAKEVEN_BAND_SPREAD_MULTIPLIER)

    is_breakeven_like = False
    if move_points_pts is not None:
        is_breakeven_like = abs(float(move_points_pts)) <= float(breakeven_band_points)
    else:
        # If we cannot compute points, do NOT attempt money-based breakeven detection.
        # Money PnL scale differs by account/broker/lot size and may misclassify phases.
        is_breakeven_like = False

    profit_protect_threshold_points = _compute_profit_protect_threshold_points(spread_points, atr_points)

    in_profit_protect = (move_points_pts is not None) and (move_points_pts >= profit_protect_threshold_points)
    
    # CRITICAL FIX: Phase 1 (DEVELOPMENT) duration enforcement.
    # OLD: (holding_sec < 900) OR is_breakeven_like → caused infinite Phase 1 in ranging markets.
    # NEW: (holding_sec < MAX_DEVELOPMENT_SEC) AND is_breakeven_like → strict time limit.
    # Day trading principle: positions that fail to reach breakeven within 30 min are losing trades.
    max_dev_sec = MAX_DEVELOPMENT_SEC  # already float from config
    
    # Phase 2 (PROFIT_PROTECT) triggers: profit threshold OR time limit exceeded
    in_phase2_by_time = (holding_sec >= max_dev_sec)
    
    # Phase determination: Phase 2 takes priority over Phase 1.
    # All non-Phase-2 positions default to DEVELOPMENT (育成フェーズ: insensitive to noise).
    phase_name = "PROFIT_PROTECT" if (in_profit_protect or in_phase2_by_time) else "DEVELOPMENT"
    
    # Note: in_development is computed for debugging/logging but NOT used in phase_name determination.
    # Phase 1 condition: short duration AND near breakeven (computed for future extensibility).
    in_development = (holding_sec < max_dev_sec) and is_breakeven_like

    # Recent signals gathered during settle window (if available). Keep it small.
    recent_signals_clean: List[dict] = []
    if isinstance(recent_signals, list) and recent_signals:
        for s in recent_signals:
            if isinstance(s, dict):
                recent_signals_clean.append(s)
    # Cap to avoid prompt bloat (we only need the latest few).
    if len(recent_signals_clean) > 8:
        recent_signals_clean = recent_signals_clean[-8:]

    # Compute additional market contexts with fallback handling
    current_price = _current_price_from_market(market)
    
    try:
        zones_context = _build_zones_context(symbol, now, current_price)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build zones_context: {e}")
        zones_context = {"total": 0, "support_count": 0, "resistance_count": 0}
    
    try:
        sma_context = _build_sma_context(market)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build sma_context: {e}")
        sma_context = None
    
    try:
        volatility_context = _build_volatility_context(market)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build volatility_context: {e}")
        volatility_context = None
    
    try:
        spread_context = _build_spread_context(market)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build spread_context: {e}")
        spread_context = None
    
    try:
        session_context = _compute_session_context(now)
    except Exception as e:
        print(f"[FXAI][WARN] Failed to build session_context: {e}")
        session_context = None

    # 型安全な閾値取得 (空文字列や不正値への耐性)
    try:
        min_conf = int(AI_CLOSE_MIN_CONFIDENCE or 0) if AI_CLOSE_MIN_CONFIDENCE else None
    except (ValueError, TypeError):
        min_conf = 65  # デフォルト値
    
    payload = _fxai_close_payload.build_close_logic_payload(
        symbol=symbol,
        phase_name=str(phase_name),
        breakeven_band_points=float(breakeven_band_points),
        profit_protect_threshold_points=float(profit_protect_threshold_points),
        holding_sec=float(holding_sec),
        move_points_pts=float(move_points_pts) if move_points_pts is not None else None,
        is_breakeven_like=bool(is_breakeven_like),
        in_profit_protect=bool(in_profit_protect),
        in_development=bool(in_development),
        pos_summary=pos_summary,
        net_avg_open=float(net_avg_open),
        move_points_price_delta=float(move_points),
        q_age_sec=int(q_age_sec),
        stats=stats,
        market=market,
        zones_context=zones_context,
        sma_context=sma_context,
        volatility_context=volatility_context,
        spread_context=spread_context,
        session_context=session_context,
        latest_signal=latest_signal,
        recent_signals_clean=recent_signals_clean,
        min_close_confidence=min_conf,
    )

    if PROMPT_COMPACT_ENABLED:
        try:
            payload = _compact_for_prompt(
                payload,
                max_list_items=max(5, int(PROMPT_MAX_LIST_ITEMS or 20)),
                max_str_len=max(120, int(PROMPT_MAX_STR_LEN or 600)),
            )
        except Exception:
            pass

    return _fxai_prompts_text.CLOSE_LOGIC_PROMPT_PREFIX + json.dumps(payload, ensure_ascii=False)


def _ai_close_hold_decision(
    symbol: str,
    market: dict,
    stats: Optional[dict],
    pos_summary: dict,
    latest_signal: dict,
    recent_signals: Optional[List[dict]] = None,
) -> Optional[Dict[str, Any]]:
    if not client:
        return None
    prompt = _build_close_logic_prompt(symbol, market, stats, pos_summary, latest_signal, recent_signals=recent_signals)
    decision = _call_openai_with_retry(prompt, symbol=symbol, kind="close_hold")
    if not decision:
        print("[FXAI][AI] No response from AI (close/hold). Using fallback.")
        return None
    validated = _validate_ai_close_decision(decision)
    if not validated:
        print("[FXAI][AI] Invalid AI response (close/hold). Using fallback.")
        try:
            _record_ai_validation_failure(symbol=symbol, kind="close_hold")
        except Exception:
            pass
        return None
    return validated


def _ai_entry_score(
    symbol: str,
    market: dict,
    stats: dict,
    action: str,
    normalized_trigger: Optional[Dict[str, Any]] = None,
    qtrend_context: Optional[Dict[str, Any]] = None,
    attempt_context: Optional[str] = None,
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
        attempt_context=attempt_context,
    )
    decision = _call_openai_with_retry(prompt, symbol=symbol, kind="entry_score")
    if not decision:
        print("[FXAI][AI] No response from AI (entry score).")
        return None

    validated = _validate_ai_entry_score(decision)
    if not validated:
        print("[FXAI][AI] Invalid AI response (entry score).")
        try:
            _record_ai_validation_failure(symbol=symbol, kind="entry_score")
        except Exception:
            pass
        return None

    return validated


def _attempt_entry_from_lorentzian(
    symbol: str,
    normalized_trigger: dict,
    now: float,
    pos_summary: Optional[dict] = None,
    bypass_ai_throttle: bool = False,
    attempt_context: Optional[str] = None,
) -> tuple[str, int]:
    """New spec entry flow.

    - Triggered ONLY by Lorentzian entry_trigger.
    - Q-Trend is context-only and read from in-memory cache.
    - Zones/FVG context read from signals_cache.
    """
    trig_side = (normalized_trigger.get("side") or "").strip().lower()
    action = "BUY" if trig_side == "buy" else "SELL" if trig_side == "sell" else ""

    # Hard guard: if an entry placement is already running for this symbol, skip this attempt.
    if _is_entry_processing_locked(symbol, now=now):
        _set_status(last_result="Entry processing locked", last_result_at=time.time())
        print(f"[FXAI][ENTRY] Skip: entry processing locked for {symbol}")
        return "Entry processing locked", 200

    # Dedupe: block re-processing of the same Lorentzian trigger.
    dedupe_key = _entry_trigger_dedupe_key(symbol, action, normalized_trigger)
    if _is_trigger_already_processed(symbol, dedupe_key, now=now):
        _set_status(last_result="Entry trigger already processed", last_result_at=time.time())
        print(f"[FXAI][ENTRY] Skip: trigger already processed {dedupe_key}")
        return "Trigger already processed", 200

    _set_status(
        last_entry_attempt_at=time.time(),
        last_entry_attempt_context=(str(attempt_context)[:160] if attempt_context else None),
        last_entry_bypass_ai_throttle=bool(bypass_ai_throttle),
    )

    def _finish(
        message: str,
        http_status: int,
        outcome: str,
        *,
        ai_score: Optional[int] = None,
        min_required: Optional[int] = None,
        ai_reason: Optional[str] = None,
        openai_response_id: Optional[str] = None,
        ai_latency_ms: Optional[int] = None,
        market: Optional[Dict[str, Any]] = None,
        qtrend_ctx: Optional[Dict[str, Any]] = None,
        window_signals: Optional[Dict[str, Any]] = None,
        zones_confirmed_recent: Optional[int] = None,
    ) -> tuple[str, int]:
        try:
            _record_entry_outcome(
                symbol=symbol,
                outcome=outcome,
                http_status=int(http_status),
                action=action,
                is_addon=is_addon,
                trigger=normalized_trigger,
                qtrend=qtrend_ctx,
                market=market,
                window_signals=window_signals,
                zones_confirmed_recent=zones_confirmed_recent,
                ai_score=ai_score,
                min_required=min_required,
                ai_reason=ai_reason,
                openai_response_id=openai_response_id,
                ai_latency_ms=ai_latency_ms,
                attempt_context=(attempt_context if attempt_context else None),
                bypass_ai_throttle=(bool(bypass_ai_throttle) if bypass_ai_throttle is not None else None),
            )
        except Exception:
            pass

        try:
            _status_append_recent_entry_event(
                {
                    "ts": time.time(),
                    "symbol": symbol,
                    "action": action,
                    "outcome": str(outcome),
                    "http": int(http_status),
                    "attempt_context": (str(attempt_context)[:160] if attempt_context else None),
                    "bypass_ai_throttle": bool(bypass_ai_throttle),
                    "ai_score": int(ai_score) if ai_score is not None else None,
                    "min_required": int(min_required) if min_required is not None else None,
                    "ai_latency_ms": int(ai_latency_ms) if ai_latency_ms is not None else None,
                    "openai_response_id": (str(openai_response_id)[:80] if openai_response_id else None),
                    "ai_reason": (str(ai_reason)[:160] if ai_reason else None),
                    "trigger": {
                        "source": (normalized_trigger or {}).get("source"),
                        "event": (normalized_trigger or {}).get("event"),
                        "side": (normalized_trigger or {}).get("side"),
                        "signal_time": (normalized_trigger or {}).get("signal_time"),
                    }
                    if isinstance(normalized_trigger, dict)
                    else None,
                }
            )
        except Exception:
            pass
        return message, int(http_status)

    if action not in {"BUY", "SELL"}:
        _set_status(last_result="Invalid Lorentzian side", last_result_at=time.time())
        return _finish("Invalid trigger", 400, "invalid_trigger")

    # Safety: block entries when EA heartbeat is stale/missing.
    if not _heartbeat_is_fresh(now_ts=now):
        _set_status(last_result="Blocked by heartbeat", last_result_at=time.time())
        return _finish("Blocked by heartbeat", 503, "blocked_heartbeat")

    # Market guard: block new entries during close/open hours based on broker time.
    if not check_trading_hours(symbol):
        # If positions are open, send CLOSE signal to EA.
        if pos_summary and int((pos_summary or {}).get("positions_open") or 0) > 0:
            try:
                _zmq_send_json_with_metrics(
                    {"type": "CLOSE", "reason": "market_guard_close"},
                    symbol=symbol,
                    kind="market_guard_close"
                )
                print(f"[Market Guard] CLOSE signal sent for {symbol} (positions open during guard window)")
            except Exception as e:
                print(f"[Market Guard] Failed to send CLOSE: {e}")
        
        _set_status(
            last_result="Blocked by market guard",
            last_result_at=time.time(),
            last_entry_guard={"market_guard": True, "reason": "close_or_open_window"},
        )
        return _finish("Blocked by market guard", 200, "blocked_market_guard")

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
            return _finish("Skip (net_side unknown)", 200, "skip_net_side_unknown")

        # Design decision: Skip opposite-direction signals to maintain strategy clarity.
        # Entry AI focuses on same-direction add-ons; Management AI handles reversal detection.
        # This prevents judgment conflicts and maintains clear role separation.
        if (not ALLOW_ADD_ON_ENTRIES) or (net_side != trig_side):
            _set_status(last_result="Skip entry (position open, opposite direction)", last_result_at=time.time())
            return _finish("Skip (position open, opposite direction)", 200, "skip_position_open")

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
                return _finish("Skip (add-on limit)", 200, "skip_addon_limit")
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

    atr_eff_price = _effective_atr_price(market)
    try:
        point = float(market.get("point") or 0.0)
    except Exception:
        point = 0.0
    atr_eff_points = (atr_eff_price / point) if point > 0 else 0.0
    spread_to_atr = (spread_points / atr_eff_points) if atr_eff_points > 0 else None
    # Fallback: when the market payload omits the atr_to_spread field, derive it
    # from the locally computed spread/ATR ratio so the soft-pass guard can still
    # evaluate EV quality instead of silently falling through to hard-block.
    if atr_to_spread_v is None and spread_to_atr is not None and spread_to_atr > 0.0:
        try:
            atr_to_spread_v = 1.0 / spread_to_atr
        except Exception:
            pass

    if spread_points <= 0:
        _set_status(last_result="Blocked (no spread)", last_result_at=time.time())
        return _finish("Blocked (no spread)", 503, "blocked_no_spread", market=market)

    if float(ENTRY_MAX_SPREAD_POINTS or 0.0) > 0 and spread_points >= float(ENTRY_MAX_SPREAD_POINTS):
        _set_status(
            last_result="Blocked (spread too wide)",
            last_result_at=time.time(),
            last_entry_guard={"spread_points": spread_points, "max": float(ENTRY_MAX_SPREAD_POINTS)},
        )
        return _finish("Blocked (spread too wide)", 200, "blocked_spread", market=market)

    if SPREAD_MAX_ATR_RATIO > 0.0 and spread_to_atr is not None:
        if spread_to_atr > SPREAD_MAX_ATR_RATIO:
            # Soft path: keep hard reject for clearly bad EV, but allow AI to judge
            # borderline sessions where ATR/spread is still tradable.
            if SPREAD_VS_ATR_SOFT_MIN > 0.0 and atr_to_spread_v is not None and atr_to_spread_v >= SPREAD_VS_ATR_SOFT_MIN:
                _set_status(
                    last_result="Spread high vs ATR (soft pass)",
                    last_result_at=time.time(),
                    last_entry_guard={
                        "spread_to_atr": spread_to_atr,
                        "max": SPREAD_MAX_ATR_RATIO,
                        "atr_to_spread": atr_to_spread_v,
                        "soft_min": SPREAD_VS_ATR_SOFT_MIN,
                        "mode": "soft_pass_to_ai",
                    },
                )
            else:
                _set_status(
                    last_result="Blocked (spread too wide vs ATR)",
                    last_result_at=time.time(),
                    last_entry_guard={"spread_to_atr": spread_to_atr, "max": SPREAD_MAX_ATR_RATIO},
                )
                return _finish("Blocked (spread too wide vs ATR)", 200, "blocked_spread_vs_atr", market=market)
    elif float(ENTRY_MIN_ATR_TO_SPREAD or 0.0) > 0 and (atr_to_spread_v is not None):
        if atr_to_spread_v < float(ENTRY_MIN_ATR_TO_SPREAD):
            _set_status(
                last_result="Blocked (ATR too small vs spread)",
                last_result_at=time.time(),
                last_entry_guard={"atr_to_spread": atr_to_spread_v, "min": float(ENTRY_MIN_ATR_TO_SPREAD)},
            )
            return _finish("Blocked (ATR too small vs spread)", 200, "blocked_atr_to_spread", market=market)

    # -------------------------------------------------------------------------
    # LRR Hard Guards (fxai_lrr_brain §31 から移植 / AI 呼び出し前に機械的拒絶)
    # -------------------------------------------------------------------------

    # SpreadMed を更新し、spike 倍率チェックにも使う
    spread_med = _update_spread_med(symbol, spread_points)

    # [LRR-1] EV Hard Reject: ATR/spread が LRR_EV_HARD_MIN 未満 → コスト対効果ゼロ
    if LRR_EV_HARD_MIN > 0 and atr_to_spread_v is not None and atr_to_spread_v < LRR_EV_HARD_MIN:
        _set_status(
            last_result="Blocked (LRR: EV hard reject)",
            last_result_at=time.time(),
            last_entry_guard={"lrr_ev": atr_to_spread_v, "min": LRR_EV_HARD_MIN},
        )
        print(f"[LRR_GUARD] EV hard reject: atr/spread={atr_to_spread_v:.1f} < {LRR_EV_HARD_MIN}")
        return _finish("Blocked (LRR: EV hard reject)", 200, "lrr_blocked_ev", market=market)

    # SpreadMed×2.5 スパイク検知 (Robbins-Monro 中央値の 2.5 倍超 = 異常ワイド)
    if spread_med > 0 and spread_points > spread_med * 2.5:
        _set_status(
            last_result="Blocked (LRR: spread spike vs median)",
            last_result_at=time.time(),
            last_entry_guard={"spread": spread_points, "spread_med": round(spread_med, 2), "ratio": round(spread_points / spread_med, 2)},
        )
        print(f"[LRR_GUARD] Spread spike: {spread_points:.1f} > med {spread_med:.1f}×2.5")
        return _finish("Blocked (LRR: spread spike)", 200, "lrr_blocked_spread_spike", market=market)

    # [LRR-2] Distance Hard Reject: M15 SMA20 乖離 / ATR >= LRR_DIST_HARD_REJECT
    if LRR_DIST_HARD_REJECT > 0:
        _sma_ctx = _build_sma_context(market)
        _dar = _sma_ctx.get("distance_atr_ratio")
        if _dar is not None and _dar < 999.0 and _dar >= LRR_DIST_HARD_REJECT:
            _set_status(
                last_result="Blocked (LRR: distance overextended)",
                last_result_at=time.time(),
                last_entry_guard={"distance_atr_ratio": round(_dar, 2), "max": LRR_DIST_HARD_REJECT,
                                  "relationship": _sma_ctx.get("relationship")},
            )
            print(f"[LRR_GUARD] Distance hard reject: d/atr={_dar:.2f} >= {LRR_DIST_HARD_REJECT}")
            return _finish("Blocked (LRR: distance overextended)", 200, "lrr_blocked_dist", market=market)

    # [LRR-3] Panic Vol Hard Reject: ATR_now / ATR_24h >= LRR_VOL_PANIC_RATIO
    if LRR_VOL_PANIC_RATIO > 0:
        _vol_ctx = _build_volatility_context(market)
        _vr = _vol_ctx.get("volatility_ratio")
        if _vr is not None and _vr >= LRR_VOL_PANIC_RATIO:
            _set_status(
                last_result="Blocked (LRR: panic volatility)",
                last_result_at=time.time(),
                last_entry_guard={"volatility_ratio": round(_vr, 2), "max": LRR_VOL_PANIC_RATIO},
            )
            print(f"[LRR_GUARD] Panic vol reject: vol_ratio={_vr:.2f} >= {LRR_VOL_PANIC_RATIO}")
            return _finish("Blocked (LRR: panic volatility)", 200, "lrr_blocked_panic_vol", market=market)

    # -------------------------------------------------------------------------

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
            return _finish("Blocked (cooldown)", 200, "blocked_cooldown", market=market)

    # --- Price drift guard (especially important for delayed entry) ---
    # Prefer prompt-driven decisioning; only hard-block when explicitly enabled.
    dynamic_limit_points = _dynamic_drift_limit_points(market)
    ok_drift, drift_reason = _check_price_drift(
        normalized_trigger.get("price"),
        market,
        trig_side,
        limit_points=dynamic_limit_points,
    )
    if DRIFT_HARD_BLOCK_ENABLED and (not ok_drift):
        _set_status(
            last_result="Blocked (price drift)",
            last_result_at=time.time(),
            last_entry_guard={
                "price_drift": True,
                "reason": drift_reason,
                "limit_points": float(dynamic_limit_points or 0.0),
                "signal_price": normalized_trigger.get("price"),
                "bid": market.get("bid"),
                "ask": market.get("ask"),
            },
        )
        print(f"[FXAI][WARN] Blocked entry due to price drift: {drift_reason}")
        return _finish("Blocked (price drift)", 200, "blocked_price_drift", market=market)

    trig_st = float(normalized_trigger.get("signal_time") or normalized_trigger.get("receive_time") or now)

    # Optional: small settle window to capture near-immediate context AFTER the trigger.
    # This is intentionally short (e.g., 5-30s) to avoid waiting minutes.
    try:
        wait_sec = float(POST_TRIGGER_WAIT_SEC or 0.0)
    except Exception:
        wait_sec = 0.0
    if wait_sec > 0:
        try:
            age = float(now - trig_st)
            remain = float(wait_sec - age)
            if remain > 0:
                time.sleep(min(remain, wait_sec))
                now = float(time.time())
        except Exception:
            pass

    window_sec = float(CONFLUENCE_WINDOW_SEC or 300)
    window_signals = _collect_window_signals_around_trigger(symbol, trig_st, trig_side, window_sec=window_sec)
    evidence_ctx = _collect_recent_context_signals(symbol, now)

    trig_tf = _normalize_tf(normalized_trigger.get("tf") or normalized_trigger.get("timeframe") or normalized_trigger.get("interval"))
    qtrend_ctx = _get_qtrend_context(symbol, now=now, tf=trig_tf)

    # If timeframe-matched Q-Trend context is unavailable (often because Q-Trend alerts omit `tf`),
    # derive it from the most recent Q-Trend signal in the same confluence window.
    if qtrend_ctx is None and isinstance(window_signals, dict):
        candidates: List[Dict[str, Any]] = []
        for group in ("aligned", "opposed", "neutral"):
            for ev in (window_signals.get(group) or []):
                if not isinstance(ev, dict):
                    continue
                src = (ev.get("source") or "")
                if not _is_qtrend_source(str(src)):
                    continue
                side = (ev.get("side") or "").strip().lower()
                if side not in {"buy", "sell"}:
                    continue
                candidates.append(ev)

        if candidates:
            best = max(candidates, key=lambda x: float(x.get("signal_time") or 0.0))
            st = float(best.get("signal_time") or 0.0)
            if st > 0 and (float(Q_TREND_MAX_AGE_SEC or 300) <= 0 or (now - st) <= float(Q_TREND_MAX_AGE_SEC or 300)):
                qtrend_ctx = {
                    "side": (best.get("side") or "").strip().lower(),
                    "strength": (best.get("strength") or "normal"),
                    "updated_at": st,
                    "tf": trig_tf or "unknown",
                    "price": best.get("price"),
                    "confirmed": best.get("confirmed"),
                    "event": best.get("event"),
                    "source": best.get("source"),
                    "derived_from_window": True,
                }

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
    if attempt_context:
        attempt_key = f"{attempt_key}:{attempt_context}"

    now_mono = time.time()
    if (not bypass_ai_throttle) and _last_ai_attempt_key == attempt_key and (now_mono - float(_last_ai_attempt_at or 0.0)) < AI_ENTRY_THROTTLE_SEC:
        _set_status(last_result="AI throttled", last_result_at=time.time())
        return _finish("AI throttled", 200, "ai_throttled", market=market, qtrend_ctx=qtrend_ctx, window_signals=window_signals, zones_confirmed_recent=int(stats.get("zones_confirmed_recent") or 0))
    _last_ai_attempt_key = attempt_key
    _last_ai_attempt_at = now_mono

    ai_decision = _ai_entry_score(
        symbol,
        market,
        stats,
        action,
        normalized_trigger=normalized_trigger,
        qtrend_context=qtrend_ctx,
        attempt_context=attempt_context,
    )
    if not ai_decision:
        _set_status(last_result="Blocked by AI (no score)", last_result_at=time.time())
        return _finish("Blocked by AI", 503, "blocked_ai_no_score", market=market, qtrend_ctx=qtrend_ctx, window_signals=window_signals, zones_confirmed_recent=int(stats.get("zones_confirmed_recent") or 0))

    ai_score = int(ai_decision.get("confluence_score") or 0)
    ai_reason = (ai_decision.get("reason") or "").strip()
    lot_mult = float(ai_decision.get("lot_multiplier") or 1.0)
    openai_response_id = ai_decision.get("_openai_response_id")
    ai_latency_ms = ai_decision.get("_ai_latency_ms")

    if is_addon:
        min_addon_score = int(ADDON_MIN_AI_SCORE or AI_ENTRY_MIN_SCORE)
        if ai_score < min_addon_score:
            reason_snip = ai_reason[:160] if ai_reason else ""
            _set_status(
                last_result="Blocked add-on by AI",
                last_result_at=time.time(),
                last_entry_guard={
                    "addon": True,
                    "ai_score": ai_score,
                    "min_required": min_addon_score,
                    "ai_reason": reason_snip,
                    "qtrend": {
                        "available": bool(qtrend_ctx),
                        "side": (qtrend_ctx or {}).get("side"),
                        "strength": (qtrend_ctx or {}).get("strength"),
                    },
                    "window_counts": (window_signals or {}).get("counts"),
                    "zones_confirmed_recent": int(stats.get("zones_confirmed_recent") or 0),
                },
            )
            print(f"[FXAI][ENTRY] Blocked add-on by AI: score={ai_score} min={min_addon_score} reason={reason_snip}")
            return _finish(
                "Blocked add-on by AI",
                403,
                "blocked_addon_ai",
                ai_score=ai_score,
                min_required=min_addon_score,
                ai_reason=reason_snip,
                openai_response_id=openai_response_id,
                ai_latency_ms=ai_latency_ms,
                market=market,
                qtrend_ctx=qtrend_ctx,
                window_signals=window_signals,
                zones_confirmed_recent=int(stats.get("zones_confirmed_recent") or 0),
            )
    else:
        min_entry_score = int(AI_ENTRY_MIN_SCORE)
        try:
            if (
                bool(qtrend_ctx)
                and (str((qtrend_ctx or {}).get("strength") or "").strip().lower() == "strong")
                and (str((qtrend_ctx or {}).get("side") or "").strip().lower() == trig_side)
            ):
                min_entry_score = int(AI_ENTRY_MIN_SCORE_STRONG_ALIGNED or AI_ENTRY_MIN_SCORE)
        except Exception:
            min_entry_score = int(AI_ENTRY_MIN_SCORE)

        if ai_score < min_entry_score:
            reason_snip = ai_reason[:160] if ai_reason else ""
            _set_status(
                last_result="Blocked by AI",
                last_result_at=time.time(),
                last_entry_guard={
                    "addon": False,
                    "ai_score": ai_score,
                    "min_required": int(min_entry_score),
                    "ai_reason": reason_snip,
                    "qtrend": {
                        "available": bool(qtrend_ctx),
                        "side": (qtrend_ctx or {}).get("side"),
                        "strength": (qtrend_ctx or {}).get("strength"),
                    },
                    "window_counts": (window_signals or {}).get("counts"),
                    "zones_confirmed_recent": int(stats.get("zones_confirmed_recent") or 0),
                },
            )
            print(f"[FXAI][ENTRY] Blocked by AI: score={ai_score} min={int(min_entry_score)} reason={reason_snip}")
            return _finish(
                f"Blocked by AI (score={ai_score}, reason={reason_snip})",
                403,
                "blocked_ai_score",
                ai_score=ai_score,
                min_required=int(min_entry_score),
                ai_reason=reason_snip,
                openai_response_id=openai_response_id,
                ai_latency_ms=ai_latency_ms,
                market=market,
                qtrend_ctx=qtrend_ctx,
                window_signals=window_signals,
                zones_confirmed_recent=int(stats.get("zones_confirmed_recent") or 0),
            )

    # Final multiplier: start from 1.0, modulate by AI.
    final_multiplier = float(_clamp(1.0 * lot_mult, 0.5, 2.0))

    # Re-check heartbeat just before sending the order (avoid race).
    if not _heartbeat_is_fresh(now_ts=time.time()):
        _set_status(last_result="Blocked by heartbeat", last_result_at=time.time())
        return _finish("Blocked by heartbeat", 503, "blocked_heartbeat", market=market, qtrend_ctx=qtrend_ctx, window_signals=window_signals, zones_confirmed_recent=int(stats.get("zones_confirmed_recent") or 0))

    reason = ai_reason or "lorentzian_entry"
    # [LRR COMPAT] sweep_extreme: structure-based SL anchor for LRR.mq5.
    # BUY → 20-bar swing low; SELL → 20-bar swing high.
    # When >0, LRR.mq5 uses max(structure_dist, atr_noise_floor) for SL → tighter, more precise.
    _sweep_extreme = float(
        market.get("swing_low_20m5" if str(action).upper() == "BUY" else "swing_high_20m5") or 0.0
    )
    payload = {
        "type": "ORDER",
        "action": action,
        "symbol": symbol,
        "atr": float(market.get("atr") or 0.0),
        "sweep_extreme": _sweep_extreme,
        "multiplier": final_multiplier,
        "reason": reason,
        "ai_confidence": ai_score,
        "ai_reason": ai_reason,
    }

    # Acquire processing lock right before order placement to prevent duplicate orders.
    lock_ctx = f"{action}:{(normalized_trigger.get('signal_time') or normalized_trigger.get('receive_time') or '')}:{(attempt_context or '')}"
    if not _try_acquire_entry_processing_lock(symbol, context=lock_ctx, now=time.time()):
        _set_status(last_result="Entry processing locked", last_result_at=time.time())
        print(f"[FXAI][ENTRY] Skip: could not acquire entry processing lock for {symbol}")
        return _finish(
            "Entry processing locked",
            200,
            "entry_locked",
            ai_score=ai_score,
            min_required=int((ADDON_MIN_AI_SCORE if is_addon else (AI_ENTRY_MIN_SCORE_STRONG_ALIGNED if (qtrend_ctx and str((qtrend_ctx or {}).get("strength") or "").strip().lower() == "strong" and str((qtrend_ctx or {}).get("side") or "").strip().lower() == trig_side) else AI_ENTRY_MIN_SCORE)) or AI_ENTRY_MIN_SCORE),
            ai_reason=ai_reason,
            openai_response_id=openai_response_id,
            ai_latency_ms=ai_latency_ms,
            market=market,
            qtrend_ctx=qtrend_ctx,
            window_signals=window_signals,
            zones_confirmed_recent=int(stats.get("zones_confirmed_recent") or 0),
        )

    # Mark as processed early (safe against re-entry). TTL is short and configurable.
    _mark_trigger_processed(symbol, dedupe_key, now=time.time())

    try:
        _zmq_send_json_with_metrics({**payload}, symbol=symbol, kind="entry_order")

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
        return _finish(
            "OK",
            200,
            "ok",
            ai_score=ai_score,
            min_required=int((ADDON_MIN_AI_SCORE if is_addon else (AI_ENTRY_MIN_SCORE_STRONG_ALIGNED if (qtrend_ctx and str((qtrend_ctx or {}).get("strength") or "").strip().lower() == "strong" and str((qtrend_ctx or {}).get("side") or "").strip().lower() == trig_side) else AI_ENTRY_MIN_SCORE)) or AI_ENTRY_MIN_SCORE),
            ai_reason=ai_reason,
            openai_response_id=openai_response_id,
            ai_latency_ms=ai_latency_ms,
            market=market,
            qtrend_ctx=qtrend_ctx,
            window_signals=window_signals,
            zones_confirmed_recent=int(stats.get("zones_confirmed_recent") or 0),
        )
    except Exception as e:
        _set_status(last_result="Order send failed", last_result_at=time.time(), last_order_error=str(e))
        print(f"[FXAI][ZMQ][ERROR] Order send failed: {e}")
        return _finish(
            "Order send failed",
            503,
            "order_send_failed",
            ai_score=ai_score,
            min_required=int((ADDON_MIN_AI_SCORE if is_addon else (AI_ENTRY_MIN_SCORE_STRONG_ALIGNED if (qtrend_ctx and str((qtrend_ctx or {}).get("strength") or "").strip().lower() == "strong" and str((qtrend_ctx or {}).get("side") or "").strip().lower() == trig_side) else AI_ENTRY_MIN_SCORE)) or AI_ENTRY_MIN_SCORE),
            ai_reason=ai_reason,
            openai_response_id=openai_response_id,
            ai_latency_ms=ai_latency_ms,
            market=market,
            qtrend_ctx=qtrend_ctx,
            window_signals=window_signals,
            zones_confirmed_recent=int(stats.get("zones_confirmed_recent") or 0),
        )
    finally:
        _release_entry_processing_lock(symbol)


@app.route('/webhook', methods=['POST'])
def webhook():
    """TradingViewからのシグナルを受信し、AIフィルターを適用してエントリーを決定。"""
    if REQUIRE_HTTPS and (not _request_is_https(request)):
        return "HTTPS required", 403

    # Simple in-memory rate limiting (disabled by default).
    client_ip = _get_client_ip(request)
    if not _rate_limit_allow(f"webhook:{client_ip}", limit_per_min=WEBHOOK_RATE_LIMIT_RPM):
        _set_status(last_result="Rate limited", last_result_at=time.time())
        return "Too Many Requests", 429

    # Optional shared-secret authentication
    if WEBHOOK_TOKEN:
        header_token = (request.headers.get("X-Webhook-Token") or "").strip()
        body_token = ""
        if ALLOW_BODY_TOKEN_AUTH:
            data_peek = request.get_json(silent=True)
            body_token = (data_peek.get("token") if isinstance(data_peek, dict) else "") or ""
        if header_token != WEBHOOK_TOKEN and str(body_token).strip() != WEBHOOK_TOKEN:
            return "Unauthorized", 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return "Invalid data", 400

    # Prevent accidental token propagation/logging.
    try:
        data.pop("token", None)
    except Exception:
        pass

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

    source_in = _sanitize_untrusted_text((data.get("source") or "").strip(), max_len=80)
    source_for_cache = source_in
    if not source_for_cache and ASSUME_ACTION_IS_QTREND and inferred_side:
        source_for_cache = "Q-Trend"

    signal = {
        "symbol": symbol,
        "source": source_for_cache,
        "side": data.get("side") or inferred_side,
        "tf": data.get("tf") or data.get("timeframe") or data.get("interval"),
        "price": data.get("price") or data.get("close") or data.get("c"),
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
        cache_before = len(signals_cache)
        appended = _append_signal_dedup_locked(normalized)
        cache_after = len(signals_cache)
        _prune_signals_cache_locked(now)
        cache_after_prune = len(signals_cache)
        _mark_cache_dirty_locked(now)
        if not CACHE_ASYNC_FLUSH_ENABLED:
            _save_cache_locked()
        
        if appended:
            print(f"[DEBUG] Cache: before={cache_before}, after_append={cache_after}, after_prune={cache_after_prune}, dirty={_cache_dirty}")
        elif cache_before == 0:
            print(f"[WARN] Failed to append signal to empty cache!")

    # Record webhook-level metrics (even if duplicate)
    try:
        _record_webhook_metric(symbol, (normalized.get("signal_type") or ""), bool(appended))
    except Exception:
        pass

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
                "tf": normalized.get("tf"),
                "price": normalized.get("price"),
                "signal_type": normalized.get("signal_type"),
                "event": normalized.get("event"),
                "confirmed": normalized.get("confirmed"),
                "strength": normalized.get("strength"),
                "time": normalized.get("signal_time"),
                "raw_tf": _sanitize_untrusted_text(data.get("tf") or data.get("timeframe") or data.get("interval"), max_len=16),
                "raw_price": data.get("price") or data.get("close") or data.get("c"),
                "raw_source": _sanitize_untrusted_text(source_in, max_len=80),
                "raw_action": _sanitize_untrusted_text(raw_action, max_len=16),
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
    # When positions are open, defer the management AI by a short settle window so that
    # near-simultaneous context alerts can be included and we avoid conflicting decisions.
    pos_summary = get_mt5_positions_summary(symbol)
    if int(pos_summary.get("positions_open") or 0) > 0:
        if AI_CLOSE_ENABLED and _schedule_deferred_mgmt(symbol, normalized, float(now)):
            # PYRAMID-DEFER: allow same-direction deferred entry while management is deferred.
            _try_schedule_pyramid_entry(symbol, pending_entry_trigger, normalized_trigger, pos_summary, float(now))
            return "Mgmt deferred", 200
        # If settle window disabled, run immediately (serialized).
        with _mgmt_lock:
            r = _run_position_management_once(symbol, normalized, float(now))
        if r is not None:
            return r

        # After management decision (unless CLOSED), allow add-on entries by falling through to ENTRY section.

    # --- ENTRY (Lorentzian) ---
    if pending_entry_trigger and normalized_trigger:
        # Entry race guard: if an entry placement is ongoing, do not start another evaluation.
        if _is_entry_processing_locked(symbol, now=now):
            _set_status(last_result="Entry processing locked", last_result_at=time.time())
            print(f"[FXAI][ENTRY] Webhook skip: entry processing locked for {symbol}")
            return "Entry processing locked", 200

        # Trigger dedupe at webhook-level to avoid redundant AI calls.
        try:
            trig_side = (normalized_trigger.get("side") or "").strip().lower()
            action = "BUY" if trig_side == "buy" else "SELL" if trig_side == "sell" else ""
            if action:
                dk = _entry_trigger_dedupe_key(symbol, action, normalized_trigger)
                if _is_trigger_already_processed(symbol, dk, now=now):
                    _set_status(last_result="Entry trigger already processed", last_result_at=time.time())
                    print(f"[FXAI][ENTRY] Webhook skip: trigger already processed {dk}")
                    return "Trigger already processed", 200
        except Exception:
            pass

        # Register pending trigger for delayed re-evaluation (even if the first attempt fails).
        if DELAYED_ENTRY_ENABLED:
            _upsert_pending_entry(symbol, normalized_trigger, float(now))

        # Signal Aggregation Window: defer the initial AI evaluation to include out-of-order context.
        if _schedule_deferred_entry(symbol, normalized_trigger, float(now)):
            return "Entry deferred", 200

        # Fallback: run immediately when aggregation window is disabled.
        if DELAYED_ENTRY_ENABLED:
            _reserve_pending_entry_attempt(symbol, float(time.time()), retry_signal=normalized_trigger)

        with _entry_lock:
            last_sent_before = float(_last_order_sent_at_by_symbol.get(symbol, 0.0) or 0.0)
        resp = _attempt_entry_from_lorentzian(symbol, normalized_trigger, now, pos_summary=pos_summary)
        with _entry_lock:
            last_sent_after = float(_last_order_sent_at_by_symbol.get(symbol, 0.0) or 0.0)
        if DELAYED_ENTRY_ENABLED and (last_sent_after > last_sent_before):
            _clear_pending_entry(symbol, reason="order_sent")
        return resp

    # --- DELAYED_ENTRY (re-evaluate on later supportive context) ---
    if DELAYED_ENTRY_ENABLED:
        delayed_resp = _maybe_attempt_delayed_entry(symbol, normalized, float(now))
        if delayed_resp is not None:
            return delayed_resp

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
    if REQUIRE_HTTPS and (not _request_is_https(request)):
        return "HTTPS required", 403
    client_ip = _get_client_ip(request)
    if not _rate_limit_allow(f"status:{client_ip}", limit_per_min=STATUS_RATE_LIMIT_RPM):
        return "Too Many Requests", 429
    # Optional shared-secret authentication (same rule as /webhook)
    if WEBHOOK_TOKEN:
        header_token = (request.headers.get("X-Webhook-Token") or "").strip()
        if header_token != WEBHOOK_TOKEN:
            return "Unauthorized", 401
    return _get_status_snapshot(), 200


@app.route('/metrics', methods=['GET'])
def metrics():
    if REQUIRE_HTTPS and (not _request_is_https(request)):
        return "HTTPS required", 403
    client_ip = _get_client_ip(request)
    if not _rate_limit_allow(f"metrics:{client_ip}", limit_per_min=METRICS_RATE_LIMIT_RPM):
        return "Too Many Requests", 429
    # Optional shared-secret authentication (same rule as /webhook)
    if WEBHOOK_TOKEN:
        header_token = (request.headers.get("X-Webhook-Token") or "").strip()
        if header_token != WEBHOOK_TOKEN:
            return "Unauthorized", 401

    if not ensure_runtime_initialized():
        return "Runtime init failed", 503

    if not ENTRY_METRICS_ENABLED:
        return {"ok": True, "enabled": False}, 200

    with _metrics_lock:
        snap = json.loads(json.dumps(_metrics))  # cheap deep-copy (small data)

    snap["ok"] = True
    snap["enabled"] = True
    snap["config"] = {
        "OPENAI_MODEL": str(OPENAI_MODEL),
        "API_TIMEOUT_SEC": float(API_TIMEOUT_SEC),
        "API_RETRY_COUNT": int(API_RETRY_COUNT),
        "API_RETRY_WAIT_SEC": float(API_RETRY_WAIT_SEC),
        "AI_ENTRY_MIN_SCORE": int(AI_ENTRY_MIN_SCORE),
        "AI_ENTRY_MIN_SCORE_STRONG_ALIGNED": int(AI_ENTRY_MIN_SCORE_STRONG_ALIGNED),
        "ADDON_MIN_AI_SCORE": int(ADDON_MIN_AI_SCORE),
        "CONFLUENCE_WINDOW_SEC": int(CONFLUENCE_WINDOW_SEC),
        "POST_TRIGGER_WAIT_SEC": float(POST_TRIGGER_WAIT_SEC or 0.0),
        "ENTRY_POST_SIGNAL_WAIT_SEC": float(ENTRY_POST_SIGNAL_WAIT_SEC or 0.0),
        "ENTRY_POST_SIGNAL_MAX_WAIT_SEC": float(ENTRY_POST_SIGNAL_MAX_WAIT_SEC or 0.0),
        "DELAYED_ENTRY_ENABLED": bool(DELAYED_ENTRY_ENABLED),
        "DELAYED_ENTRY_MAX_WAIT_SEC": float(DELAYED_ENTRY_MAX_WAIT_SEC or 0.0),
        "DELAYED_ENTRY_MIN_RETRY_INTERVAL_SEC": float(DELAYED_ENTRY_MIN_RETRY_INTERVAL_SEC or 0.0),
        "DELAYED_ENTRY_MAX_ATTEMPTS": int(DELAYED_ENTRY_MAX_ATTEMPTS or 0),
        "ENTRY_FRESHNESS_SEC": float(ENTRY_FRESHNESS_SEC or 0.0),
        "DRIFT_LIMIT_POINTS": float(DRIFT_LIMIT_POINTS or 0.0),
        "DRIFT_LIMIT_ATR_MULT": float(DRIFT_LIMIT_ATR_MULT or 0.0),
        "DRIFT_LIMIT_MIN_POINTS": float(DRIFT_LIMIT_MIN_POINTS or 0.0),
        "DRIFT_LIMIT_MAX_POINTS": float(DRIFT_LIMIT_MAX_POINTS or 0.0),
        "ATR_SPIKE_CAP_MULT": float(ATR_SPIKE_CAP_MULT or 0.0),
        "ATR_FLOOR_MULT": float(ATR_FLOOR_MULT or 0.0),
        "DRIFT_HARD_BLOCK_ENABLED": bool(DRIFT_HARD_BLOCK_ENABLED),
        "ENTRY_MAX_SPREAD_POINTS": float(ENTRY_MAX_SPREAD_POINTS),
        "ENTRY_MIN_ATR_TO_SPREAD": float(ENTRY_MIN_ATR_TO_SPREAD),
        "SPREAD_MAX_ATR_RATIO": float(SPREAD_MAX_ATR_RATIO or 0.0),
        "SPREAD_VS_ATR_SOFT_MIN": SPREAD_VS_ATR_SOFT_MIN,
        "AUTO_TUNE_ENABLED": bool(AUTO_TUNE_ENABLED),
        "AUTO_TUNE_INTERVAL_SEC": float(AUTO_TUNE_INTERVAL_SEC or 0.0),
        "AUTO_TUNE_PCTL": float(AUTO_TUNE_PCTL or 0.0),
        "AUTO_TUNE_MIN_SAMPLES": int(AUTO_TUNE_MIN_SAMPLES or 0),
        "AUTO_TUNE_ENV_PATH": str(AUTO_TUNE_ENV_PATH or ".env"),
        "AUTO_TUNE_WRITE_ENV": bool(AUTO_TUNE_WRITE_ENV),
        "ENTRY_COOLDOWN_SEC": float(ENTRY_COOLDOWN_SEC),
        "ENTRY_PROCESSING_LOCK_MAX_SEC": float(ENTRY_PROCESSING_LOCK_MAX_SEC or 0.0),
        "ENTRY_TRIGGER_DEDUPE_TTL_SEC": float(ENTRY_TRIGGER_DEDUPE_TTL_SEC or 0.0),
        "Q_TREND_MAX_AGE_SEC": int(Q_TREND_MAX_AGE_SEC),
        "Q_TREND_TF_FALLBACK_ENABLED": bool(Q_TREND_TF_FALLBACK_ENABLED),
        "SIGNAL_LOOKBACK_SEC": int(SIGNAL_LOOKBACK_SEC),
        "ZONE_LOOKBACK_SEC": int(ZONE_LOOKBACK_SEC),
        "ZONE_TOUCH_LOOKBACK_SEC": int(ZONE_TOUCH_LOOKBACK_SEC),
        "SIGNAL_INDEX_ENABLED": bool(SIGNAL_INDEX_ENABLED),
        "SIGNAL_INDEX_BUCKET_SEC": int(SIGNAL_INDEX_BUCKET_SEC),
        "REQUIRE_HTTPS": bool(REQUIRE_HTTPS),
        "WEBHOOK_RATE_LIMIT_RPM": int(WEBHOOK_RATE_LIMIT_RPM),
        "STATUS_RATE_LIMIT_RPM": int(STATUS_RATE_LIMIT_RPM),
        "METRICS_RATE_LIMIT_RPM": int(METRICS_RATE_LIMIT_RPM),
        "MAX_REQUEST_BYTES": int(MAX_REQUEST_BYTES or 0),
        "ALLOW_BODY_TOKEN_AUTH": bool(ALLOW_BODY_TOKEN_AUTH),
        "PROMPT_COMPACT_ENABLED": bool(PROMPT_COMPACT_ENABLED),
        "PROMPT_MAX_LIST_ITEMS": int(PROMPT_MAX_LIST_ITEMS),
        "PROMPT_MAX_STR_LEN": int(PROMPT_MAX_STR_LEN),
    }

    return Response(json.dumps(snap, ensure_ascii=False), mimetype="application/json"), 200


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
        app.run(host='0.0.0.0', port=WEBHOOK_PORT, threaded=True)
    except OSError as e:
        print(f"[FXAI][FATAL] Failed to start web server on 0.0.0.0:{WEBHOOK_PORT}. OS error: {e}")
        print("[FXAI][HINT] If you must use port 80, ensure nothing else is listening on it (IIS/Apache/Nginx/another Python process).")
        print("[FXAI][HINT] Windows: `netstat -ano | findstr :80` then `tasklist /fi \"PID eq <pid>\"`.")
        raise
