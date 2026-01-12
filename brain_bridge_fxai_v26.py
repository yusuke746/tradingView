import os
import json
import time
import socket
from datetime import datetime, timezone
from threading import Lock
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
SYMBOL = os.getenv("SYMBOL", "GOLD")

# --- v2.6 confluence params ---
CONFLUENCE_LOOKBACK_SEC = int(os.getenv("CONFLUENCE_LOOKBACK_SEC", "600"))
Q_TREND_MAX_AGE_SEC = int(os.getenv("Q_TREND_MAX_AGE_SEC", "300"))
MIN_OTHER_SIGNALS_FOR_ENTRY = int(os.getenv("MIN_OTHER_SIGNALS_FOR_ENTRY", "1"))

# --- Signal retention / freshness ---
SIGNAL_LOOKBACK_SEC = int(os.getenv("SIGNAL_LOOKBACK_SEC", "1200"))     # 通常シグナル: 20分
SIGNAL_MAX_AGE_SEC = int(os.getenv("SIGNAL_MAX_AGE_SEC", "300"))
# Zone「存在/確定」情報は長く保持し、touch系イベントは短く保持する
ZONE_LOOKBACK_SEC = int(os.getenv("ZONE_LOOKBACK_SEC", "86400"))        # デフォルト24時間
ZONE_TOUCH_LOOKBACK_SEC = int(os.getenv("ZONE_TOUCH_LOOKBACK_SEC", str(SIGNAL_LOOKBACK_SEC)))

# Cache file name
CACHE_FILE = os.getenv("CACHE_FILE", "signals_cache.json")

# --- Debug / behavior ---
FORCE_AI_CALL_WHEN_FLAT = os.getenv("FORCE_AI_CALL_WHEN_FLAT", "false").lower() == "true"
AI_THROTTLE_SEC = int(os.getenv("AI_THROTTLE_SEC", "10"))

# --- Webhook auth (optional) ---
# If set, require the token to match either header `X-Webhook-Token` or JSON field `token`.
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN", "").strip()

# --- AI entry filter (v2.7 hybrid upgrade) ---
AI_ENTRY_ENABLED = os.getenv("AI_ENTRY_ENABLED", "true").lower() == "true"
AI_ENTRY_MIN_CONFIDENCE = max(0, min(100, int(os.getenv("AI_ENTRY_MIN_CONFIDENCE", "70"))))
AI_ENTRY_THROTTLE_SEC = float(os.getenv("AI_ENTRY_THROTTLE_SEC", "4.0"))
# on AI error: "skip" (safest) or "default_allow"
AI_ENTRY_FALLBACK = os.getenv("AI_ENTRY_FALLBACK", "skip").strip().lower()
AI_ENTRY_DEFAULT_CONFIDENCE = int(os.getenv("AI_ENTRY_DEFAULT_CONFIDENCE", "50"))
AI_ENTRY_DEFAULT_MULTIPLIER = float(os.getenv("AI_ENTRY_DEFAULT_MULTIPLIER", "1.0"))

# --- AI close/hold management (Day Trading) ---
AI_CLOSE_ENABLED = os.getenv("AI_CLOSE_ENABLED", "true").lower() == "true"
AI_CLOSE_MIN_CONFIDENCE = max(0, min(100, int(os.getenv("AI_CLOSE_MIN_CONFIDENCE", "70"))))
# Default to a conservative cadence; allow immediate evaluation on clear reversal-like signals.
AI_CLOSE_THROTTLE_SEC = float(os.getenv("AI_CLOSE_THROTTLE_SEC", "60.0"))
# on AI error: "hold" (safest) or "default_close"
AI_CLOSE_FALLBACK = os.getenv("AI_CLOSE_FALLBACK", "hold").strip().lower()
AI_CLOSE_DEFAULT_CONFIDENCE = int(os.getenv("AI_CLOSE_DEFAULT_CONFIDENCE", "50"))

# --- Position add-on / pyramiding (optional) ---
# If true: when positions are open, a SAME-DIRECTION Q-Trend trigger can still go through ENTRY gating.
# Default false to preserve conservative behavior.
ALLOW_ADD_ON_ENTRIES = os.getenv("ALLOW_ADD_ON_ENTRIES", "false").lower() == "true"
# If true: allow multiple entries for the same qtrend anchor time (advanced; default false).
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

# If no OpenAI client is available, AI-entry gating cannot run.
# In that case, disable AI_ENTRY to avoid blocking entries unexpectedly.
if AI_ENTRY_ENABLED and not client:
    print("[FXAI][WARN] AI_ENTRY_ENABLED=true but OPENAI_API_KEY is missing. Disabling AI entry filter.")
    AI_ENTRY_ENABLED = False

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

# --- De-dupe / throttle ---
_executed_qtrend_times_by_symbol: Dict[str, list] = {}
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
    arr = _executed_qtrend_times_by_symbol.get(symbol, [])
    for t in arr:
        if _qtime_equal(t, q_time):
            return True
    return False


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


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


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
                with signals_lock:
                    signals_cache.extend(data)
                print(f"[FXAI] Cache loaded: {len(data)} signals recovered.")
    except Exception as e:
        print(f"[FXAI][WARN] Failed to load cache: {e}")

def _save_cache_locked():
    """シグナルキャッシュをファイルに保存する (Lock保持中に呼ぶこと)"""
    try:
        # アトミック書き込みが理想だが、簡易的に上書き
        with open(CACHE_FILE, 'w') as f:
            json.dump(signals_cache, f)
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

        if is_zones:
            if (sig_type in {"structure", "zones", "zone"} and event in zone_presence_events) or event in zone_presence_events:
                limit_sec = ZONE_LOOKBACK_SEC
            elif any(m in event for m in zone_touch_markers):
                limit_sec = ZONE_TOUCH_LOOKBACK_SEC
            else:
                # 不明なZonesイベントは長期保持せず、誤認を避けるため短期で消す
                limit_sec = SIGNAL_LOOKBACK_SEC
        else:
            limit_sec = SIGNAL_LOOKBACK_SEC

        age = now - float(s.get("receive_time", now) or now)
        if age < float(limit_sec or SIGNAL_LOOKBACK_SEC):
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

    # Zones presence is meaningful even if it happened before Q-Trend.
    # Count recent zone confirmations (within ZONE_LOOKBACK_SEC by receive_time).
    for s in normalized:
        if (s.get("source") == "Zones") and (s.get("signal_type") == "structure") and (s.get("event") == "new_zone_confirmed"):
            rt = float(s.get("receive_time") or 0.0)
            if rt > 0 and (now - rt) <= float(ZONE_LOOKBACK_SEC):
                zones_confirmed_recent += 1

    for s in normalized:
        st = float(s.get("signal_time") or s.get("receive_time") or 0)
        if st < q_time:
            continue
        if CONFLUENCE_LOOKBACK_SEC > 0 and st > (q_time + CONFLUENCE_LOOKBACK_SEC):
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
            continue

        if side == q_side:
            confirm_signals += 1
            confirm_unique_sources.add(src)
            weighted_confirm_score += (w * event_weight)
            if s.get("strength") == "strong":
                strong_after_q = True
        elif side in {"buy", "sell"}:
            opp_signals += 1
            opp_unique_sources.add(src)
            weighted_oppose_score += (w * event_weight)

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

    rates_m5 = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M5, 0, 14)
    if rates_m5 is not None and len(rates_m5) > 0:
        ranges = [abs(_rate_field(r, "high", 0.0) - _rate_field(r, "low", 0.0)) for r in rates_m5]
        atr = sum(ranges) / max(1, len(ranges))
    else:
        atr = 0.0

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


def _validate_ai_entry_decision(decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """期待するJSON: {action: "ENTRY"|"SKIP", confidence: 0-100, multiplier: 0.5-1.5, reason: str}。

    NOTE: multiplier は "local_multiplier に掛ける係数"（0.5-1.5）として扱う。
    """
    if not isinstance(decision, dict):
        return None

    action = decision.get("action")
    if not isinstance(action, str):
        return None
    action = action.strip().upper()
    if action not in {"ENTRY", "SKIP"}:
        return None

    confidence = _safe_int(decision.get("confidence"), AI_ENTRY_DEFAULT_CONFIDENCE)
    confidence = int(_clamp(confidence, 0, 100))

    # multiplier must be explicitly present
    if "multiplier" not in decision:
        return None
    multiplier = _safe_float(decision.get("multiplier"), AI_ENTRY_DEFAULT_MULTIPLIER)
    multiplier = float(_clamp(multiplier, 0.5, 1.5))

    reason = decision.get("reason")
    if not isinstance(reason, str):
        reason = ""
    reason = reason.strip()

    return {"action": action, "confidence": confidence, "multiplier": multiplier, "reason": reason}


def _build_entry_logic_prompt(symbol: str, market: dict, stats: dict, action: str) -> str:
    """Q-Trend発生時の環境を渡し、AIに ENTRY/SKIP と倍率を判断させる専用プロンプト。"""

    # statsが無い（まだQ-Trendを保持していない/鮮度で弾かれた）場合でも500にしない。
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
            "note": "No Q-Trend stats available yet. Be conservative.",
        }
        return (
            "You are a strict trading entry decision engine.\n"
            "Decide if the proposed entry should be executed now.\n"
            "Return ONLY strict JSON with this schema:\n"
            '{"action": "ENTRY"|"SKIP", "confidence": 0-100, "multiplier": 0.5-1.5, "reason": "short"}\n\n'
            "ContextJSON:\n"
            + json.dumps(minimal_payload, ensure_ascii=False)
        )

    # 既存のコンテキスト構築を流用しつつ、出力スキーマだけ仕様に合わせる
    base = _build_entry_filter_prompt(symbol, market, stats, action)
    return (
        "You are a strict trading entry decision engine.\n"
        "Decide if the proposed entry should be executed now.\n"
        "Return ONLY strict JSON with this schema:\n"
        '{"action": "ENTRY"|"SKIP", "confidence": 0-100, "multiplier": 0.5-1.5, "reason": "short"}\n\n'
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
    spread_flag = "WIDE" if spread_points >= 40 else "NORMAL"

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
            "multiplier_factor_range": [0.5, 1.5],
            "confidence_range": [0, 100],
            "local_multiplier": local_multiplier,
            "final_multiplier_max": 2.0,
            "note": "Return JSON only. IMPORTANT: If 'zones_confirmed_recent' > 0 (HTF Structure exists) but 'confluence_score' is low, strictly reduce confidence. Strong momentum (trigger_type=Strong) can justify ENTRY even with some opposite touch signals if EV/space is good.",
        },
    }

    return (
        "You are a strict XAUUSD/GOLD DAY TRADING entry gate (not scalping).\n"
        "Core principle: Minimize loss, Maximize profit (cut losers fast; let winners run when EV is positive).\n"
        "You MUST NOT suggest BUY/SELL; the proposed_action is already decided locally.\n"
        "Use these decision principles:\n"
        "- Prefer ALIGNED higher-timeframe trend_alignment and OSGFC alignment.\n"
        "- Prefer stronger confluence: higher confirm_unique_sources, higher weighted_confirm_score.\n"
        "- Evaluate opposition with nuance: confirmed/structural opposition matters most; touch-based opposition can be noise.\n"
        "- If qtrend.trigger_type is Strong (strong momentum), treat it as higher breakout probability: be more willing to approve ENTRY even if there are some opposite touch signals (e.g., opposite FVG touch), as long as EV/space (ATR vs spread) remains attractive.\n"
        "- If some opposite FVG/Zones exist BUT trend is aligned and ATR-to-spread is healthy and confluence is decent, you MAY still approve ENTRY (EV can remain positive).\n"
        "- If structural Zones context exists (zones_confirmed_recent > 0) and confluence is weak, be conservative unless other evidence strongly improves EV.\n"
        "- Penalize wide spread.\n"
        "Return ONLY strict JSON schema (multiplier is a FACTOR for local_multiplier):\n"
        '{"action": "ENTRY"|"SKIP", "confidence": 0-100, "multiplier": 0.5-1.5, "reason": "short"}\n\n'
        "ContextJSON:\n"
        + json.dumps(payload, ensure_ascii=False)
    )


def _attempt_entry_from_stats(symbol: str, stats: dict, normalized_trigger: dict, now: float) -> tuple[str, int]:
    """Try to place an order based on latest Q-Trend stats.

    Returns (message, http_status).
    """
    if not stats:
        _set_status(last_result="Stored (no qtrend stats)", last_result_at=time.time())
        return "Stored", 200

    q_time = float(stats.get("q_time") or now)
    q_age_sec = int(now - q_time)

    if Q_TREND_MAX_AGE_SEC > 0 and q_age_sec > Q_TREND_MAX_AGE_SEC:
        _set_status(last_result="Stale Q-Trend", last_result_at=time.time())
        return "Stale Q-Trend", 200

    # strict duplicate prevention for the same q_time
    if (not ALLOW_MULTI_ENTRY_SAME_QTREND) and _was_qtrend_executed(symbol, q_time):
        _set_status(last_result="Duplicate", last_result_at=time.time())
        return "Duplicate", 200

    # confluence requirement
    if int(stats.get("confirm_unique_sources") or 0) < MIN_OTHER_SIGNALS_FOR_ENTRY:
        _set_status(last_result="Waiting confluence", last_result_at=time.time())
        return "Waiting confluence", 200

    q_side = (stats.get("q_side") or "").lower()
    action = "BUY" if q_side == "buy" else "SELL" if q_side == "sell" else ""
    if action not in {"BUY", "SELL"}:
        _set_status(last_result="Invalid action", last_result_at=time.time())
        return "Invalid action", 400

    market = get_mt5_market_data(symbol)
    local_multiplier = _compute_entry_multiplier(
        int(stats.get("confirm_unique_sources") or 0),
        bool(stats.get("strong_after_q")),
    )

    final_multiplier = float(local_multiplier)
    ai_conf = None
    ai_reason = ""

    # AIフィルター（有効時のみ）
    if AI_ENTRY_ENABLED:
        # Throttle AI calls but allow re-eval when a NEW supporting signal arrives.
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

        ai_decision = _ai_entry_filter(symbol, market, stats, action)
        if (not ai_decision) or ai_decision["action"] != "ENTRY" or ai_decision["confidence"] < AI_ENTRY_MIN_CONFIDENCE:
            print(f"[FXAI][AI] Entry blocked by AI. Decision: {ai_decision}")
            if (not ai_decision) and AI_ENTRY_FALLBACK == "default_allow":
                ai_decision = {
                    "action": "ENTRY",
                    "confidence": int(_clamp(AI_ENTRY_DEFAULT_CONFIDENCE, 0, 100)),
                    "multiplier": float(_clamp(AI_ENTRY_DEFAULT_MULTIPLIER, 0.5, 1.5)),
                    "reason": "fallback_default_allow",
                }
            else:
                _set_status(last_result="Blocked by AI", last_result_at=time.time())
                return "Blocked by AI", 403

        ai_conf = ai_decision["confidence"]
        ai_reason = ai_decision.get("reason") or ""
        final_multiplier = float(_clamp(local_multiplier * float(ai_decision["multiplier"]), 0.5, 2.0))

    # ZMQでエントリー指示を送信
    zmq_socket.send_json(
        {
            "type": "ORDER",
            "action": action,
            "symbol": symbol,
            "atr": float(market.get("atr") or 0.0),
            "multiplier": final_multiplier,
            "reason": ai_reason,
            "ai_confidence": ai_conf,
            "ai_reason": ai_reason,
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
            "ai_confidence": ai_conf,
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
    print(f"[FXAI][ZMQ] Order sent: {action} {symbol} with multiplier {final_multiplier}")
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
    return {"action": action, "confidence": confidence, "reason": reason}


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
        "You are an elite XAUUSD/GOLD DAY TRADER focused on expected value (Loss-cut small / Profit-target large).\n"
        "Decide whether to HOLD or CLOSE existing positions based on changing market conditions.\n"
        "Avoid knee-jerk exits on noisy opposite touch events; however, if strong reversal signs appear (e.g., multiple oppositions, trend misalignment, repeated opposite zone touches), CLOSE early to protect capital.\n"
        "If the current trend shows strong momentum (qtrend_context.is_strong_momentum=true), you should be more willing to HOLD to aim for larger profits, unless clear reversal/structural opposition increases risk materially.\n"
        "Return ONLY strict JSON with this schema:\n"
        '{"action": "HOLD"|"CLOSE", "confidence": 0-100, "reason": "short"}\n\n'
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


def _ai_entry_filter(symbol: str, market: dict, stats: dict, action: str) -> Optional[Dict[str, Any]]:
    """AIにエントリーの許可を求めるフィルター。"""
    prompt = _build_entry_logic_prompt(symbol, market, stats, action)
    decision = _call_openai_with_retry(prompt)
    if not decision:
        print("[FXAI][AI] No response from AI. Using fallback.")
        return None

    validated_decision = _validate_ai_entry_decision(decision)
    if not validated_decision:
        print("[FXAI][AI] Invalid AI response. Using fallback.")
        return None

    return validated_decision


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
    symbol = data.get("symbol", SYMBOL)

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

    with signals_lock:
        signals_cache.append(signal)
        _prune_signals_cache_locked(now)
        _save_cache_locked()  # ★追加: 更新されたキャッシュをファイルに保存

    normalized = _normalize_signal_fields(signal)

    # Determine Q-Trend trigger early (used for both management and optional add-on entries)
    qtrend_trigger = (normalized.get("source") in {"Q-Trend-Strong", "Q-Trend-Normal"} and normalized.get("side") in {"buy", "sell"})

    # --- POSITION MANAGEMENT MODE (CLOSE/HOLD) ---
    pos_summary = get_mt5_positions_summary(symbol)
    if int(pos_summary.get("positions_open") or 0) > 0:
        net_side = (pos_summary.get("net_side") or "flat").lower()

        # Optional: allow add-on entries when SAME-DIRECTION Q-Trend arrives
        if not (ALLOW_ADD_ON_ENTRIES and qtrend_trigger and net_side in {"buy", "sell"} and normalized.get("side") == net_side):
            # 管理対象外のソースはAI呼び出しを避けて保存のみ（コスト/レート制御）
            if (normalized.get("source") or "") not in {"Q-Trend", "FVG", "Zones", "OSGFC"}:
                _set_status(last_result="Stored (mgmt ignored source)", last_result_at=time.time())
                return "Stored", 200

            market = get_mt5_market_data(symbol)
            stats = get_qtrend_anchor_stats(symbol)

            if AI_CLOSE_ENABLED:
                global _last_close_attempt_key, _last_close_attempt_at
                now_mono = time.time()

                src = (normalized.get("source") or "")
                evt = (normalized.get("event") or "")
                sig_side = (normalized.get("side") or "").lower()

                is_reversal_like = (
                    (net_side in {"buy", "sell"} and sig_side in {"buy", "sell"} and sig_side != net_side)
                    and (
                        (src == "Q-Trend")
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
                    return "AI close throttled", 200

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
                        return "HOLD", 200

                decision_action = ai_decision["action"]
                decision_reason = ai_decision.get("reason") or ""
                if decision_action == "CLOSE":
                    zmq_socket.send_json({"type": "CLOSE", "reason": decision_reason})
                    _set_status(
                        last_result="CLOSE",
                        last_result_at=time.time(),
                        last_mgmt_action="CLOSE",
                        last_mgmt_confidence=int(ai_decision.get("confidence") or 0),
                        last_mgmt_reason=decision_reason,
                        last_mgmt_at=time.time(),
                        last_mgmt_throttled=None,
                    )
                    return "CLOSE", 200
                else:
                    zmq_socket.send_json({"type": "HOLD", "reason": decision_reason})
                    _set_status(
                        last_result="HOLD",
                        last_result_at=time.time(),
                        last_mgmt_action="HOLD",
                        last_mgmt_confidence=int(ai_decision.get("confidence") or 0),
                        last_mgmt_reason=decision_reason,
                        last_mgmt_at=time.time(),
                        last_mgmt_throttled=None,
                    )
                    return "HOLD", 200
            else:
                _set_status(last_result="Stored (mgmt disabled)", last_result_at=time.time())
                return "Stored", 200

        # if add-on entries allowed, fall through to entry section

    # --- ENTRY MODE (Q-Trend reservation + follow-up entry) ---
    stats = get_qtrend_anchor_stats(symbol)
    if not stats:
        _set_status(last_result="Stored", last_result_at=time.time())
        return "Stored", 200

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
        return _attempt_entry_from_stats(symbol, stats, normalized, now)

    if has_pending_qtrend and src in supporting_sources:
        # this is the key upgrade: immediate re-eval on new evidence within Q-Trend TTL
        return _attempt_entry_from_stats(symbol, stats, normalized, now)

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
    print(f"[FXAI] symbol: {SYMBOL}")
    # ★追加: 起動時にキャッシュを復元
    _load_cache()
    bind_err = _check_port_bindable("0.0.0.0", int(WEBHOOK_PORT))
    if bind_err:
        print(f"[FXAI][FATAL] Cannot bind WEBHOOK_PORT={WEBHOOK_PORT}. OS error: {bind_err}")
        print("[FXAI][HINT] Set WEBHOOK_PORT to an available port (e.g. 8000) or run with sufficient privileges.")
        raise SystemExit(2)
    app.run(host='0.0.0.0', port=WEBHOOK_PORT)
