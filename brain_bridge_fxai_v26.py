import os
import json
import time
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

# --- Signal retention / freshness ---
SIGNAL_LOOKBACK_SEC = int(os.getenv("SIGNAL_LOOKBACK_SEC", "1200"))
SIGNAL_MAX_AGE_SEC = int(os.getenv("SIGNAL_MAX_AGE_SEC", "300"))  # v2.6 SignalMaxAgeSec

# --- v2.6 confluence params ---
CONFLUENCE_LOOKBACK_SEC = int(os.getenv("CONFLUENCE_LOOKBACK_SEC", "600"))
Q_TREND_MAX_AGE_SEC = int(os.getenv("Q_TREND_MAX_AGE_SEC", "300"))
MIN_OTHER_SIGNALS_FOR_ENTRY = int(os.getenv("MIN_OTHER_SIGNALS_FOR_ENTRY", "1"))

# --- Debug / behavior ---
FORCE_AI_CALL_WHEN_FLAT = os.getenv("FORCE_AI_CALL_WHEN_FLAT", "false").lower() == "true"
AI_THROTTLE_SEC = int(os.getenv("AI_THROTTLE_SEC", "10"))

# --- AI entry filter (v2.7 hybrid upgrade) ---
AI_ENTRY_ENABLED = os.getenv("AI_ENTRY_ENABLED", "true").lower() == "true"
AI_ENTRY_MIN_CONFIDENCE = max(75, int(os.getenv("AI_ENTRY_MIN_CONFIDENCE", "75")))
AI_ENTRY_THROTTLE_SEC = float(os.getenv("AI_ENTRY_THROTTLE_SEC", "4.0"))
# on AI error: "skip" (safest) or "default_allow"
AI_ENTRY_FALLBACK = os.getenv("AI_ENTRY_FALLBACK", "skip").strip().lower()
AI_ENTRY_DEFAULT_CONFIDENCE = int(os.getenv("AI_ENTRY_DEFAULT_CONFIDENCE", "50"))
AI_ENTRY_DEFAULT_MULTIPLIER = float(os.getenv("AI_ENTRY_DEFAULT_MULTIPLIER", "1.0"))

# --- OpenAI ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", os.getenv("MODEL", "gpt-4o-mini"))
API_RETRY_COUNT = int(os.getenv("API_RETRY_COUNT", "3"))
API_RETRY_WAIT_SEC = float(os.getenv("API_RETRY_WAIT_SEC", "1.0"))
API_TIMEOUT_SEC = float(os.getenv("API_TIMEOUT_SEC", "7.0"))

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# --- ZMQ ---
context = zmq.Context.instance()
zmq_socket = context.socket(zmq.PUSH)
zmq_socket.bind(ZMQ_BIND)

# --- MT5 ---
if not mt5.initialize():
    raise SystemExit("MT5初期化失敗")

signals_lock = Lock()
signals_cache = []  # list[dict]

# --- De-dupe / throttle ---
_last_entry_q_time = None
_last_ai_attempt_key = None
_last_ai_attempt_at = 0.0

_last_entry_ai_key = None
_last_entry_ai_at = 0.0


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
    if src_lower in {"q-trend", "qtrend", "q-trendstrong", "qtrendstrong"}:
        s["source"] = "Q-Trend"
        if "strong" in src_lower and strength != "strong":
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


def _weight_confirmed(confirmed: str) -> float:
    """bar_close の方が信頼度が高い想定。"""
    c = (confirmed or "").lower()
    if c == "bar_close":
        return 1.0
    if c == "intrabar":
        return 0.6
    return 0.8


def _prune_signals_cache_locked(now: float) -> None:
    # 受信時刻ベースの保持（既存ロジック）
    signals_cache[:] = [s for s in signals_cache if now - s.get("receive_time", now) < SIGNAL_LOOKBACK_SEC]


def _filter_fresh_signals(symbol: str, now: float) -> list:
    """v2.6の SignalMaxAgeSec 相当：signal_time が古すぎるものを落とす。"""
    with signals_lock:
        normalized = [_normalize_signal_fields(s) for s in signals_cache if s.get("symbol") == symbol]

    fresh = []
    for s in normalized:
        st = float(s.get("signal_time") or 0)
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

    latest_q = None
    for s in reversed(normalized):
        if s.get("source") == "Q-Trend" and s.get("side") in {"buy", "sell"}:
            latest_q = s
            break
    if not latest_q:
        return None

    q_time = float(latest_q.get("signal_time") or latest_q.get("receive_time") or now)
    q_side = (latest_q.get("side") or "").lower()
    q_is_strong = (latest_q.get("strength") == "strong")

    confirm_unique_sources = set(["Q-Trend"])
    opp_unique_sources = set(["Q-Trend"])
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
    zones_confirmed = 0
    weighted_confirm_score = 0.0
    weighted_oppose_score = 0.0

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
        if src == "Q-Trend":
            continue

        # OSGFC trend filter is useful even when used as a filter (strength of context)
        if src == "OSGFC" and side in {"buy", "sell"}:
            # keep latest within window
            if osgfc_latest_time is None or st >= float(osgfc_latest_time):
                osgfc_latest_time = st
                osgfc_latest_side = side

        # Zones: neutral confirmations (no side) indicate S/R reliability
        if src == "Zones" and sig_type == "structure" and event == "new_zone_confirmed":
            zones_confirmed += 1

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

        if side == q_side:
            confirm_signals += 1
            confirm_unique_sources.add(src)
            weighted_confirm_score += w
            if s.get("strength") == "strong":
                strong_after_q = True
        elif side in {"buy", "sell"}:
            opp_signals += 1
            opp_unique_sources.add(src)
            weighted_oppose_score += w

    return {
        "q_time": q_time,
        "q_side": q_side,
        "q_is_strong": q_is_strong,
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
        "zones_confirmed": zones_confirmed,
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

    return {"bid": bid, "ask": ask, "m15_ma": ma15, "atr": atr, "spread": spread}


def get_mt5_position_state(symbol: str):
    try:
        positions = mt5.positions_get(symbol=symbol)
        if positions is None:
            return {"positions_open": 0}
        return {"positions_open": len(positions)}
    except Exception:
        return {"positions_open": 0}


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
    """期待するJSON: {action: "ENTRY"|"SKIP", confidence: 0-100, multiplier: 0.5-1.5, reason: str}."""
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
    zones_confirmed = int(stats.get("zones_confirmed") or 0)
    w_confirm = float(stats.get("weighted_confirm_score") or 0.0)
    w_oppose = float(stats.get("weighted_oppose_score") or 0.0)

    confluence_score = (confirm_u * 2) + min(confirm_n, 6)
    opposition_score = (opp_u * 3) + min(opp_n, 6)
    strong_flag = bool(stats.get("strong_after_q"))

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
            "strong_after_q": strong_flag,
        },
        "confluence": {
            "confirm_unique_sources": confirm_u,
            "confirm_signals": confirm_n,
            "oppose_unique_sources": opp_u,
            "oppose_signals": opp_n,
            "confluence_score": confluence_score,
            "opposition_score": opposition_score,
            "weighted_confirm_score": w_confirm,
            "weighted_oppose_score": w_oppose,
            "fvg_touch_same": fvg_same,
            "fvg_touch_opp": fvg_opp,
            "zones_touch_same": zones_same,
            "zones_touch_opp": zones_opp,
            "zones_confirmed": zones_confirmed,
            "osgfc_latest_side": osgfc_side,
            "osgfc_alignment": osgfc_align,
        },
        "market": {
            "bid": market.get("bid"),
            "ask": market.get("ask"),
            "m15_sma20": market.get("m15_ma"),
            "m15_trend": m15_trend,
            "trend_alignment": trend_align,
            "atr_m5_approx": market.get("atr"),
            "spread_points": spread_points,
            "spread_flag": spread_flag,
        },
        "constraints": {
            "multiplier_range": [0.5, 1.5],
            "confidence_range": [0, 100],
            "note": "Return JSON only. Do not propose BUY/SELL; decide ENTRY or SKIP for the given proposed_action.",
        },
    }

    return (
        "You are a strict trading entry gate for XAUUSD/GOLD scalping. FILTER entries to maximize expected value and avoid low-quality trades.\n"
        "You MUST NOT suggest BUY/SELL; the proposed_action is already decided locally.\n"
        "Use these decision principles:\n"
        "- Prefer ALIGNED higher-timeframe trend_alignment and OSGFC alignment.\n"
        "- Prefer stronger confluence: higher confirm_unique_sources, higher weighted_confirm_score.\n"
        "- Penalize opposition: higher oppose_unique_sources, higher weighted_oppose_score, fvg/zones touches against.\n"
        "- Penalize wide spread.\n"
        "Return ONLY strict JSON schema:\n"
        '{"action": "ENTRY"|"SKIP", "confidence": 0-100, "multiplier": 0.5-1.5, "reason": "short"}\n\n'
        "ContextJSON:\n"
        + json.dumps(payload, ensure_ascii=False)
    )


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
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return "Invalid data", 400

    now = time.time()
    symbol = data.get("symbol", SYMBOL)

    # 受信シグナルをキャッシュに保存（合流判定のため）
    # TradingViewテンプレが action しか送らない場合は、互換目的で source=Q-Trend 扱いに寄せる。
    raw_action = data.get("action") or data.get("side")
    raw_action_upper = (raw_action or "").strip().upper()
    inferred_side = ""
    if raw_action_upper in {"BUY", "SELL"}:
        inferred_side = raw_action_upper.lower()
    elif isinstance(raw_action, str) and raw_action.strip().lower() in {"buy", "sell"}:
        inferred_side = raw_action.strip().lower()

    signal = {
        "symbol": symbol,
        "source": data.get("source") or ("Q-Trend" if inferred_side else ""),
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

    normalized = _normalize_signal_fields(signal)

    # エントリートリガーは Q-Trend のみ（それ以外は保存して終了）
    qtrend_trigger = (normalized.get("source") == "Q-Trend" and normalized.get("side") in {"buy", "sell"})
    if not qtrend_trigger:
        return "Stored", 200

    stats = get_qtrend_anchor_stats(symbol)
    if not stats:
        return "Stored", 200

    q_age_sec = int(now - float(stats.get("q_time") or now))
    if Q_TREND_MAX_AGE_SEC > 0 and q_age_sec > Q_TREND_MAX_AGE_SEC:
        return "Stale Q-Trend", 200

    # 合流条件（ユニークソースで最低限の合流が取れていない場合は見送り）
    if int(stats.get("confirm_unique_sources") or 0) < MIN_OTHER_SIGNALS_FOR_ENTRY:
        return "Waiting confluence", 200

    # ローカルで BUY/SELL を確定（AIに決めさせない）
    q_side = (stats.get("q_side") or normalized.get("side") or "").lower()
    action = "BUY" if q_side == "buy" else "SELL" if q_side == "sell" else ""
    if action not in {"BUY", "SELL"}:
        return "Invalid action", 400

    # 重複送信ガード（同じQ-Trend起点で何度もORDERを出さない）
    global _last_entry_q_time
    if _last_entry_q_time is not None:
        try:
            if abs(float(_last_entry_q_time) - float(stats.get("q_time") or 0.0)) < 0.0001:
                return "Duplicate", 200
        except Exception:
            pass

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
        ai_decision = _ai_entry_filter(symbol, market, stats, action)
        if (not ai_decision) or ai_decision["action"] != "ENTRY" or ai_decision["confidence"] < AI_ENTRY_MIN_CONFIDENCE:
            print(f"[FXAI][AI] Entry blocked by AI. Decision: {ai_decision}")
            return "Blocked by AI", 403
        ai_conf = ai_decision["confidence"]
        ai_reason = ai_decision.get("reason") or ""
        # AIのmultiplierはローカル倍率への係数として扱う（過度に増えないよう上限を2.0に丸める）
        final_multiplier = float(_clamp(local_multiplier * float(ai_decision["multiplier"]), 0.5, 2.0))

    # ZMQでエントリー指示を送信
    zmq_socket.send_json({
        "type": "ORDER",
        "action": action,
        "symbol": symbol,
        "multiplier": final_multiplier,
        "reason": ai_reason,
        "ai_confidence": ai_conf,
        "ai_reason": ai_reason,
    })

    _last_entry_q_time = float(stats.get("q_time") or now)
    print(f"[FXAI][ZMQ] Order sent: {action} {symbol} with multiplier {final_multiplier}")
    return "OK", 200


if __name__ == '__main__':
    print(f"[FXAI] webhook http://0.0.0.0:{WEBHOOK_PORT}/webhook")
    print(f"[FXAI] ZMQ bind: {ZMQ_BIND}")
    print(f"[FXAI] symbol: {SYMBOL}")
    app.run(host='0.0.0.0', port=WEBHOOK_PORT)
