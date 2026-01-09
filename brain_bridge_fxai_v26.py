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
        s["source"] = src

    if isinstance(s.get("side"), str):
        s["side"] = s["side"].strip().lower()
    if isinstance(s.get("strength"), str):
        s["strength"] = s["strength"].strip().lower()

    parsed = _parse_signal_time_to_epoch(s.get("time"))
    if parsed is not None:
        s["signal_time"] = parsed
    else:
        s["signal_time"] = s.get("receive_time")

    return s


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

    for s in normalized:
        st = float(s.get("signal_time") or s.get("receive_time") or 0)
        if st < q_time:
            continue
        if CONFLUENCE_LOOKBACK_SEC > 0 and st > (q_time + CONFLUENCE_LOOKBACK_SEC):
            continue

        src = s.get("source")
        side = s.get("side")
        if src == "Q-Trend":
            continue

        if side == q_side:
            confirm_signals += 1
            confirm_unique_sources.add(src)
            if s.get("strength") == "strong":
                strong_after_q = True
        elif side in {"buy", "sell"}:
            opp_signals += 1
            opp_unique_sources.add(src)

    return {
        "q_time": q_time,
        "q_side": q_side,
        "q_is_strong": q_is_strong,
        "confirm_unique_sources": max(0, len(confirm_unique_sources) - 1),
        "confirm_signals": confirm_signals,
        "opp_unique_sources": max(0, len(opp_unique_sources) - 1),
        "opp_signals": opp_signals,
        "strong_after_q": strong_after_q,
    }


def get_mt5_market_data(symbol: str):
    tick = mt5.symbol_info_tick(symbol)
    rates_m15 = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M15, 0, 20)
    ma15 = sum([r["close"] for r in rates_m15]) / 20 if rates_m15 is not None else 0

    rates_m5 = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_M5, 0, 14)
    atr = sum([abs(r["high"] - r["low"]) for r in rates_m5]) / 14 if rates_m5 is not None else 0

    info = mt5.symbol_info(symbol)
    point = info.point if info else 0
    spread = (tick.ask - tick.bid) / point if (tick and point) else 0

    return {"bid": tick.bid, "ask": tick.ask, "m15_ma": ma15, "atr": atr, "spread": spread}


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


def _build_management_prompt(symbol: str, market: dict, state: dict, stats: dict) -> str:
    q_age_sec = int(time.time() - stats["q_time"]) if stats.get("q_time") else -1
    m15_trend = "UPTREND" if market["bid"] > market["m15_ma"] else "DOWNTREND"

    return f"""
Role: Elite Gold (XAUUSD/GOLD) Scalper on M5.
Objective: Maximize Expected Value while avoiding low-quality entries.

### Market Context (MT5 Live)
- Symbol: {symbol}
- M15 Trend (SMA20): {m15_trend}  (Bid={market['bid']}, Ask={market['ask']}, SMA20={market['m15_ma']})
- M5 ATR(approx): {market['atr']}  Spread(points): {market['spread']}

### State
- PositionsOpen: {state['positions_open']}

### Q-Trend Anchor (Computed Locally, do NOT recompute)
- LatestQTrendTimeEpoch: {stats['q_time']}
- LatestQTrendSide: {stats['q_side']}
- LatestQTrendAgeSec: {q_age_sec}
- LatestQTrendIsStrong: {str(stats['q_is_strong']).lower()}

### Post-Q Confluence (Computed Locally)
- ConfirmUniqueSources (excluding Q-Trend): {stats['confirm_unique_sources']}
- ConfirmSignals (count): {stats['confirm_signals']}
- OpposeUniqueSources (excluding Q-Trend): {stats['opp_unique_sources']}
- OpposeSignals (count): {stats['opp_signals']}
- StrongDetectedAfterQ: {str(stats['strong_after_q']).lower()}

### Strategy (fxChartAI v2.6 aligned)
1) ENTRY:
    - Entries are handled locally. When PositionsOpen == 0, output HOLD.
2) MANAGEMENT (PositionsOpen > 0):
    - If oppositions rise (OpposeUniqueSources >= 1) and/or M15 trend flips against Q-Trend side, consider CLOSE.
    - If Q-Trend side remains supported and oppositions are low, output HOLD.
3) Output must be STRICT JSON only:
    {{"action": "HOLD"|"CLOSE", "confidence": "HIGH"|"NORMAL", "reason": "txt"}}
""".strip()


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        return "No data", 400

    now = time.time()

    data['receive_time'] = now
    with signals_lock:
        signals_cache.append(data)
        _prune_signals_cache_locked(now)

    stats = get_qtrend_anchor_stats(SYMBOL)
    if not stats:
        return "No Q-Trend", 200

    market = get_mt5_market_data(SYMBOL)
    state = get_mt5_position_state(SYMBOL)

    q_age_sec = int(now - stats["q_time"]) if stats.get("q_time") else -1
    entry_window_ok = (q_age_sec >= 0 and q_age_sec <= Q_TREND_MAX_AGE_SEC)
    entry_confluence_ok = (stats["confirm_unique_sources"] >= MIN_OTHER_SIGNALS_FOR_ENTRY)

    m15_up = market["bid"] > market["m15_ma"]
    m15_down = market["ask"] < market["m15_ma"]

    # 1) ENTRY (local, v2.6 aligned)
    if state["positions_open"] == 0:
        if entry_window_ok and entry_confluence_ok:
            action = "BUY" if stats["q_side"] == "buy" else "SELL" if stats["q_side"] == "sell" else None

            # Trend Guard (same as your current prompt intent)
            if action == "BUY" and not m15_up:
                action = None
            if action == "SELL" and not m15_down:
                action = None

            if action:
                global _last_entry_q_time
                # de-dupe: same Q anchor -> only once
                if _last_entry_q_time is None or float(stats["q_time"]) != float(_last_entry_q_time):
                    _last_entry_q_time = float(stats["q_time"])

                    mult = _compute_entry_multiplier(stats["confirm_unique_sources"], stats["strong_after_q"])
                    cmd = {
                        "type": "ORDER",
                        "action": action,
                        "multiplier": mult,
                        "atr": market["atr"],
                        "reason": f"Q-Trend entry (uSrc={stats['confirm_unique_sources']} strongAfterQ={stats['strong_after_q']})",
                    }
                    zmq_socket.send_json(cmd)
                    print(f">>> SENT ORDER (local): {cmd}")

                return "OK", 200

        # flat時は原則AI呼ばない（pasadと同じ）
        if not FORCE_AI_CALL_WHEN_FLAT:
            zmq_socket.send_json({"type": "HOLD", "reason": "Flat: waiting for Q-Trend confluence"})
            return "OK", 200

    # 2) MANAGEMENT (AI: CLOSE/HOLD only)
    # throttle key: (q_time, positions_open)
    key = f"{stats.get('q_time')}|{state['positions_open']}"
    global _last_ai_attempt_key, _last_ai_attempt_at
    if _last_ai_attempt_key == key and (now - _last_ai_attempt_at) < AI_THROTTLE_SEC:
        return "OK", 200
    _last_ai_attempt_key = key
    _last_ai_attempt_at = now

    if state["positions_open"] == 0 and not FORCE_AI_CALL_WHEN_FLAT:
        return "OK", 200

    prompt = _build_management_prompt(SYMBOL, market, state, stats)
    decision = _call_openai_with_retry(prompt)
    if not decision:
        return "OK", 200

    action = (decision.get("action") or "").strip().upper()
    reason = (decision.get("reason") or "").strip()

    if action == "CLOSE":
        zmq_socket.send_json({"type": "CLOSE", "reason": reason or "AI CLOSE"})
        print(">>> SENT CLOSE")
    else:
        zmq_socket.send_json({"type": "HOLD", "reason": reason or "AI HOLD"})
        print(">>> SENT HOLD")

    return "OK", 200


if __name__ == '__main__':
    print(f"[FXAI] webhook http://0.0.0.0:{WEBHOOK_PORT}/webhook")
    print(f"[FXAI] ZMQ bind: {ZMQ_BIND}")
    print(f"[FXAI] symbol: {SYMBOL}")
    app.run(host='0.0.0.0', port=WEBHOOK_PORT)
