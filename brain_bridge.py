import os
import zmq
import json
import time
from datetime import datetime, timezone
import MetaTrader5 as mt5
from flask import Flask, request
from threading import Lock
from openai import OpenAI
from dotenv import load_dotenv  # dotenv をインポート

app = Flask(__name__)

# .env ファイルを読み込む
load_dotenv()

# --- 設定 ---
WEBHOOK_PORT = 80
ZMQ_PORT = os.getenv("ZMQ_PORT")
SYMBOL = "GOLD"  # 取引通貨
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # .env から読み込む
MODEL = os.getenv("OPENAI_MODEL")
SIGNAL_LOOKBACK = 1200  # 20分（M5運用）

# fxChartAI v2.6 思想寄せ（Q-Trend起点）
CONFLUENCE_LOOKBACK_SEC = int(os.getenv("CONFLUENCE_LOOKBACK_SEC", "600"))
Q_TREND_MAX_AGE_SEC = int(os.getenv("Q_TREND_MAX_AGE_SEC", "300"))
MIN_OTHER_SIGNALS_FOR_ENTRY = int(os.getenv("MIN_OTHER_SIGNALS_FOR_ENTRY", "1"))

# 初期化
client = OpenAI(api_key=OPENAI_API_KEY)
context = zmq.Context()
zmq_socket = context.socket(zmq.PUSH)
zmq_socket.bind(f"tcp://*:{ZMQ_PORT}")

if not mt5.initialize():
    print("MT5初期化失敗")
    quit()

signals_lock = Lock()
signals_cache = [] # メモリ上のシグナル保管庫

def _parse_signal_time_to_epoch(value):
    """TradingView {{timenow}} / ISO8601 をできる範囲で epoch(sec) に変換。失敗時は None。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # 既に epoch の可能性
        v = float(value)
        if v > 1e12:
            return v / 1000.0
        return v
    if not isinstance(value, str):
        return None

    s = value.strip()
    if not s:
        return None

    # 例: 2026-01-08T22:55:00Z / 2026-01-08T22:55:00.123Z
    try:
        iso = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        pass

    return None


def _normalize_signal_fields(signal):
    """fxChartAI側の NormalizeSourceName / Q-Trend variants を Python 側で寄せる。"""
    s = dict(signal)
    src = (s.get("source") or "").strip()
    strength = (s.get("strength") or "").strip().lower()

    # Q-Trend Strong を Q-Trend + strong として扱う
    src_lower = src.lower().replace(" ", "").replace("_", "")
    if src_lower in {"q-trend", "qtrend", "q-trendstrong", "qtrendstrong"}:
        s["source"] = "Q-Trend"
        if "strong" in src_lower and strength != "strong":
            s["strength"] = "strong"
    else:
        s["source"] = src

    # side / strength の正規化
    if "side" in s and isinstance(s["side"], str):
        s["side"] = s["side"].strip().lower()
    if "strength" in s and isinstance(s["strength"], str):
        s["strength"] = s["strength"].strip().lower()

    # signal_time（epoch）を付与
    parsed = _parse_signal_time_to_epoch(s.get("time"))
    if parsed is not None:
        s["signal_time"] = parsed
    else:
        # 最悪、受信時刻を使う
        s["signal_time"] = s.get("receive_time")

    return s


def get_qtrend_anchor_stats(target_symbol):
    """fxChartAI v2.6 の『Q-Trend起点で合流をEA側で集計』を Python 側で再現。

    返り値は AI に渡すための要約（時系列集計をAIにさせない）。
    """
    with signals_lock:
        if not signals_cache:
            return None

        # 正規化した view を作る（元の dict を破壊しない）
        normalized = [_normalize_signal_fields(s) for s in signals_cache if s.get("symbol") == target_symbol]
        if not normalized:
            return None

        # 1) 最新のQ-Trendを特定（Q-Trend Strong も含む）
        latest_q = None
        for s in reversed(normalized):
            if s.get("source") == "Q-Trend" and s.get("side") in {"buy", "sell"}:
                latest_q = s
                break
        if not latest_q:
            return None

        q_time = float(latest_q.get("signal_time") or latest_q.get("receive_time") or time.time())
        q_side = (latest_q.get("side") or "").lower()
        q_is_strong = (latest_q.get("strength") == "strong")

        # 2) Q-Trend以降(lookback内)の賛成/反対を集計
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

def get_mt5_market_data():
    """MT5から生のチャート情報を取得"""
    tick = mt5.symbol_info_tick(SYMBOL)
    rates_m15 = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M15, 0, 20)
    ma15 = sum([r['close'] for r in rates_m15]) / 20 if rates_m15 is not None else 0
    rates_m5 = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 0, 14)
    atr = sum([abs(r['high'] - r['low']) for r in rates_m5]) / 14 if rates_m5 is not None else 0
    point = mt5.symbol_info(SYMBOL).point
    spread = (tick.ask - tick.bid) / point if point else 0
    return {"bid": tick.bid, "ask": tick.ask, "m15_ma": ma15, "atr": atr, "spread": spread}


def get_mt5_position_state():
    """EA側の PositionsTotal 相当の最低限情報を取得"""
    try:
        positions = mt5.positions_get(symbol=SYMBOL)
        if positions is None:
            return {"positions_open": 0}
        return {"positions_open": len(positions)}
    except Exception:
        return {"positions_open": 0}

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data: return "No data", 400
    
    data['receive_time'] = time.time()
    with signals_lock:
        signals_cache.append(data)
        now = time.time()
        signals_cache[:] = [s for s in signals_cache if now - s['receive_time'] < SIGNAL_LOOKBACK]

    # 合流ロジック実行
    stats = get_qtrend_anchor_stats(SYMBOL)
    if not stats:
        return "No Q-Trend", 200

    # MT5データ取得
    market = get_mt5_market_data()
    state = get_mt5_position_state()

    q_age_sec = int(time.time() - stats["q_time"]) if stats.get("q_time") else -1

    # エントリー可否（fxChartAI思想：Q-Trendトリガー + 合流）
    entry_window_ok = (q_age_sec >= 0 and q_age_sec <= Q_TREND_MAX_AGE_SEC)
    entry_confluence_ok = (stats["confirm_unique_sources"] >= MIN_OTHER_SIGNALS_FOR_ENTRY)
    m15_trend = "UPTREND" if market["bid"] > market["m15_ma"] else "DOWNTREND"

    # AIへの超詳細プロンプト
    prompt = f"""
Role: Elite Gold (XAUUSD/GOLD) Scalper on M5.
Objective: Maximize Expected Value while avoiding low-quality entries.

### Market Context (MT5 Live)
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

### Local Entry Gate (Computed)
- EntryWindowOK (Q age <= {Q_TREND_MAX_AGE_SEC}s): {str(entry_window_ok).lower()}
- EntryConfluenceOK (ConfirmUniqueSources >= {MIN_OTHER_SIGNALS_FOR_ENTRY}): {str(entry_confluence_ok).lower()}

### Strategy (fxChartAI v2.6 aligned)
1) When PositionsOpen == 0: default action is HOLD.
    - Only consider BUY/SELL if EntryWindowOK and EntryConfluenceOK are true.
2) Trend Guard:
    - Only BUY if Bid > SMA20 (M15 uptrend).
    - Only SELL if Ask < SMA20 (M15 downtrend).
    - If guard fails, output HOLD.
3) Management (PositionsOpen > 0):
    - If oppositions rise (OpposeUniqueSources >= 1) and/or M15 trend flips against Q-Trend side, consider CLOSE.
    - If Q-Trend side remains supported and oppositions are low, output HOLD.
4) Output must be STRICT JSON only:
    {{"action": "BUY"|"SELL"|"HOLD"|"CLOSE", "confidence": "HIGH"|"NORMAL", "reason": "txt"}}

### Notes
- You are NOT allowed to output markdown or extra keys.
"""

    try:
        res = client.chat.completions.create(
            model=MODEL,
            response_format={ "type": "json_object" }, # これを追加
            messages=[{"role": "system", "content": "You are a trading engine. Output ONLY JSON."},
                      {"role": "user", "content": prompt}],
            temperature=0.0
        )

        # AIの回答を取得
        raw_content = res.choices[0].message.content.strip()
        
        # ```json や ``` といったマークダウンの囲いを除去する処理を追加
        if raw_content.startswith("```"):
            raw_content = raw_content.replace("```json", "").replace("```", "").strip()

        decision = json.loads(raw_content)
        
        if decision['action'] in ["BUY", "SELL"]:
            # fxChartAI思想：Q-Trend後の合流(ユニークソース数)を主票にし、strong は加点
            votes = stats['confirm_unique_sources'] + (2 if stats['strong_after_q'] else 0)
            multiplier = 2.0 if votes >= 4 else 1.5 if votes >= 2 else 1.0
            
            cmd = {
                "type": "ORDER", "action": decision['action'], "multiplier": multiplier,
                "atr": market['atr'], "reason": decision['reason']
            }
            zmq_socket.send_json(cmd)
            print(f">>> SENT ORDER: {cmd}")
            
        elif decision['action'] == "CLOSE":
            zmq_socket.send_json({"type": "CLOSE", "reason": decision['reason']})
            print(">>> SENT CLOSE")

        elif decision['action'] == "HOLD":
            zmq_socket.send_json({"type": "HOLD", "reason": decision['reason']})
            print(">>> SENT HOLD SIGNAL")

    except Exception as e:
        print(f"Error: {e}")

    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=WEBHOOK_PORT)