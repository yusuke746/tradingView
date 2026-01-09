import os
import zmq
import json
import time
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

def get_confluence_stats(target_symbol):
    """MQL5 v2.6のロジックをPythonで完全再現"""
    with signals_lock:
        if not signals_cache: return None
        # 1. 最新のQ-Trendを特定
        latest_q = None
        for s in reversed(signals_cache):
            if s.get('symbol') == target_symbol and s.get('source') == "Q-Trend":
                latest_q = s
                break
        if not latest_q: return None

        q_time = latest_q['receive_time']
        q_side = latest_q['side'].lower()
        
        # 2. Q-Trend以降のユニークソースを特定
        unique_sources = set()
        has_strong = False
        for s in signals_cache:
            if s['symbol'] == target_symbol and s['receive_time'] >= q_time:
                if s['side'].lower() == q_side and s['source'] != "Q-Trend":
                    unique_sources.add(s['source'])
                    if s.get('strength') == 'strong': has_strong = True
        
        return {"side": q_side, "count": len(unique_sources), "has_strong": has_strong, "q_time": q_time}

def get_mt5_market_data():
    """MT5から生のチャート情報を取得"""
    tick = mt5.symbol_info_tick(SYMBOL)
    rates_m15 = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M15, 0, 20)
    ma15 = sum([r['close'] for r in rates_m15]) / 20 if rates_m15 is not None else 0
    rates_m5 = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M5, 0, 14)
    atr = sum([abs(r['high'] - r['low']) for r in rates_m5]) / 14 if rates_m5 is not None else 0
    return {"bid": tick.bid, "ask": tick.ask, "m15_ma": ma15, "atr": atr, "spread": (tick.ask - tick.bid)/mt5.symbol_info(SYMBOL).point}

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
    stats = get_confluence_stats(SYMBOL)
    if not stats: return "No Q-Trend", 200

    # MT5データ取得
    market = get_mt5_market_data()

    # AIへの超詳細プロンプト
    prompt = f"""
    Role: Elite Gold Trader (M5). Decision based on Q-Trend Confluence vs Live MT5 Data.
    
    ### 1. Confluence Stats (Since latest Q-Trend)
    - Side: {stats['side']}
    - Extra Unique Sources: {stats['count']}
    - Strong Signal in Confluence: {stats['has_strong']}
    
    ### 2. MT5 Market Data
    - Bid: {market['bid']}, Ask: {market['ask']}
    - M15 SMA(20): {market['m15_ma']} ({'UP' if market['bid'] > market['m15_ma'] else 'DOWN'} trend)
    - M5 ATR: {market['atr']}, Spread: {market['spread']}

    ### Instructions
    1. Only BUY if Bid > M15 SMA. Only SELL if Ask < M15 SMA.
    2. If market is ranging or price is far from signal, output HOLD.
    3. Output CLOSE if trend reversal detected.
    4. Output JSON ONLY: {{"action": "BUY"|"SELL"|"HOLD"|"CLOSE", "confidence": "HIGH"|"NORMAL", "reason": "txt"}}
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
            votes = stats['count'] + (2 if stats['has_strong'] else 0)
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