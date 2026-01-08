import os
import zmq
import json
import requests
from flask import Flask, request
from dotenv import load_dotenv

# .envファイルから環境変数を読み込み
load_dotenv()

app = Flask(__name__)

# --- 設定ロード ---
API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
ZMQ_PORT = os.getenv("ZMQ_PORT", "5555")

# ZeroMQ PUSHソケットの設定
context = zmq.Context()
zmq_socket = context.socket(zmq.PUSH)
zmq_socket.bind(f"tcp://*:{ZMQ_PORT}")

def call_openai_brain(signal_data):
    """
    MT5のロジック(M15トレンド判断など)を内包したAI判断関数
    """
    # [cite_start]既存のEAロジック [cite: 48, 49] をプロンプトに統合
    prompt = f"""
    Role: Elite Gold (XAUUSD) Scalper.
    Objective: Maximize Expected Value (RR 1:2+).
    
    ### [cite_start]Strategy Guidelines [cite: 49]
    1. STRONG ENTRY: If signals align with trend -> 'BUY'/'SELL'.
    2. RANGE FILTER: If mixed signals -> 'HOLD'.
    3. PANIC EXIT: If momentum breaks -> 'CLOSE'.

    ### Incoming Signal Context
    {json.dumps(signal_data)}

    ### Output (Strict JSON Only)
    {{"action": "BUY"|"SELL"|"HOLD"|"CLOSE", "lot": 0.1, "sl_dist_points": 200, "reason": "txt"}}
    """

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }
    
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "You are a trading engine. [cite_start]Output ONLY JSON. [cite: 51]"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.0
    }

    try:
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=10)
        res_json = response.json()
        content = res_json['choices'][0]['message']['content']
        return json.loads(content)
    except Exception as e:
        print(f"Brain Error: {e}")
        return None

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        return "No data", 400

    print(f"Brain received signal from: {data.get('source', 'unknown')}")

    # AIに判断させる
    decision = call_openai_brain(data)
    
    if decision and decision.get("action") in ["BUY", "SELL", "CLOSE"]:
        # MT5 (Muscle) へ即座に命令を飛ばす
        zmq_socket.send_json(decision)
        print(f"Sent Command to MT5: {decision['action']} - {decision['reason']}")

    return "OK", 200

if __name__ == '__main__':
    print(f"Brain Bridge is running. ZMQ Port: {ZMQ_PORT}")
    app.run(host='0.0.0.0', port=5000)