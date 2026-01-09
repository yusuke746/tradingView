import requests
import json
from datetime import datetime

# あなたのサーバーのIPアドレスまたはドメインに書き換えてください
# ポートを5000に変更した場合は :5000 にしてください
url = "http://127.0.0.1:5001/webhook"

# テストデータ (TradingViewの変数をシミュレート)
test_data = {
    "time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "symbol": "GOLD",
    "price": "4450.464",
    "source": "Q-Trend",
    "side": "buy",
    "strength": "normal",
    "comment": "Trend Start",
    "tf": "5",
    "signal_type": "entry_trigger",
    "event": "trend_start",
    "confirmed": "bar_close"
}

try:
    response = requests.post(
        url,
        data=json.dumps(test_data),
        headers={'Content-Type': 'application/json'},
        timeout=10
    )
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Error: {e}")