from flask import Flask, request
import datetime

app = Flask(__name__)

# 以下の1行を書き換えます。MT5の「共通フォルダ」という特別な場所です。
# パスが分からない場合は、後ほどMT5側で確認する方法を教えます。
LOG_FILE = r"C:\Users\MT4ver2-u70-t1jaowH5\AppData\Roaming\MetaQuotes\Terminal\Common\Files\signals_tradingview.csv"

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    if data:
        now = datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")
        # 銘柄名と売買方向を記録
        log_entry = f"{now},{data.get('symbol')},{data.get('side')}\n"
        
        with open(LOG_FILE, "a") as f:
            f.write(log_entry)
        
        print(f"信号受信: {log_entry}")
        return "Success", 200
    return "Error", 400

if __name__ == '__main__':
    # ポートを80に変更
    app.run(host='0.0.0.0', port=80)
