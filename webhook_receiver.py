from flask import Flask, request
import datetime
import os

app = Flask(__name__)

# MT5の「共通フォルダ」のパス
LOG_FILE = r"C:\Users\MT4ver2-u70-t1jaowH5\AppData\Roaming\MetaQuotes\Terminal\Common\Files\signals_tradingview.csv"

def cleanup_csv(file_path, max_size_kb=1000, keep_records=50):
    """
    CSVファイルが指定サイズを超えていたら、ヘッダーと最新のデータを残してトリミングする関数
   
    :param file_path:  CSVファイルのパス
    :param max_size_kb: トリミングを実行する閾値（KB単位）。デフォルト1MB
    :param keep_records: 残すレコード数（ヘッダーを除く）。
    """
   
    # ファイルが存在しない場合は何もしない
    if not os.path.exists(file_path):
        return

    # ファイルサイズを取得 (バイト単位なので / 1024 でKBに変換)
    current_size_kb = os.path.getsize(file_path) / 1024

    # 指定サイズを超えていないなら何もしないで終了
    if current_size_kb < max_size_kb:
        return

    print(f"File size {current_size_kb:.2f}KB exceeds limit.  Cleaning up...")

    try:
        # ファイルを読み込む（エンコーディングは環境に合わせて変更してください。通常utf-8かshift_jis）
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            lines = f.readlines()

        # データが少なすぎる場合は何もしない
        # (ヘッダー1行 + 残したい行数 より少ない場合)
        if len(lines) <= keep_records + 1:
            return

        # --- トリミング処理 ---
        # 1. ヘッダー（1行目）を確保
        header = lines[0]
       
        # 2. 末尾から指定行数分だけ確保
        new_data = lines[-keep_records:]
       
        # 3. 結合（ヘッダー + 最新データ）
        new_content = [header] + new_data

        # 上書き保存
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            f.writelines(new_content)
           
        print(f"Cleanup complete.  Kept last {keep_records} records.")

    except Exception as e:
        print(f"Error cleaning CSV: {e}")

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    if data:
        # 新しいJSON形式に対応
        time = data.get('time', '')
        symbol = data.get('symbol', '')
        price = data.get('price', '')
        source = data.get('source', '')
        side = data.get('side', '')
        strength = data.get('strength', '')
        comment = data.get('comment', '')
        
        # CSVに記録（カンマ区切り）
        log_entry = f"{time},{symbol},{price},{source},{side},{strength},{comment}\n"
        
        # ファイルが存在しない場合はヘッダーを書き込む
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", encoding='utf-8') as f:
                f.write("time,symbol,price,source,side,strength,comment\n")
        
        # データを追記
        with open(LOG_FILE, "a", encoding='utf-8') as f:
            f.write(log_entry)
        
        print(f"信号受信: {log_entry.strip()}")
        
        # CSV自動クリーンアップ（1MBを超えたら最新50件だけ残す）
        cleanup_csv(LOG_FILE, max_size_kb=1000, keep_records=50)
        
        return "Success", 200
    return "Error", 400

if __name__ == '__main__':
    # ポートを80に変更
    app.run(host='0.0.0.0', port=80)
