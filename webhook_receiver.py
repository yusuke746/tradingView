from flask import Flask, request
import csv
import os

app = Flask(__name__)

# MT5の「共通フォルダ」のパス
LOG_FILE = r"C:\Users\MT4ver2-u70-t1jaowH5\AppData\Roaming\MetaQuotes\Terminal\Common\Files\signals_tradingview.csv"

CSV_FIELDS = [
    "time",
    "symbol",
    "price",
    "source",
    "side",
    "strength",
    "comment",
    "tf",
    "signal_type",
    "event",
    "confirmed",
]

LEGACY_CSV_FIELDS = [
    "time",
    "symbol",
    "price",
    "source",
    "side",
    "strength",
    "comment",
]


def ensure_csv_header(file_path: str, header_fields: list[str]) -> None:
    """CSVヘッダーを保証し、旧ヘッダーなら新ヘッダーへ移行する。"""
    if not os.path.exists(file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header_fields)
        return

    try:
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            with open(file_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(header_fields)
            return

        existing_header = rows[0]
        if existing_header == header_fields:
            return

        # 旧ヘッダーの場合は列を増やして移行（末尾の新列は空で埋める）
        if existing_header == LEGACY_CSV_FIELDS:
            migrated_rows = [header_fields]
            for r in rows[1:]:
                migrated_rows.append(r + [""] * (len(header_fields) - len(existing_header)))

            with open(file_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(migrated_rows)
            return

        # それ以外の未知ヘッダーは壊さない（追記時に新ヘッダーを書き直すと整合が崩れるため）
        print(f"Warning: Unexpected CSV header. Keeping existing header: {existing_header}")
    except Exception as e:
        print(f"Error ensuring CSV header: {e}")

def cleanup_csv(file_path, max_size_kb=10, keep_records=50):
    """
    CSVファイルが指定サイズを超えていたら、ヘッダーと最新のデータを残してトリミングする関数
   
    :param file_path:  CSVファイルのパス
    :param max_size_kb: トリミングを実行する閾値（KB単位）
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
        # csvとして読み込み、最後のN行だけ残す（値にカンマやクォートがあっても壊れない）
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if len(rows) <= keep_records + 1:
            return

        header = rows[0]
        new_rows = [header] + rows[-keep_records:]

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(new_rows)

        print(f"Cleanup complete. Kept last {keep_records} records.")
    except Exception as e:
        print(f"Error cleaning CSV: {e}")

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not data:
        return "Error", 400

    # 新JSON形式（+ 旧形式の互換）
    row = {
        "time": data.get("time", ""),
        "symbol": data.get("symbol", ""),
        "price": data.get("price", ""),
        "source": data.get("source", ""),
        "side": data.get("side", ""),
        "strength": data.get("strength", ""),
        "comment": data.get("comment", ""),
        "tf": data.get("tf", data.get("interval", "")),
        "signal_type": data.get("signal_type", ""),
        "event": data.get("event", ""),
        "confirmed": data.get("confirmed", ""),
    }

    ensure_csv_header(LOG_FILE, CSV_FIELDS)

    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([row.get(field, "") for field in CSV_FIELDS])
    except Exception as e:
        print(f"Error writing CSV: {e}")
        return "Error", 500

    print(f"信号受信: {row}")

    # CSV自動クリーンアップ（1MBを超えたら最新50件だけ残す）
    cleanup_csv(LOG_FILE, max_size_kb=1000, keep_records=50)

    return "Success", 200

if __name__ == '__main__':
    # ポートを80に変更
    app.run(host='0.0.0.0', port=80)
