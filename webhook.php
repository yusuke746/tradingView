<?php
// TradingViewから送られてくるJSONを取得
$json = file_get_contents('php://input');
$data = json_decode($json, true);

if ($data) {
    // ログとして保存（MT5が読み取る用）
    $log_entry = date("Y.m.d H:i:s") . "," . $data['indicator'] . "," . $data['side'] . "\n";
    file_put_contents('signals.csv', $log_entry, FILE_APPEND | LOCK_EX);
    echo "Signal Received";
} else {
    echo "No Data";
}
?>
