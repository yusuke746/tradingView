<?php
// TradingViewから送られてくるJSONを取得
$json = file_get_contents('php://input');
$data = json_decode($json, true);

if ($data && isset($data['indicator']) && isset($data['side'])) {
    // データのサニタイズ（改行やカンマを除去）
    $indicator = str_replace(["\n", "\r", ","], "", $data['indicator']);
    $side = str_replace(["\n", "\r", ","], "", $data['side']);
    
    // ログとして保存（MT5が読み取る用）
    $log_entry = date("Y.m.d H:i:s") . "," . $indicator . "," . $side . "\n";
    $result = file_put_contents('signals.csv', $log_entry, FILE_APPEND | LOCK_EX);
    
    if ($result !== false) {
        echo "Signal Received";
    } else {
        echo "Write Error";
    }
} else {
    echo "No Data";
}
?>
