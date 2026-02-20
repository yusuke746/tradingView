#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fxai_lrr_brain.py
XAUUSD LRR Brain - Liquidity Raid Reversal Strategy
Python側シグナル生成エンジン (ZmqMuscle LRR v3.1 対応)

■ アーキテクチャ:
    Python(Brain) --ZMQ PUSH(5555)--> MT5(Muscle)
    Python(Brain) <--ZMQ PULL(5556)-- MT5(Muscle)  [Heartbeat受信]

■ 実装内容:
    [1] LRR (Liquidity Raid Reversal) シグナル生成
    [2] 流動性プール（LP）検出 (Swing High/Low last 12-24 bars)
    [3] スイープ検出 (Liquidity Sweep: ヒゲでLP超え→逆実体引け)
    [4] FVG (Fair Value Gap) 確認
    [5] セッションランク判定 (JST時刻ベース)
    [6] マルチタイムフレーム整合性チェック (15m トレンド方向)
    [7] ボリュームフィルター (1.3× 直近10本平均)
    [8] ニュースフィルター (CPI/FOMC/NFP 前後30分ブロック)
    [9] ZMQ PUSH 送信 (port 5555) / Heartbeat PULL 受信 (port 5556)
    [10] 日次管理 (エントリー3回/日・連敗停止・日次損失キャップ)
    [11] Robbins-Monro SpreadMed (O(1)逐次中央値推定)
    [12] ハートビート監視 (MT5死活確認・自動アラート)

■ 依存ライブラリ:
    pip install MetaTrader5 pyzmq numpy pandas

■ 使用方法:
    python fxai_lrr_brain.py
    (MT5 + fxChartAI_ZmqMuscle_LRR.mq5 が起動中であること)
"""

import time
import json
import logging
import threading
import collections
import math
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import zmq

try:
    from flask import Flask as _Flask, request as _flask_request, jsonify as _flask_jsonify
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    print("[WARN] Flask not installed. TV Webhook disabled. Run: pip install flask")

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    MT5_AVAILABLE = False
    print("[WARN] MetaTrader5 not installed. Run: pip install MetaTrader5")

# =============================================================================
#  セクション 1: ログ設定
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("fxai_lrr_brain.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("LRR_BRAIN")

# =============================================================================
#  セクション 2: 設定 (MQL5 InpXxx と対応させる)
# =============================================================================

# --- Symbol ---
SYMBOL          = "XAUUSD"
TIMEFRAME_M5    = mt5.TIMEFRAME_M5   if MT5_AVAILABLE else 5
TIMEFRAME_M15   = mt5.TIMEFRAME_M15  if MT5_AVAILABLE else 15
TIMEFRAME_H1    = mt5.TIMEFRAME_H1   if MT5_AVAILABLE else 60

# --- ZMQ ---
ZMQ_PUSH_URL    = "tcp://localhost:5555"   # MT5へシグナル送信
ZMQ_PULL_URL    = "tcp://localhost:5556"   # MT5からハートビート受信
ZMQ_SEND_TIMEOUT = 1000  # ms

# --- LRR 戦略パラメータ ---
LP_LOOKBACK         = 24       # 流動性プール: 直近何本のSwing H/L を参照
SWEEP_MIN_PIPS      = 0.02     # Sweepと判定する最小オーバーシュート ($)
SWEEP_MAX_PIPS      = 0.50     # Sweep最大オーバーシュート ($)  ※超えはトレンド継続疑い
MAX_FVG_BARS_BACK   = 5        # Sweep後の何本以内でFVGを確認するか
VOLUME_MULT_FILTER  = 1.3      # Volume > 直近10本平均のこの倍率
VOLUME_LOOKBACK     = 10       # Volume平均算出に使う過去本数
MTF_LOOKBACK        = 20       # 15m トレンド方向のMA期間
PARABOLIC_ANGLE_THR = 0.70     # 15m MA角度(正規化)超えでエントリースキップ

# --- 日次管理 ---
MAX_ENTRIES_PER_DAY = 3        # 1日最大エントリー回数
MAX_CONSEC_LOSSES   = 3        # 連敗停止回数
DAILY_LOSS_CAP_PCT  = 2.0      # 日次最大損失 (%)

# --- Risk Manager §2 デフォルト ---
BASE_RISK_PCT       = 0.40
MAX_RISK_PER_TRADE  = 0.60
VOL_RATIO_HIGH      = 1.35
VOL_RATIO_LOW       = 0.85
VOL_MULT_HIGH       = 0.55
VOL_MULT_NORMAL     = 1.00
VOL_MULT_LOW        = 1.10
SPREAD_REF_DOLLAR   = 0.30

# --- SpreadMed (Robbins-Monro) ---
SPREAD_MED_LR       = 0.03     # 学習率 (1/lr≈33サンプルで80%追従)
SPREAD_HARD_CAP     = 0.50     # エントリー絶対停止閾値 ($)

# --- ループ間隔 ---
MAIN_LOOP_INTERVAL_SEC = 2.0   # メインループ間隔

# --- TradingView Webhook (A+B ハイブリッド) ---
TV_WEBHOOK_ENABLED  = True    # False にすると Webhook サーバーを起動しない
TV_WEBHOOK_PORT     = 80    # 受信ポート (TradingView → ngrok → このポート)
TV_SIGNAL_STALE_SEC = 60      # この秒数より古い TV アラートは使わず MT5 計算で代替

# --- MT5サーバー時刻 → JST オフセット ---
# EET (GMT+2) → JST: +7h  /  夏時間 (GMT+3): +6h
JST_SERVER_OFFSET_HR = 7

# =============================================================================
#  セクション 3: ニュースフィルター設定
#
#  重要指標の発表時刻を設定する（JST）。
#  発表30分前〜発表時刻まで自動ブロック。
#  ・毎週更新またはForexFactory APIと連携推奨。
#  ・空リストにすると無効化される。
#
#  フォーマット: {"name": "...", "jst": "YYYY-MM-DD HH:MM"}
# =============================================================================

NEWS_SCHEDULE: list[dict] = [
    # 例: {"name": "US CPI", "jst": "2026-02-21 22:30"},
    # 例: {"name": "FOMC",   "jst": "2026-01-30 04:00"},
    # 例: {"name": "NFP",    "jst": "2026-02-06 22:30"},
]
NEWS_BLOCK_BEFORE_MIN = 30   # 発表何分前からブロックするか
NEWS_BLOCK_AFTER_MIN  = 10   # 発表後何分間ブロックを継続するか

# =============================================================================
#  セクション 4: データクラス / 型定義
# =============================================================================

@dataclass
class LRRSignal:
    """MT5へ送信するシグナルパケット"""
    action:        str              # "BUY" | "SELL"
    sweep_extreme: float            # Sweepヒゲ先端価格 (MT5側SL計算に使用)
    atr_m5:        float            # ATR(M5,14) 現在値
    ai_confidence: float            # シグナル信頼度スコア 0-100
    ai_reason:     str              # ロジック説明 (ログ用)
    session_rank:  str              # "S"|"A"|"B"
    vol_regime:    str              # "HIGH"|"NORMAL"|"LOW"
    multiplier:    float = 1.0      # ロット乗数 (通常1.0 / Python側で上書き時)
    symbol:        str = SYMBOL

@dataclass
class DailyState:
    """日次管理状態"""
    date:              str   = ""
    entry_count:       int   = 0
    consec_losses:     int   = 0
    daily_risk_used:   float = 0.0
    day_start_equity:  float = 0.0
    halt:              bool  = False
    halt_reason:       str   = ""

@dataclass
class MT5HeartbeatState:
    """MT5ハートビート監視状態"""
    last_recv_ts:      float = 0.0
    last_equity:       float = 0.0
    last_positions:    int   = 0
    mt5_halt:          bool  = False
    mt5_emg_system:    str   = ""
    stale_warned:      bool  = False
    STALE_WARN_SEC:    int   = 15   # この秒数HBなしで警告

# =============================================================================
#  セクション 5: グローバル状態
# =============================================================================

g_daily   = DailyState()
g_hb      = MT5HeartbeatState()
g_spread_med: float = 0.20   # Robbins-Monro逐次中央値

# 最後に処理したバー時刻 (バー確定ごとに1回処理するため)
g_last_bar_time: Optional[datetime] = None

# ニュースブロック状態
g_news_block_until: Optional[datetime] = None

# ZMQコンテキスト・ソケット
g_zmq_ctx:        Optional[zmq.Context] = None
g_zmq_push:       Optional[zmq.Socket]  = None
g_zmq_pull:       Optional[zmq.Socket]  = None

# TradingView Webhook キャッシュ (最新アラート)
g_tv_cache_lock = threading.Lock()
g_tv_cache: Optional["TVSignalCache"] = None

# =============================================================================
#  セクション 6: MT5 接続 / データ取得
# =============================================================================

def mt5_init() -> bool:
    """MT5初期化。失敗時はFalseを返す。"""
    if not MT5_AVAILABLE:
        log.error("[MT5] MetaTrader5 library not available.")
        return False
    if not mt5.initialize():
        log.error("[MT5] initialize() failed: %s", mt5.last_error())
        return False
    info = mt5.terminal_info()
    log.info("[MT5] Connected. build=%s path=%s", info.build, info.path)
    return True

def get_rates_m5(count: int = 100) -> Optional[np.ndarray]:
    """M5 OHLCV データを取得 (structured array)"""
    rates = mt5.copy_rates_from_pos(SYMBOL, TIMEFRAME_M5, 0, count)
    if rates is None or len(rates) == 0:
        log.warning("[MT5] copy_rates_from_pos(M5) failed: %s", mt5.last_error())
        return None
    return rates

def get_rates_m15(count: int = 50) -> Optional[np.ndarray]:
    """M15 OHLCV データを取得"""
    rates = mt5.copy_rates_from_pos(SYMBOL, TIMEFRAME_M15, 0, count)
    if rates is None or len(rates) == 0:
        log.warning("[MT5] copy_rates_from_pos(M15) failed: %s", mt5.last_error())
        return None
    return rates

def get_account_equity() -> float:
    """口座資産額を取得"""
    info = mt5.account_info()
    return info.equity if info else 0.0

def get_symbol_spread() -> float:
    """現在のスプレッド ($)"""
    tick = mt5.symbol_info_tick(SYMBOL)
    if tick is None:
        return 0.0
    return max(0.0, tick.ask - tick.bid)

# =============================================================================
#  セクション 7: SpreadMed (Robbins-Monro 逐次中央値推定)
#
#  更新式: med_new = med_old + lr * sign(x - med_old)
#  O(1)更新 / O(n log n)ソート不要 / スパイク耐性あり
# =============================================================================

def update_spread_med(spread: float) -> float:
    """スプレッド中央値をRobbins-Monro式で更新し新しい推定値を返す"""
    global g_spread_med
    diff = spread - g_spread_med
    g_spread_med += SPREAD_MED_LR * (1.0 if diff > 0 else (-1.0 if diff < 0 else 0.0))
    return g_spread_med

# =============================================================================
#  セクション 8: ATR計算 (Python側独自計算 / MT5側と整合検証用)
# =============================================================================

def calc_atr(rates: np.ndarray, period: int = 14) -> float:
    """True Range のSMA (ATR) を計算"""
    if len(rates) < period + 1:
        return 0.0
    highs  = rates["high"]
    lows   = rates["low"]
    closes = rates["close"]
    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:]  - closes[:-1])
        )
    )
    return float(np.mean(tr[-period:]))

# =============================================================================
#  セクション 9: セッションランク判定 (JST時刻ベース)
#
#  LRR Strategist 定義:
#    S級(×1.50): 22:20〜23:00 JST  (NYオープン)
#    A級(×1.00): 17:00〜18:30 JST  (ロンドン) | 22:00〜22:19 JST
#    B級(×0.70): 03:00〜03:59 JST  (アジア初動)
#    無効(禁止) : 12:00〜16:59 JST  (DeadZone)
# =============================================================================

JST = timezone(timedelta(hours=9))

def get_jst_now() -> datetime:
    return datetime.now(JST)

def get_session_rank(now_jst: Optional[datetime] = None) -> tuple[str, float]:
    """
    現在のセッションランクと乗数を返す。
    Returns: (rank_str, multiplier)  e.g. ("S", 1.5)
    """
    if now_jst is None:
        now_jst = get_jst_now()
    h, m = now_jst.hour, now_jst.minute
    time_min = h * 60 + m

    # DeadZone: 12:00〜16:59 JST
    if 720 <= time_min <= 1019:
        return ("INVALID", 0.0)
    # S級: 22:20〜23:00 JST
    if (22 * 60 + 20) <= time_min <= (23 * 60):
        return ("S", 1.5)
    # A級(ロンドン): 17:00〜18:30 JST
    if (17 * 60) <= time_min <= (18 * 60 + 30):
        return ("A", 1.0)
    # A級(NYプレ): 22:00〜22:19 JST
    if (22 * 60) <= time_min <= (22 * 60 + 19):
        return ("A", 1.0)
    # B級(アジア初動): 03:00〜03:59 JST
    if (3 * 60) <= time_min <= (3 * 60 + 59):
        return ("B", 0.7)
    # その他 → 保守的にA級
    return ("A", 1.0)

# =============================================================================
#  セクション 10: ボラティリティレジーム判定
# =============================================================================

def get_vol_regime(atr_m5: float, atr_h1: float) -> tuple[str, float]:
    """
    VolRatio = ATR_M5 / ATR_H1 でレジームと VolMult を返す。
    Returns: (regime_str, vol_mult)
    """
    if atr_m5 <= 0 or atr_h1 <= 0:
        return ("NORMAL", VOL_MULT_NORMAL)
    ratio = atr_m5 / atr_h1
    if ratio >= VOL_RATIO_HIGH:
        return ("HIGH",   VOL_MULT_HIGH)
    if ratio <  VOL_RATIO_LOW:
        return ("LOW",    VOL_MULT_LOW)
    return ("NORMAL", VOL_MULT_NORMAL)

# =============================================================================
#  セクション 11: 流動性プール（LP）検出
#
#  直近 LP_LOOKBACK 本のSwing High / Swing Low を特定する。
#  Swing High: i本が前後のN本より高い (N=2)
#  Swing Low : i本が前後のN本より低い
# =============================================================================

def find_liquidity_pools(rates: np.ndarray, lookback: int = LP_LOOKBACK
                         ) -> tuple[float, float]:
    """
    直近 lookback 本の中から Swing High / Swing Low を算出する。
    Returns: (swing_high, swing_low)
    """
    window = rates[-lookback:] if len(rates) >= lookback else rates
    swing_high = float(np.max(window["high"]))
    swing_low  = float(np.min(window["low"]))
    return swing_high, swing_low

# =============================================================================
#  セクション 12: スイープ（Liquidity Sweep）検出
#
#  判定条件:
#    上方Sweep: 最新完成バーの high > swing_high (SWEEP_MIN_PIPS 超)
#               かつ close < swing_high (逆実体引け = Sweep確定)
#    下方Sweep: 最新完成バーの low  < swing_low  (SWEEP_MIN_PIPS 超)
#               かつ close > swing_low
#
#  sweep_extreme = Sweepヒゲ先端価格 (MT5側SL計算に使用)
#  ヒゲが実体の3倍以上の場合はアルゴ暴走として除外 (弱点②対策)
# =============================================================================

def detect_sweep(bar: dict, swing_high: float, swing_low: float,
                 atr_m5: float) -> tuple[Optional[str], float]:
    """
    完成バー1本でSweepを判定する。
    Returns: (action, sweep_extreme)  action = "BUY"|"SELL"|None
    """
    high  = bar["high"]
    low   = bar["low"]
    open_ = bar["open"]
    close = bar["close"]
    body  = abs(close - open_)

    # --- 上方Sweep → カウンタトレード = SELL ---
    if high > swing_high + SWEEP_MIN_PIPS:
        overshoot = high - swing_high
        if overshoot > SWEEP_MAX_PIPS:
            return None, 0.0   # トレンド継続の疑いがある → スキップ
        if close >= swing_high:
            return None, 0.0   # 逆実体引けしていない
        wick_upper = high - max(open_, close)
        if body > 0 and wick_upper > body * 3:
            return None, 0.0   # ヒゲ/実体比 3倍超 = アルゴ暴走 → スキップ
        return "SELL", high

    # --- 下方Sweep → カウンタトレード = BUY ---
    if low < swing_low - SWEEP_MIN_PIPS:
        overshoot = swing_low - low
        if overshoot > SWEEP_MAX_PIPS:
            return None, 0.0
        if close <= swing_low:
            return None, 0.0
        wick_lower = min(open_, close) - low
        if body > 0 and wick_lower > body * 3:
            return None, 0.0
        return "BUY", low

    return None, 0.0

# =============================================================================
#  セクション 13: FVG (Fair Value Gap) 確認
#
#  直近 MAX_FVG_BARS_BACK 本の中からSweep方向に一致するFVGを探す。
#  上昇FVG (BUY): bars[i-2].high < bars[i].low   (ギャップ空白)
#  下降FVG (SELL): bars[i-2].low > bars[i].high
# =============================================================================

def find_fvg(rates: np.ndarray, action: str,
             max_bars: int = MAX_FVG_BARS_BACK) -> Optional[float]:
    """
    actionと同方向のFVGを直近max_bars本の中から探す。
    見つかった場合: FVG中心価格を返す (指値設置目安)
    見つからない場合: None
    """
    window = rates[-max_bars - 2:] if len(rates) >= max_bars + 2 else rates
    for i in range(2, len(window)):
        if action == "BUY":
            # 上昇FVG: N-2本高値 < N本安値
            if window[i - 2]["high"] < window[i]["low"]:
                fvg_mid = (window[i - 2]["high"] + window[i]["low"]) / 2.0
                return float(fvg_mid)
        else:  # SELL
            # 下降FVG: N-2本安値 > N本高値
            if window[i - 2]["low"] > window[i]["high"]:
                fvg_mid = (window[i - 2]["low"] + window[i]["high"]) / 2.0
                return float(fvg_mid)
    return None

# =============================================================================
#  セクション 14: ボリュームフィルター
#
#  Sweep発生直後の5分足出来高が直近10本平均の1.3倍以上あること。
#  出来高が伴わないSweepは機関参加なしと判断してスキップ。
# =============================================================================

def check_volume_filter(rates: np.ndarray,
                        lookback: int = VOLUME_LOOKBACK,
                        mult: float = VOLUME_MULT_FILTER) -> tuple[bool, float]:
    """
    直近完成バーの出来高が直近 lookback 本平均の mult 倍以上か判定。
    Returns: (passed, vol_ratio)
    """
    if len(rates) < lookback + 1:
        return True, 1.0   # データ不足時はスキップしない (保守的)
    vols   = rates["tick_volume"]
    avg    = float(np.mean(vols[-lookback - 1:-1]))
    latest = float(vols[-1])
    if avg <= 0:
        return True, 1.0
    ratio = latest / avg
    return ratio >= mult, ratio

# =============================================================================
#  セクション 15: マルチタイムフレーム整合性 (15m)
#
#  15m足の直近MA方向と、5m側のエントリー方向が一致するか確認。
#  逆行の場合: TP1で全決済モードを示唆 (multiplier を 0.7 に抑制)
#  15m MAが急角度 (パラボリック) の場合: エントリーをスキップ
# =============================================================================

def check_mtf_alignment(action: str, rates_m15: Optional[np.ndarray]
                        ) -> tuple[bool, float, str]:
    """
    15m 足トレンドとのアライメントチェック。
    Returns: (is_aligned, adjustment_mult, reason)
      is_aligned=False: エントリースキップ推奨 (パラボリック判定)
      adjustment_mult < 1.0: TP1で全決済モード推奨
    """
    if rates_m15 is None or len(rates_m15) < MTF_LOOKBACK:
        log.debug("[MTF] data insufficient -> skip MTF check")
        return True, 1.0, "mtf_data_insufficient"

    closes = rates_m15["close"][-MTF_LOOKBACK:]
    ma = float(np.mean(closes))
    current = float(closes[-1])
    ma_diff = (current - ma) / ma if ma > 0 else 0.0

    # パラボリック判定: MA乖離率がPARABOLIC_ANGLE_THR(0.70%)超
    if abs(ma_diff) > PARABOLIC_ANGLE_THR / 100.0:
        log.info("[MTF] Parabolic 15m MA. diff=%.4f%% > thr=%.2f%% -> SKIP",
                 ma_diff * 100, PARABOLIC_ANGLE_THR)
        return False, 0.0, "mtf_parabolic"

    # トレンド方向の判定
    m15_trend = "up" if current > ma else "down"
    if action == "BUY" and m15_trend == "up":
        return True, 1.0, "mtf_aligned_bull"
    if action == "SELL" and m15_trend == "down":
        return True, 1.0, "mtf_aligned_bear"

    # 逆行 → TP1で全決済推奨 (乗数を0.7に抑制)
    log.info("[MTF] Counter-trend. action=%s 15m=%s -> mult=0.7 (TP1-only mode)",
             action, m15_trend)
    return True, 0.7, "mtf_counter_trend"

# =============================================================================
#  セクション 16: スプレッドゲート（Python側予備チェック）
#
#  MT5側でも同じ3条件チェックを行うが、Python側で事前に弾くことで
#  無駄なZMQ送信を減らす。
# =============================================================================

def is_spread_blocked(spread: float, atr_m5: float) -> tuple[bool, str]:
    """
    スプレッドが広すぎる場合True。
    Returns: (blocked, reason)
    """
    if spread > SPREAD_HARD_CAP:
        return True, f"HARD_CAP sp={spread:.4f}>{SPREAD_HARD_CAP}"
    if g_spread_med > 0 and spread > g_spread_med * 2.5:
        return True, f"SPIKE sp={spread:.4f}>med*2.5={g_spread_med*2.5:.4f}"
    if atr_m5 > 0 and spread > atr_m5 * 0.18:
        return True, f"ATR_REL sp={spread:.4f}>atr*0.18={atr_m5*0.18:.4f}"
    return False, ""

# =============================================================================
#  セクション 17: ニュースフィルター
# =============================================================================

def is_news_blocked(now_jst: Optional[datetime] = None) -> tuple[bool, str]:
    """
    経済指標スケジュールに基づきエントリーをブロックすべきか判定。
    Returns: (blocked, reason)
    """
    global g_news_block_until
    if now_jst is None:
        now_jst = get_jst_now()

    # アクティブなブロック解除チェック
    if g_news_block_until and now_jst > g_news_block_until:
        g_news_block_until = None

    if g_news_block_until:
        return True, f"news_block until {g_news_block_until.strftime('%H:%M')}"

    # スケジュールチェック
    for news in NEWS_SCHEDULE:
        try:
            news_jst = datetime.strptime(news["jst"], "%Y-%m-%d %H:%M").replace(tzinfo=JST)
        except ValueError:
            continue
        block_from = news_jst - timedelta(minutes=NEWS_BLOCK_BEFORE_MIN)
        block_to   = news_jst + timedelta(minutes=NEWS_BLOCK_AFTER_MIN)
        if block_from <= now_jst <= block_to:
            g_news_block_until = block_to
            reason = f"news={news['name']} {news['jst']} (±{NEWS_BLOCK_BEFORE_MIN}m)"
            log.warning("[NEWS] Block activated: %s", reason)
            return True, reason

    return False, ""

def send_news_block_to_mt5(blocked: bool) -> None:
    """ニュースブロック状態をMT5に通知する"""
    if g_zmq_push is None:
        return
    pkt = {"type": "NEWS_BLOCK", "news_block": blocked,
           "ts": int(time.time())}
    _zmq_send(pkt, reason="news_block_update")

# =============================================================================
#  セクション 18: リスク%計算 (Python側ログ用 / MT5側が最終確定)
# =============================================================================

def calc_risk_pct(session_mult: float, vol_mult: float, spread: float) -> float:
    """
    RiskPct = BaseRisk * SessionMult * VolMult * SpreadMult
    Pythonログ用 (MT5側が最終値を使用する)
    """
    sp_ref = SPREAD_REF_DOLLAR if SPREAD_REF_DOLLAR > 0 else 0.30
    spread_mult = max(0.50, min(1.00, 1.0 - (spread / sp_ref) * 0.25))
    risk = BASE_RISK_PCT * session_mult * vol_mult * spread_mult
    return min(risk, MAX_RISK_PER_TRADE)

# =============================================================================
#  セクション 19: シグナル信頼度スコア計算 (0〜100)
# =============================================================================

def calc_confidence(session_rank: str, vol_regime: str,
                    vol_ratio: float, mtf_mult: float,
                    fvg_found: bool) -> tuple[float, str]:
    """
    LRRシグナルの信頼度を0〜100でスコアリングし、理由を返す。
    """
    score = 50.0  # ベーススコア
    reasons = []

    # セッションランクボーナス
    if session_rank == "S":
        score += 20; reasons.append("NY_S")
    elif session_rank == "A":
        score += 10; reasons.append("LDN_A")
    elif session_rank == "B":
        score -= 10; reasons.append("ASIA_B")
    else:
        score -= 30; reasons.append("INVALID")

    # ボラレジームボーナス
    if vol_regime == "NORMAL":
        score += 10; reasons.append("vol_normal")
    elif vol_regime == "LOW":
        score +=  5; reasons.append("vol_low")
    elif vol_regime == "HIGH":
        score -= 10; reasons.append("vol_high_risky")

    # FVGボーナス
    if fvg_found:
        score += 15; reasons.append("fvg_found")
    else:
        score -=  5; reasons.append("no_fvg")

    # MTFアライメント
    if mtf_mult >= 1.0:
        score += 10; reasons.append("mtf_aligned")
    elif mtf_mult >= 0.7:
        score -=  5; reasons.append("mtf_counter")
    else:
        score -= 20; reasons.append("mtf_blocked")

    score = max(0.0, min(100.0, score))
    return score, "+".join(reasons)

# =============================================================================
#  セクション 20: ZMQ 送受信
# =============================================================================

def zmq_init() -> bool:
    """ZMQ ソケット初期化"""
    global g_zmq_ctx, g_zmq_push, g_zmq_pull
    try:
        g_zmq_ctx  = zmq.Context()
        g_zmq_push = g_zmq_ctx.socket(zmq.PUSH)
        g_zmq_push.setsockopt(zmq.SNDHWM, 10)
        g_zmq_push.setsockopt(zmq.SNDTIMEO, ZMQ_SEND_TIMEOUT)
        g_zmq_push.connect(ZMQ_PUSH_URL)

        g_zmq_pull = g_zmq_ctx.socket(zmq.PULL)
        g_zmq_pull.setsockopt(zmq.RCVHWM, 20)
        g_zmq_pull.setsockopt(zmq.RCVTIMEO, 0)   # ノンブロッキング
        g_zmq_pull.connect(ZMQ_PULL_URL)

        log.info("[ZMQ] PUSH connected -> %s", ZMQ_PUSH_URL)
        log.info("[ZMQ] PULL connected -> %s", ZMQ_PULL_URL)
        return True
    except zmq.ZMQError as e:
        log.error("[ZMQ] init failed: %s", e)
        return False

def _zmq_send(payload: dict, reason: str = "") -> bool:
    """JSON シリアライズして PUSH 送信"""
    if g_zmq_push is None:
        return False
    try:
        msg = json.dumps(payload, ensure_ascii=False)
        g_zmq_push.send_string(msg)
        log.debug("[ZMQ] SENT %s: %s", reason, msg[:200])
        return True
    except zmq.ZMQError as e:
        log.warning("[ZMQ] send failed (%s): %s", reason, e)
        return False

def send_signal(sig: LRRSignal) -> bool:
    """LRRシグナルをMT5へ送信する"""
    payload = {
        "type":          "ORDER",
        "symbol":        sig.symbol,
        "action":        sig.action,
        "sweep_extreme": round(sig.sweep_extreme, 3),
        "atr":           round(sig.atr_m5, 5),
        "ai_confidence": round(sig.ai_confidence, 1),
        "ai_reason":     sig.ai_reason,
        "session_rank":  sig.session_rank,
        "vol_regime":    sig.vol_regime,
        "multiplier":    round(sig.multiplier, 3),
        "ts":            int(time.time()),
        "news_block":    False,
    }
    ok = _zmq_send(payload, reason=f"ORDER_{sig.action}")
    if ok:
        log.info("[SIGNAL] SENT action=%s conf=%.0f sweep_ext=%.3f atr=%.4f "
                 "sess=%s vol=%s mult=%.2f reason=%s",
                 sig.action, sig.ai_confidence, sig.sweep_extreme, sig.atr_m5,
                 sig.session_rank, sig.vol_regime, sig.multiplier, sig.ai_reason)
    return ok

def send_hold(reason: str) -> None:
    """HOLDシグナルを送信 (エントリー不可を通知)"""
    _zmq_send({"type": "HOLD", "reason": reason, "ts": int(time.time())},
              reason="HOLD")

# =============================================================================
#  セクション 21: MT5 ハートビート受信スレッド
# =============================================================================

def heartbeat_receiver_thread() -> None:
    """バックグラウンドでMT5からハートビートを受信し g_hb を更新する"""
    log.info("[HB_RX] Thread started.")
    while True:
        if g_zmq_pull is None:
            time.sleep(1)
            continue
        try:
            msg = g_zmq_pull.recv_string(zmq.NOBLOCK)
            data = json.loads(msg)
            now  = time.time()
            g_hb.last_recv_ts   = now
            g_hb.last_equity    = data.get("equity", 0.0)
            g_hb.last_positions = data.get("positions", 0)
            g_hb.mt5_halt       = data.get("halt", False)
            g_hb.mt5_emg_system = data.get("emg_system", "")
            g_hb.stale_warned   = False

            # MT5でEmergencyが発動していたら警告
            if g_hb.mt5_emg_system:
                log.warning("[HB_RX] MT5 emg_system=%s halt=%s",
                            g_hb.mt5_emg_system, g_hb.mt5_halt)

            # MT5の日次エントリーカウントで差異チェック
            mt5_entries = data.get("daily_entries", -1)
            if mt5_entries >= 0 and g_daily.entry_count != mt5_entries:
                log.debug("[HB_RX] entry_count sync: python=%d mt5=%d -> use mt5",
                          g_daily.entry_count, mt5_entries)
                g_daily.entry_count = mt5_entries

        except zmq.ZMQError:
            pass  # ノンブロッキング: メッセージなし
        except json.JSONDecodeError as e:
            log.debug("[HB_RX] JSON parse error: %s", e)
        except Exception as e:
            log.error("[HB_RX] Unexpected error: %s", e)

        # ハートビート途絶検知
        if (g_hb.last_recv_ts > 0 and
                time.time() - g_hb.last_recv_ts > g_hb.STALE_WARN_SEC and
                not g_hb.stale_warned):
            elapsed = int(time.time() - g_hb.last_recv_ts)
            log.warning("[HB_RX] *** MT5 HEARTBEAT STALE *** elapsed=%ds > %ds. "
                        "Is MT5/EA running?", elapsed, g_hb.STALE_WARN_SEC)
            g_hb.stale_warned = True

        time.sleep(0.1)

# =============================================================================
#  セクション 22: 日次管理
# =============================================================================

def _today_jst() -> str:
    return get_jst_now().strftime("%Y-%m-%d")

def check_daily_reset() -> None:
    """日付変更時に日次カウンターをリセットする"""
    today = _today_jst()
    if g_daily.date == today:
        return
    if g_daily.date:
        log.info("[DAILY] Date changed %s -> %s. Resetting counters.",
                 g_daily.date, today)
    g_daily.date            = today
    g_daily.entry_count     = 0
    g_daily.consec_losses   = 0
    g_daily.daily_risk_used = 0.0
    g_daily.day_start_equity = get_account_equity() if MT5_AVAILABLE else 0.0
    g_daily.halt            = False
    g_daily.halt_reason     = ""
    log.info("[DAILY] Reset done. date=%s equity=%.2f", today, g_daily.day_start_equity)

def is_daily_halted() -> tuple[bool, str]:
    """日次エントリー停止チェック"""
    if g_daily.halt:
        return True, g_daily.halt_reason
    if g_daily.entry_count >= MAX_ENTRIES_PER_DAY:
        g_daily.halt = True
        g_daily.halt_reason = f"max_entries_per_day={MAX_ENTRIES_PER_DAY}"
        return True, g_daily.halt_reason
    if g_daily.consec_losses >= MAX_CONSEC_LOSSES:
        g_daily.halt = True
        g_daily.halt_reason = f"consec_losses={g_daily.consec_losses}>={MAX_CONSEC_LOSSES}"
        return True, g_daily.halt_reason
    if (g_daily.day_start_equity > 0 and MT5_AVAILABLE):
        eq = get_account_equity()
        loss_pct = (g_daily.day_start_equity - eq) / g_daily.day_start_equity * 100
        if loss_pct >= DAILY_LOSS_CAP_PCT:
            g_daily.halt = True
            g_daily.halt_reason = f"daily_loss_cap={loss_pct:.2f}%>={DAILY_LOSS_CAP_PCT}%"
            return True, g_daily.halt_reason
    return False, ""

def on_entry_sent(risk_pct: float) -> None:
    """エントリー送信後に日次カウンターを更新する"""
    g_daily.entry_count   += 1
    g_daily.daily_risk_used += risk_pct
    log.info("[DAILY] entry_count=%d/%d daily_risk=%.3f%%",
             g_daily.entry_count, MAX_ENTRIES_PER_DAY, g_daily.daily_risk_used)

def on_trade_closed(profit: float) -> None:
    """
    トレードクローズ時に連敗カウンターを更新する。
    (HeartbeatのconsecLossesと同期することを推奨)
    """
    if profit < 0:
        g_daily.consec_losses += 1
        log.info("[DAILY] Loss. consec_losses=%d/%d profit=%.2f",
                 g_daily.consec_losses, MAX_CONSEC_LOSSES, profit)
    else:
        if g_daily.consec_losses > 0:
            log.info("[DAILY] Win/BE -> reset consec_losses (%d->0) profit=%.2f",
                     g_daily.consec_losses, profit)
        g_daily.consec_losses = 0

# =============================================================================
#  セクション 25: マーケットコンテキスト指標
#
#  ■ distance_atr_ratio:
#     M15-20SMAからの乖離をATR単位で評価。
#     >= 5.0 → 伸び切り (順張り拒絶 / 保持中なら強制利確)
#     >= 4.0 → 要注意   (保持中: Override利確候補)
#
#  ■ atr_to_spread_approx:
#     ATR(M5) / Spread → 期待値効率。10未満: コスト負け拒絶 / 15未満: 低評価
#
#  ■ volatility_ratio:
#     ATR(M5)_now / ATR(M5)_24h_mean → 0.85未満: スクイーズ / 2.0以上: パニック
# =============================================================================

# SMA20 / ATR計算に使うM15バー数
SMA20_PERIOD      = 20
DIST_HARD_REJECT  = 5.0    # distance_atr_ratio: 絶対拒絶
DIST_OVERRIDE_THR = 4.0    # distance_atr_ratio: Override利確トリガー
ATS_HARD_REJECT   = 10.0   # atr_to_spread: コスト負け拒絶閾値
ATS_WARN_THR      = 15.0   # atr_to_spread: 低評価閾値
VOL_SQUEEZE_THR   = 0.85   # volatility_ratio: スクイーズ判定
VOL_PANIC_THR     = 2.0    # volatility_ratio: パニック判定
ATR24H_BARS       = 288    # M5足で約24時間分 (24*60/5)の本数

@dataclass
class MarketContext:
    """AIが参照するマーケットコンテキスト数値まとめ"""
    distance_atr_ratio:  float = 0.0   # SMA20乖離 (ATR単位)
    direction_vs_sma:    str   = ""    # "above" / "below"
    atr_to_spread:       float = 0.0   # ATR/Spread (期待値効率)
    volatility_ratio:    float = 1.0   # ATR_now / ATR_24h
    vol_state:           str   = "NORMAL"  # "SQUEEZE" / "PANIC" / "NORMAL"
    current_session:     str   = "A"   # "S" / "A" / "B"
    atr_m5:              float = 0.0
    atr_m15_raw:         float = 0.0
    sma20_m15:           float = 0.0


def calc_market_context(rates_m5: np.ndarray,
                        rates_m15: Optional[np.ndarray],
                        spread: float,
                        session_rank: str) -> MarketContext:
    """マーケットコンテキスト数値を一括計算する"""
    ctx = MarketContext()
    ctx.current_session = session_rank

    if rates_m5 is None or len(rates_m5) < 2:
        return ctx

    ctx.atr_m5 = calc_atr(rates_m5, period=14)

    # --- atr_to_spread_approx ---
    if spread > 0 and ctx.atr_m5 > 0:
        ctx.atr_to_spread = ctx.atr_m5 / spread
    else:
        ctx.atr_to_spread = 0.0

    # --- volatility_ratio: ATR_now / ATR_24h ---
    if len(rates_m5) >= ATR24H_BARS + 15:
        atr_24h = calc_atr(rates_m5[-ATR24H_BARS:], period=14)
        ctx.volatility_ratio = (ctx.atr_m5 / atr_24h) if atr_24h > 0 else 1.0
    if ctx.volatility_ratio < VOL_SQUEEZE_THR:
        ctx.vol_state = "SQUEEZE"
    elif ctx.volatility_ratio >= VOL_PANIC_THR:
        ctx.vol_state = "PANIC"
    else:
        ctx.vol_state = "NORMAL"

    # --- distance_atr_ratio: M15 SMA20乖離 ---
    if rates_m15 is not None and len(rates_m15) >= SMA20_PERIOD:
        ctx.atr_m15_raw = calc_atr(rates_m15, period=14)
        closes_m15 = rates_m15["close"]
        ctx.sma20_m15 = float(np.mean(closes_m15[-SMA20_PERIOD:]))
        latest_close  = float(closes_m15[-1])
        atr_base = ctx.atr_m15_raw if ctx.atr_m15_raw > 0 else ctx.atr_m5
        if atr_base > 0:
            ctx.distance_atr_ratio = abs(latest_close - ctx.sma20_m15) / atr_base
        ctx.direction_vs_sma = "above" if latest_close >= ctx.sma20_m15 else "below"

    return ctx


# =============================================================================
#  セクション 26: Lorentzian Classification (簡易実装)
#
#  ■ アルゴリズム:
#     Pine Script "Machine Learning: Lorentzian Classification" の
#     Pythonポート（軽量版）。最新足を"クエリ"として過去の特徴ベクトルと
#     Lorentzian距離で比較し、k-NNの多数決でBUY/SELLを予測する。
#
#  ■ 特徴量 (brain_bridge_fxai_v26 と整合):
#     f0: RSI(14)    f1: RSI(9)
#     f2: CCI(20)正規化  f3: ADX近似
#
#  ■ Lorentzian距離:
#     d = sum( log(1 + |fi_query - fi_past|) )
#
#  ■ 返値: ("BUY"|"SELL"|"NEUTRAL", confidence: float 0.0-1.0)
# =============================================================================

LC_NEIGHBORS    = 8    # k-NN 近傍数
LC_MAX_BARS_BK  = 200  # 過去何本まで探索するか
LC_MIN_BARS_BK  = 4    # 近傍候補のサンプリング間隔 (過学習防止)

def _calc_rsi(closes: np.ndarray, period: int = 14) -> float:
    """RSI を計算 (最新値のみ)"""
    if len(closes) < period + 1:
        return 50.0
    delta  = np.diff(closes[-(period + 1):])
    gains  = np.where(delta > 0, delta, 0.0)
    losses = np.where(delta < 0, -delta, 0.0)
    avg_g  = float(np.mean(gains))
    avg_l  = float(np.mean(losses))
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - (100.0 / (1.0 + rs))

def _calc_cci(highs: np.ndarray, lows: np.ndarray,
              closes: np.ndarray, period: int = 20) -> float:
    """CCI を計算 (最新値のみ, 0〜1に正規化して返す)"""
    if len(closes) < period:
        return 0.5
    tp    = (highs[-period:] + lows[-period:] + closes[-period:]) / 3.0
    tp_ma = float(np.mean(tp))
    md    = float(np.mean(np.abs(tp - tp_ma)))
    if md == 0:
        return 0.5
    cci   = (tp[-1] - tp_ma) / (0.015 * md)
    return float(max(0.0, min(1.0, (cci + 200) / 400)))  # -200〜+200 → 0〜1

def _calc_adx_approx(highs: np.ndarray, lows: np.ndarray,
                     closes: np.ndarray, period: int = 14) -> float:
    """ADX 近似 (0〜1に正規化して返す)"""
    if len(highs) < period + 1:
        return 0.25
    dm_plus  = np.maximum(np.diff(highs[-(period + 1):]), 0)
    dm_minus = np.maximum(-np.diff(lows[-(period + 1):]), 0)
    tr = np.maximum(
        highs[-period:] - lows[-period:],
        np.maximum(
            np.abs(highs[-period:] - closes[-(period + 1):-1]),
            np.abs(lows[-period:]  - closes[-(period + 1):-1])
        )
    )
    atr  = float(np.mean(tr))
    if atr <= 0:
        return 0.25
    dip = float(np.mean(dm_plus))  / atr
    dim = float(np.mean(dm_minus)) / atr
    dx  = abs(dip - dim) / (dip + dim + 1e-9)
    return float(min(1.0, dx))

def _extract_lc_features(rates: np.ndarray, idx: int) -> Optional[np.ndarray]:
    """指定インデックスの特徴ベクトルを抽出する"""
    if idx < 25:
        return None
    sl = rates[:idx + 1]
    closes = sl["close"]
    highs  = sl["high"]
    lows   = sl["low"]
    f0 = _calc_rsi(closes, 14) / 100.0   # RSI14, 0〜1
    f1 = _calc_rsi(closes, 9)  / 100.0   # RSI9,  0〜1
    f2 = _calc_cci(highs, lows, closes, 20)
    f3 = _calc_adx_approx(highs, lows, closes, 14)
    return np.array([f0, f1, f2, f3], dtype=np.float64)

def lorentzian_classify(rates_m5: np.ndarray) -> tuple[str, float]:
    """
    Lorentzian Classification でエントリー方向と信頼度を返す。
    Returns: (direction: "BUY"|"SELL"|"NEUTRAL", confidence: 0.0-1.0)
    """
    n = len(rates_m5)
    if n < 30:
        return "NEUTRAL", 0.0

    # クエリ: 最新完成バー(-2)
    qidx   = n - 2
    q_feat = _extract_lc_features(rates_m5, qidx)
    if q_feat is None:
        return "NEUTRAL", 0.0

    distances: list[tuple[float, int]] = []
    # LC_MIN_BARS_BK おきにサンプリング (過学習・未来データ混入防止)
    earliest = max(25, qidx - LC_MAX_BARS_BK)
    for i in range(earliest, qidx - LC_MIN_BARS_BK, LC_MIN_BARS_BK):
        feat = _extract_lc_features(rates_m5, i)
        if feat is None:
            continue
        d = float(np.sum(np.log1p(np.abs(q_feat - feat))))
        distances.append((d, i))

    if not distances:
        return "NEUTRAL", 0.0

    distances.sort(key=lambda x: x[0])
    neighbors = distances[:LC_NEIGHBORS]

    buy_votes  = 0
    sell_votes = 0
    for _, idx in neighbors:
        # 近傍バーの4本後の価格変化でラベル付け (未来参照なので注意: トレーニング用のみ)
        future_idx = idx + 4
        if future_idx >= qidx:
            continue
        future_ret = rates_m5[future_idx]["close"] - rates_m5[idx]["close"]
        if future_ret > 0:
            buy_votes  += 1
        elif future_ret < 0:
            sell_votes += 1

    total = buy_votes + sell_votes
    if total == 0:
        return "NEUTRAL", 0.0

    if buy_votes > sell_votes:
        conf = buy_votes / total
        return "BUY", conf
    elif sell_votes > buy_votes:
        conf = sell_votes / total
        return "SELL", conf
    return "NEUTRAL", 0.5


# =============================================================================
#  セクション 27: Q-Trend (方向 + 強さ判定)
#
#  ■ 仕様 (brain_bridge_fxai_v26 準拠):
#     方向:  EMA_fast > EMA_slow → BUY / < → SELL
#     強さ:  ADX近似 > 0.30 → STRONG / それ以下 → NORMAL
#     色変化: 前回バーと方向が変わった → "CHANGE" (転換シグナル)
#
#  ■ 返値: (direction: "BUY"|"SELL"|"NEUTRAL",
#           strength:  "STRONG"|"NORMAL",
#           changed:   bool)
# =============================================================================

QTREND_FAST_EMA = 8
QTREND_SLOW_EMA = 21
QTREND_ADX_THR  = 0.30   # STRONG判定閾値

def _ema(values: np.ndarray, period: int) -> float:
    """直近でのEMA値を返す (単純再帰計算)"""
    if len(values) < period:
        return float(np.mean(values))
    k  = 2.0 / (period + 1)
    em = float(np.mean(values[:period]))
    for v in values[period:]:
        em = v * k + em * (1.0 - k)
    return em

def calc_qtrend(rates: np.ndarray) -> tuple[str, str, bool]:
    """
    Q-Trend を計算する。
    Returns: (direction, strength, changed)
    """
    if len(rates) < QTREND_SLOW_EMA + 5:
        return "NEUTRAL", "NORMAL", False

    closes = rates["close"]
    highs  = rates["high"]
    lows   = rates["low"]

    ema_fast_now  = _ema(closes,        QTREND_FAST_EMA)
    ema_slow_now  = _ema(closes,        QTREND_SLOW_EMA)
    ema_fast_prev = _ema(closes[:-1],   QTREND_FAST_EMA)
    ema_slow_prev = _ema(closes[:-1],   QTREND_SLOW_EMA)

    direction = "BUY"  if ema_fast_now > ema_slow_now  else                 "SELL" if ema_fast_now < ema_slow_now  else "NEUTRAL"
    prev_dir  = "BUY"  if ema_fast_prev > ema_slow_prev else                 "SELL" if ema_fast_prev < ema_slow_prev else "NEUTRAL"
    changed   = direction != prev_dir and direction != "NEUTRAL"

    adx_val   = _calc_adx_approx(highs, lows, closes, 14)
    strength  = "STRONG" if adx_val >= QTREND_ADX_THR else "NORMAL"

    return direction, strength, changed


# =============================================================================
#  セクション 28: Zones Detector
#
#  ■ M15 Confirmed Zone: 強力な壁（床・天井）
#     スウィングH/Lのクラスタリングで「ゾーン帯」を検出。
#     直前バーでゾーン付近から反転した足があれば Confirmed とする。
#
#  ■ M5 Touch Zone: 早期警戒サイン
#     現在価格がゾーン帯に「触れている」かを判定。
#     ゾーンは ±zone_width ($) の帯として評価。
#
#  ■ 返値: ZoneResult (confirmed_support, confirmed_resist,
#                        touching_zone, zone_type, zone_price)
# =============================================================================

ZONE_WIDTH_FACTOR  = 0.4   # ATR_M5 * factor = ゾーン幅 ($)
ZONE_SWING_PERIOD  = 5     # スウィングの前後何本チェックするか
ZONE_MIN_TOUCHES   = 2     # ゾーンとして認定する最低タッチ回数

@dataclass
class ZoneResult:
    confirmed_support: bool  = False
    confirmed_resist:  bool  = False
    touching_zone:     bool  = False
    zone_type:         str   = ""      # "SUPPORT" | "RESIST" | ""
    zone_price:        float = 0.0
    zone_strength:     int   = 0       # タッチ回数


def _find_swing_clusters(rates: np.ndarray, atr: float, is_high: bool
                         ) -> list[tuple[float, int]]:
    """スウィング H or L のクラスタ (価格, タッチ数) リストを返す"""
    prices: list[float] = []
    n = len(rates)
    sw = ZONE_SWING_PERIOD
    for i in range(sw, n - sw):
        p = float(rates[i]["high"]) if is_high else float(rates[i]["low"])
        neighbors_high = rates[max(0, i - sw): i]["high"]
        neighbors_low  = rates[max(0, i - sw): i]["low"]
        after_high = rates[i + 1: min(n, i + sw + 1)]["high"]
        after_low  = rates[i + 1: min(n, i + sw + 1)]["low"]
        if is_high:
            if all(p >= x for x in neighbors_high) and all(p >= x for x in after_high):
                prices.append(p)
        else:
            if all(p <= x for x in neighbors_low) and all(p <= x for x in after_low):
                prices.append(p)

    if not prices:
        return []

    # クラスタリング: ±zone_width以内はまとめる
    width = atr * ZONE_WIDTH_FACTOR
    clusters: list[tuple[float, int]] = []
    for p in sorted(prices):
        merged = False
        for idx, (cp, cnt) in enumerate(clusters):
            if abs(p - cp) <= width:
                clusters[idx] = ((cp * cnt + p) / (cnt + 1), cnt + 1)
                merged = True
                break
        if not merged:
            clusters.append((p, 1))

    return [(cp, cnt) for cp, cnt in clusters if cnt >= ZONE_MIN_TOUCHES]


def detect_zones(rates_m5: np.ndarray,
                 rates_m15: Optional[np.ndarray],
                 atr_m5: float) -> ZoneResult:
    """
    ZoneResult を返す。
    M15 Confirmed: 構造的ゾーン
    M5 Touch: 現在価格がゾーン付近にいるかどうか
    """
    result = ZoneResult()
    if atr_m5 <= 0:
        return result

    # M15 Confirmed Zones
    m15_src = rates_m15 if (rates_m15 is not None and len(rates_m15) >= 30) else None
    if m15_src is not None:
        resist_clusters = _find_swing_clusters(m15_src, atr_m5, is_high=True)
        support_clusters = _find_swing_clusters(m15_src, atr_m5, is_high=False)
        current_m5 = float(rates_m5[-1]["close"])
        zone_width = atr_m5 * ZONE_WIDTH_FACTOR

        for zp, zcnt in resist_clusters:
            if abs(current_m5 - zp) <= zone_width * 2:
                result.confirmed_resist = True
                if not result.zone_price or zcnt > result.zone_strength:
                    result.zone_type    = "RESIST"
                    result.zone_price   = zp
                    result.zone_strength = zcnt

        for zp, zcnt in support_clusters:
            if abs(current_m5 - zp) <= zone_width * 2:
                result.confirmed_support = True
                if not result.zone_price or zcnt > result.zone_strength:
                    result.zone_type    = "SUPPORT"
                    result.zone_price   = zp
                    result.zone_strength = zcnt

    # M5 Touch (現在バーがゾーン帯に接触しているか)
    if result.zone_price > 0:
        bar_high = float(rates_m5[-1]["high"])
        bar_low  = float(rates_m5[-1]["low"])
        half_w   = atr_m5 * ZONE_WIDTH_FACTOR
        in_zone  = bar_high >= result.zone_price - half_w and                    bar_low  <= result.zone_price + half_w
        result.touching_zone = in_zone

    return result


# =============================================================================
#  セクション 29: OSGFC (EMAカラーフィルター)
#
#  ■ 概念:
#     close > EMA(close, period) → Green (上昇トレンド)
#     close < EMA(close, period) → Red   (下降トレンド)
#     前バーと色が変わった場合 → changed=True (転換シグナル)
#
#  ■ 返値: (color: "GREEN"|"RED"|"NEUTRAL", changed: bool)
# =============================================================================

OSGFC_EMA_PERIOD = 34

def calc_osgfc(rates_m5: np.ndarray) -> tuple[str, bool]:
    """
    OSGFC (EMAカラーフィルター) を計算する。
    Returns: (color, changed)
    """
    if len(rates_m5) < OSGFC_EMA_PERIOD + 2:
        return "NEUTRAL", False

    closes = rates_m5["close"]
    ema_now  = _ema(closes,        OSGFC_EMA_PERIOD)
    ema_prev = _ema(closes[:-1],   OSGFC_EMA_PERIOD)

    color_now  = "GREEN" if closes[-1]  > ema_now  else "RED"
    color_prev = "GREEN" if closes[-2]  > ema_prev else "RED"
    changed    = (color_now != color_prev)

    return color_now, changed


# =============================================================================
#  セクション 30: M15 FVG拡張 (LuxAlgo FVG スタイル / ターゲット候補)
#
#  M15足のFVGは「戻り目」や「目標値」として位置付ける。
#  エントリーゲートでは使わず、TP Target / Override判定の参考に使う。
# =============================================================================

M15_FVG_LOOKBACK = 20

def find_m15_fvg_targets(rates_m15: Optional[np.ndarray], action: str
                         ) -> list[float]:
    """
    M15足の未充填FVG中心価格を最大3件リストで返す。
    Returns: [(fvg_mid, ...), ...]  価格で昇順ソート
    """
    if rates_m15 is None or len(rates_m15) < M15_FVG_LOOKBACK + 2:
        return []

    targets: list[float] = []
    window = rates_m15[-M15_FVG_LOOKBACK:]
    for i in range(2, len(window)):
        if action == "BUY":
            if window[i - 2]["high"] < window[i]["low"]:
                mid = (float(window[i - 2]["high"]) + float(window[i]["low"])) / 2.0
                targets.append(mid)
        else:
            if window[i - 2]["low"] > window[i]["high"]:
                mid = (float(window[i - 2]["low"]) + float(window[i]["high"])) / 2.0
                targets.append(mid)

    targets = sorted(set(targets))
    return targets[:3]


# =============================================================================
#  セクション 31: AI GateKeeper (エントリー評価エンジン)
#
#  ■ 評価ステップ (brain_bridge_fxai_v26 のロジックを移植):
#    Step1: 絶対拒絶チェック (EVチェック / 乖離チェック / パニック)
#    Step2: 各インジケーター得点の積み上げ (スコアリング)
#    Step3: 最終判定 (ENTER / SKIP / HOLD)
#
#  ■ 返値: GateResult
# =============================================================================

GATE_MIN_SCORE      = 30    # この得点未満はSKIP
GATE_STRONG_SCORE   = 60    # この得点以上はSTRONG ENTER (乗数1.0以上を許容)

@dataclass
class GateResult:
    verdict:      str   = "SKIP"   # "ENTER" | "SKIP" | "HARD_REJECT"
    score:        float = 0.0
    multiplier:   float = 1.0
    reason:       str   = ""
    hard_reject:  bool  = False
    lc_direction: str   = "NEUTRAL"
    lc_conf:      float = 0.0
    qtrend_dir:   str   = "NEUTRAL"
    qtrend_str:   str   = "NORMAL"
    zone_support: bool  = False
    zone_resist:  bool  = False
    osgfc_color:  str   = "NEUTRAL"
    ctx:          MarketContext = field(default_factory=MarketContext)
    m15_fvg_targets: list = field(default_factory=list)


def ai_gate_keeper(action: str,
                   ctx: MarketContext,
                   lc_dir: str, lc_conf: float,
                   qtrend_dir: str, qtrend_str: str, qtrend_changed: bool,
                   zone: ZoneResult,
                   osgfc_color: str, osgfc_changed: bool,
                   fvg_m5_found: bool,
                   m15_fvg_targets: list[float],
                   session_rank: str,
                   mtf_mult: float) -> GateResult:
    """
    全インジケーターを統合して GateResult を返す。
    """
    result = GateResult()
    result.ctx              = ctx
    result.lc_direction     = lc_dir
    result.lc_conf          = lc_conf
    result.qtrend_dir       = qtrend_dir
    result.qtrend_str       = qtrend_str
    result.zone_support     = zone.confirmed_support
    result.zone_resist      = zone.confirmed_resist
    result.osgfc_color      = osgfc_color
    result.m15_fvg_targets  = m15_fvg_targets

    reasons  = []
    score    = 0.0
    mult     = 1.0

    # ──────────────────────────────────────────────────────────────
    # Step 1: 絶対拒絶チェック (hard reject → HARD_REJECT 即返却)
    # ──────────────────────────────────────────────────────────────

    # [HARD] コスト負け (atr_to_spread < 10)
    if ctx.atr_to_spread > 0 and ctx.atr_to_spread < ATS_HARD_REJECT:
        result.verdict     = "HARD_REJECT"
        result.hard_reject = True
        result.reason      = f"EV_REJECT atr/sp={ctx.atr_to_spread:.1f}<{ATS_HARD_REJECT}"
        log.info("[GATE_AI][HARD] %s", result.reason)
        return result

    # [HARD] 乖離しすぎ (distance_atr_ratio >= 5.0)
    if ctx.distance_atr_ratio >= DIST_HARD_REJECT:
        result.verdict     = "HARD_REJECT"
        result.hard_reject = True
        result.reason      = (f"DIST_REJECT d/atr={ctx.distance_atr_ratio:.2f}"
                              f">={DIST_HARD_REJECT} dir={ctx.direction_vs_sma}")
        log.info("[GATE_AI][HARD] %s", result.reason)
        return result

    # [HARD] パニック相場 (vol_ratio >= 2.0)
    if ctx.vol_state == "PANIC":
        result.verdict     = "HARD_REJECT"
        result.hard_reject = True
        result.reason      = f"PANIC vol_ratio={ctx.volatility_ratio:.2f}>={VOL_PANIC_THR}"
        log.info("[GATE_AI][HARD] %s", result.reason)
        return result

    # ──────────────────────────────────────────────────────────────
    # Step 2: トラップチェック (スコアを大きく削る)
    # ──────────────────────────────────────────────────────────────

    # [TRAP] スクイーズ中 (騙し多発) → スコア −20
    if ctx.vol_state == "SQUEEZE":
        score -= 20
        reasons.append(f"squeeze(vol={ctx.volatility_ratio:.2f})")

    # [TRAP] 伸び切り注意 (4.0 <= d/atr < 5.0) → スコア −15
    if ctx.distance_atr_ratio >= DIST_OVERRIDE_THR:
        score -= 15
        reasons.append(f"dist_warn({ctx.distance_atr_ratio:.1f})")

    # [TRAP] ATR/Spread低評価 (10〜15) → スコア −10
    if 0 < ctx.atr_to_spread < ATS_WARN_THR:
        score -= 10
        reasons.append(f"low_ev(atr/sp={ctx.atr_to_spread:.1f})")

    # ──────────────────────────────────────────────────────────────
    # Step 3: インジケータースコア積み上げ
    # ──────────────────────────────────────────────────────────────

    # [A] Lorentzian Classification (エントリートリガー)
    # LC方向が一致:      +25 (Strong入力 if conf > 0.75)
    # LC方向が不一致:    −20
    # LC Neutral:        ±0
    if lc_dir == action:
        pts = 25 if lc_conf >= 0.75 else 15
        score += pts
        reasons.append(f"LC_{lc_dir}(conf={lc_conf:.2f})+{pts}")
    elif lc_dir not in ("NEUTRAL", ""):
        score -= 20
        reasons.append(f"LC_contra({lc_dir})-20")
    else:
        reasons.append("LC_neutral")

    # [B] Q-Trend (環境認識の主軸) ── 最重要
    # 一致かつSTRONG:    +30
    # 一致かつNORMAL:    +15
    # 不一致:            −25
    # 転換シグナル(一致):+10追加
    if qtrend_dir == action:
        pts = 30 if qtrend_str == "STRONG" else 15
        score += pts
        reasons.append(f"QT_{qtrend_dir}_{qtrend_str}+{pts}")
        if qtrend_changed:
            score += 10
            reasons.append("QT_change+10")
    elif qtrend_dir not in ("NEUTRAL", ""):
        score -= 25
        reasons.append(f"QT_contra({qtrend_dir})-25")
    else:
        reasons.append("QT_neutral")

    # [C] M15 ZonesDetector Confirmed (構造的支柱)
    # BUY+confirmed_support (価格が床の上):  +20
    # SELL+confirmed_resist (価格が天井の下): +20
    # BUY時に confirmed_resist:             −15 (壁が行く手を塞ぐ)
    # SELL時に confirmed_support:           −15
    if action == "BUY" and zone.confirmed_support:
        score += 20
        reasons.append(f"ZONE_SUP_M15+20(p={zone.zone_price:.2f})")
    elif action == "SELL" and zone.confirmed_resist:
        score += 20
        reasons.append(f"ZONE_RES_M15+20(p={zone.zone_price:.2f})")
    if action == "BUY" and zone.confirmed_resist:
        score -= 15
        reasons.append("ZONE_RES_BLOCK-15")
    if action == "SELL" and zone.confirmed_support:
        score -= 15
        reasons.append("ZONE_SUP_BLOCK-15")

    # [D] ZonesDetector Touch (M5) – 早期警戒
    # エントリー方向のゾーンにタッチ中:
    #   BUY→SUPPORT touch: ここから弾む可能性 → +5
    #   SELL→RESIST touch: ここから跳ね返す可能性 → +5
    #   反対ゾーンタッチ: −10 (ブレイク失敗リスク)
    if zone.touching_zone:
        if (action == "BUY"  and zone.zone_type == "SUPPORT") or            (action == "SELL" and zone.zone_type == "RESIST"):
            score +=  5
            reasons.append("ZONE_TOUCH+5")
        else:
            score -= 10
            reasons.append("ZONE_TOUCH_CONTRA-10")

    # [E] LuxAlgo FVG M15 (価格の真空地帯)
    # M15 FVGターゲットが順方向に存在 → +10
    if m15_fvg_targets:
        score += 10
        reasons.append(f"M15_FVG+10(tgt={m15_fvg_targets[0]:.2f})")

    # M5 FVGも確認できている → +5
    if fvg_m5_found:
        score +=  5
        reasons.append("M5_FVG+5")

    # [F] OSGFC (トレンドフィルター)
    # 色一致:       +10
    # 色一致+転換:  +15
    # 色不一致:     −10
    osgfc_match = (action == "BUY" and osgfc_color == "GREEN") or                   (action == "SELL" and osgfc_color == "RED")
    if osgfc_match:
        pts = 15 if osgfc_changed else 10
        score += pts
        reasons.append(f"OSGFC_{osgfc_color}{'_CHG' if osgfc_changed else ''}+{pts}")
    elif osgfc_color not in ("NEUTRAL", ""):
        score -= 10
        reasons.append(f"OSGFC_contra({osgfc_color})-10")

    # [G] セッションランク (補正)
    session_bonus = {"S": +10, "A": 0, "B": -5, "INVALID": -50}
    sb = session_bonus.get(session_rank, 0)
    if sb != 0:
        score += sb
        reasons.append(f"SESS_{session_rank}{'+' if sb >= 0 else ''}{sb}")

    # ──────────────────────────────────────────────────────────────
    # Step 4: 乗数決定
    # ──────────────────────────────────────────────────────────────
    score = max(-100.0, min(150.0, score))

    if score >= GATE_STRONG_SCORE:
        mult = mtf_mult * 1.0    # フル乗数
    elif score >= GATE_MIN_SCORE:
        mult = mtf_mult * 0.8    # やや絞る
    else:
        mult = 0.0               # SKIP

    # ──────────────────────────────────────────────────────────────
    # Step 5: 最終判定
    # ──────────────────────────────────────────────────────────────
    if mult <= 0:
        result.verdict = "SKIP"
    else:
        result.verdict    = "ENTER"
        result.multiplier = mult

    result.score  = score
    result.reason = "|".join(reasons)

    log.info("[GATE_AI] action=%s verdict=%s score=%.0f mult=%.2f "
             "lc=%s(%.2f) qt=%s_%s osgfc=%s dist=%.2f atr/sp=%.1f vol=%s "
             "zone_s=%s zone_r=%s m15fvg=%d reasons=%s",
             action, result.verdict, score, mult,
             lc_dir, lc_conf, qtrend_dir, qtrend_str, osgfc_color,
             ctx.distance_atr_ratio, ctx.atr_to_spread, ctx.vol_state,
             zone.confirmed_support, zone.confirmed_resist,
             len(m15_fvg_targets), result.reason[:120])

    return result


# =============================================================================
#  セクション 32: AI Position Management (保持フェーズ判定 + Override)
#
#  ■ フェーズ判定:
#     Phase1 "NURTURE" (育成): エントリー後30分以内 かつ 浮利 < 1.0R
#     Phase2 "PROTECT" (保護): 保持30分以上 または 浮利 >= 1.0R
#
#  ■ Override利確トリガー:
#     (a) distance_atr_ratio >= DIST_OVERRIDE_THR (4.0) かつ
#         Q-Trendの勢いが NORMAL に落ちた → 平均回帰リスク → 全利確
#     (b) M15 Confirmed Zoneを背にした逆方向シグナルが出た → 反転認定 → 全利確
#     (c) vol_state == "PANIC" → 緊急脱出
#
#  ■ 返値: ManagementAction
# =============================================================================

@dataclass
class ManagementAction:
    phase:          str  = "NURTURE"   # "NURTURE" | "PROTECT"
    override_close: bool = False       # True: 即時全決済推奨
    override_reason: str = ""
    partial_tp:     bool = False       # True: 部分利確推奨
    partial_reason: str  = ""

PHASE_TIME_THR_SEC  = 1800   # 30分
PHASE_PROFIT_THR_R  = 1.0    # 1.0R以上で PROTECT へ


def ai_position_management(entry_price: float,
                           current_price: float,
                           sl_dist: float,
                           action: str,
                           entry_time: datetime,
                           ctx: MarketContext,
                           qtrend_dir: str, qtrend_str: str,
                           zone: ZoneResult,
                           osgfc_color: str) -> ManagementAction:
    """
    保持ポジションの管理アクションを返す。
    """
    mgmt = ManagementAction()

    elapsed_sec = (datetime.now(timezone.utc) - entry_time.replace(tzinfo=timezone.utc)
                   if entry_time.tzinfo is None else
                   (datetime.now(timezone.utc) - entry_time).total_seconds())

    profit = (current_price - entry_price) if action == "BUY"              else (entry_price - current_price)
    profit_r = (profit / sl_dist) if sl_dist > 0 else 0.0

    # --- フェーズ判定 ---
    if elapsed_sec >= PHASE_TIME_THR_SEC or profit_r >= PHASE_PROFIT_THR_R:
        mgmt.phase = "PROTECT"
    else:
        mgmt.phase = "NURTURE"

    # --- Override (a): 伸び切り + 勢い低下 → 平均回帰リスク ---
    if (ctx.distance_atr_ratio >= DIST_OVERRIDE_THR and
            qtrend_str == "NORMAL"):
        mgmt.override_close  = True
        mgmt.override_reason = (f"MEAN_REVERT d/atr={ctx.distance_atr_ratio:.2f}"
                                f" qtrend_strength=NORMAL")
        log.info("[MGMT_AI] Override(a): %s", mgmt.override_reason)
        return mgmt

    # --- Override (b): M15 Confirmed Zone + 逆シグナル = 反転認定 ---
    # BUY保持中: confirmed_resist があり Q-Trend が SELL に転換
    # SELL保持中: confirmed_support があり Q-Trend が BUY に転換
    reverse_zone = (action == "BUY"  and zone.confirmed_resist  and qtrend_dir == "SELL") or                    (action == "SELL" and zone.confirmed_support and qtrend_dir == "BUY")
    if reverse_zone:
        mgmt.override_close  = True
        mgmt.override_reason = (f"REVERSAL_ZONE zone={zone.zone_price:.2f}"
                                f" qtrend={qtrend_dir}")
        log.info("[MGMT_AI] Override(b): %s", mgmt.override_reason)
        return mgmt

    # --- Override (c): パニック相場 ---
    if ctx.vol_state == "PANIC":
        mgmt.override_close  = True
        mgmt.override_reason = f"PANIC vol={ctx.volatility_ratio:.2f}"
        log.info("[MGMT_AI] Override(c): %s", mgmt.override_reason)
        return mgmt

    # --- 部分利確推奨: PROTECT フェーズ + M15 FVGターゲット付近 ---
    if mgmt.phase == "PROTECT" and profit_r >= 0.8:
        mgmt.partial_tp     = True
        mgmt.partial_reason = f"phase=PROTECT profitR={profit_r:.2f}"

    return mgmt


# =============================================================================
#  セクション 33: A+B ハイブリッド AI入力 (TradingView Webhook + MT5フォールバック)
#
#  ■ 設計:
#     A) TradingView アラート受信 (優先)
#        Pine Script の正確な計算結果を Webhook で受け取る。
#        アラートが新鮮 (< TV_SIGNAL_STALE_SEC) なら TV の値を使う。
#
#     B) MT5 直接計算 (フォールバック)
#        TV アラートが届いていない/古い場合に Python で再計算。
#
#  ■ TradingView アラート JSON フォーマット:
#     {
#       "time":                    "{{timenow}}",
#       "symbol":                  "{{ticker}}",
#       "lc_direction":            "BUY",         // "BUY"|"SELL"|"NEUTRAL"
#       "lc_conf":                 0.82,           // 0.0 - 1.0
#       "qtrend_dir":              "BUY",          // "BUY"|"SELL"|"NEUTRAL"
#       "qtrend_str":              "STRONG",       // "STRONG"|"NORMAL"
#       "qtrend_changed":          false,
#       "zone_type":               "SUPPORT",      // "SUPPORT"|"RESIST"|""
#       "zone_price":              2650.5,
#       "zone_confirmed_support":  true,
#       "zone_confirmed_resist":   false,
#       "zone_touching":           false,
#       "zone_strength":           3,
#       "osgfc_color":             "GREEN",        // "GREEN"|"RED"|"NEUTRAL"
#       "osgfc_changed":           false,
#       "distance_atr_ratio":      1.5,
#       "atr_to_spread":           25.0,
#       "volatility_ratio":        1.1,
#       "vol_state":               "NORMAL",       // "NORMAL"|"SQUEEZE"|"PANIC"
#       "direction_vs_sma":        "above",        // "above"|"below"
#       "sma20_m15":               2645.2,
#       "m15_fvg_targets":         [2648.5, 2651.2]
#     }
#
#  ■ Pine Script アラート本文テンプレート (例):
#     {
#       "time": "{{timenow}}", "symbol": "{{ticker}}",
#       "lc_direction": "{{plot("LC_dir")}}", "lc_conf": {{plot("LC_conf")}},
#       "qtrend_dir": "{{plot("QT_dir")}}", "qtrend_str": "{{plot("QT_str")}}",
#       ... (各 plot() の値を埋め込む)
#     }
# =============================================================================

@dataclass
class TVSignalCache:
    """TradingViewから受信した最新インジケーター値"""
    recv_ts:               float = 0.0     # Unix秒 (受信時刻)
    symbol:                str   = ""
    # Lorentzian Classification
    lc_direction:          str   = "NEUTRAL"
    lc_conf:               float = 0.0
    # Q-Trend
    qtrend_dir:            str   = "NEUTRAL"
    qtrend_str:            str   = "NORMAL"
    qtrend_changed:        bool  = False
    # Zones
    zone_type:             str   = ""
    zone_price:            float = 0.0
    zone_confirmed_support: bool = False
    zone_confirmed_resist:  bool = False
    zone_touching:         bool  = False
    zone_strength:         int   = 0
    # OSGFC
    osgfc_color:           str   = "NEUTRAL"
    osgfc_changed:         bool  = False
    # Market Context (from TV)
    distance_atr_ratio:    float = 0.0
    atr_to_spread:         float = 0.0
    volatility_ratio:      float = 1.0
    vol_state:             str   = "NORMAL"
    direction_vs_sma:      str   = ""
    sma20_m15:             float = 0.0
    m15_fvg_targets:       list  = field(default_factory=list)
    source:                str   = "TV"    # "TV" | "MT5"


def _is_tv_fresh(cache: Optional[TVSignalCache]) -> bool:
    """TV キャッシュが存在かつ新鮮かどうかを返す"""
    if cache is None:
        return False
    age = time.time() - cache.recv_ts
    return age <= TV_SIGNAL_STALE_SEC


def _parse_tv_alert(data: dict) -> Optional[TVSignalCache]:
    """Webhook JSON を TVSignalCache に変換する (型変換/バリデーション付き)"""
    try:
        c = TVSignalCache()
        c.recv_ts       = time.time()
        c.symbol        = str(data.get("symbol", SYMBOL))
        c.lc_direction  = str(data.get("lc_direction", "NEUTRAL")).upper()
        c.lc_conf       = float(data.get("lc_conf", 0.0))
        c.qtrend_dir    = str(data.get("qtrend_dir", "NEUTRAL")).upper()
        c.qtrend_str    = str(data.get("qtrend_str", "NORMAL")).upper()
        c.qtrend_changed = bool(data.get("qtrend_changed", False))

        c.zone_type             = str(data.get("zone_type", "")).upper()
        c.zone_price            = float(data.get("zone_price", 0.0))
        c.zone_confirmed_support = bool(data.get("zone_confirmed_support", False))
        c.zone_confirmed_resist  = bool(data.get("zone_confirmed_resist",  False))
        c.zone_touching          = bool(data.get("zone_touching",          False))
        c.zone_strength          = int(data.get("zone_strength",   0))

        c.osgfc_color   = str(data.get("osgfc_color", "NEUTRAL")).upper()
        c.osgfc_changed = bool(data.get("osgfc_changed", False))

        c.distance_atr_ratio = float(data.get("distance_atr_ratio", 0.0))
        c.atr_to_spread      = float(data.get("atr_to_spread",      0.0))
        c.volatility_ratio   = float(data.get("volatility_ratio",   1.0))
        c.vol_state          = str(data.get("vol_state",   "NORMAL")).upper()
        c.direction_vs_sma   = str(data.get("direction_vs_sma", ""))
        c.sma20_m15          = float(data.get("sma20_m15", 0.0))

        raw_fvg = data.get("m15_fvg_targets", [])
        c.m15_fvg_targets = [float(x) for x in raw_fvg if x is not None][:3]

        c.source = "TV"
        return c
    except Exception as e:
        log.warning("[WEBHOOK] Parse error: %s  data=%s", e, str(data)[:200])
        return None


def _tv_cache_to_zone_result(c: TVSignalCache, atr_m5: float) -> ZoneResult:
    """TVSignalCache を ZoneResult に変換する"""
    z = ZoneResult()
    z.confirmed_support = c.zone_confirmed_support
    z.confirmed_resist  = c.zone_confirmed_resist
    z.touching_zone     = c.zone_touching
    z.zone_type         = c.zone_type
    z.zone_price        = c.zone_price
    z.zone_strength     = c.zone_strength
    return z


def _tv_cache_to_market_context(c: TVSignalCache,
                                 rates_m5: np.ndarray,
                                 spread: float,
                                 session_rank: str) -> MarketContext:
    """TVSignalCache を MarketContext に変換する"""
    ctx = MarketContext()
    ctx.current_session     = session_rank
    ctx.distance_atr_ratio  = c.distance_atr_ratio
    ctx.atr_to_spread       = c.atr_to_spread if c.atr_to_spread > 0 else (
                                calc_atr(rates_m5, 14) / spread if spread > 0 else 0.0)
    ctx.volatility_ratio    = c.volatility_ratio
    ctx.vol_state           = c.vol_state
    ctx.direction_vs_sma    = c.direction_vs_sma
    ctx.sma20_m15           = c.sma20_m15
    ctx.atr_m5              = calc_atr(rates_m5, 14)
    return ctx


# ── Flask Webhook サーバー ───────────────────────────────────────────────────

_tv_flask_app = _Flask("tv_webhook") if FLASK_AVAILABLE else None


def _create_flask_app():
    """Flask アプリを設定して返す"""
    app = _Flask("tv_webhook")

    @app.route("/", methods=["GET"])
    def health():
        fresh = _is_tv_fresh(g_tv_cache)
        age   = round(time.time() - g_tv_cache.recv_ts, 1) if g_tv_cache else -1
        return _flask_jsonify({"status": "ok", "tv_fresh": fresh, "age_sec": age})

    @app.route("/webhook", methods=["POST"])
    def webhook():
        global g_tv_cache
        try:
            data = _flask_request.get_json(force=True, silent=True) or {}
            cache = _parse_tv_alert(data)
            if cache is not None:
                with g_tv_cache_lock:
                    g_tv_cache = cache
                log.info("[WEBHOOK] TV alert received: sym=%s lc=%s(%s) qt=%s_%s "
                         "osgfc=%s zone=%s dist=%.2f vol=%s",
                         cache.symbol, cache.lc_direction, f"{cache.lc_conf:.2f}",
                         cache.qtrend_dir, cache.qtrend_str, cache.osgfc_color,
                         cache.zone_type, cache.distance_atr_ratio, cache.vol_state)
                return _flask_jsonify({"ok": True}), 200
            else:
                return _flask_jsonify({"ok": False, "error": "parse_failed"}), 400
        except Exception as e:
            log.error("[WEBHOOK] Unexpected error: %s", e, exc_info=True)
            return _flask_jsonify({"ok": False, "error": str(e)}), 500

    return app


def start_tv_webhook_server() -> None:
    """TV Webhook Flask サーバーをバックグラウンドスレッドで起動する"""
    if not TV_WEBHOOK_ENABLED:
        log.info("[WEBHOOK] TV_WEBHOOK_ENABLED=False. Skipping.")
        return
    if not FLASK_AVAILABLE:
        log.warning("[WEBHOOK] Flask not available. TV Webhook disabled.")
        return

    def _run():
        try:
            app = _create_flask_app()
            log.info("[WEBHOOK] Flask server starting on port %d ...", TV_WEBHOOK_PORT)
            # use_reloader=False 必須 (サブスレッドから起動するため)
            import logging as _std_logging
            _std_logging.getLogger("werkzeug").setLevel(_std_logging.WARNING)
            app.run(host="0.0.0.0", port=TV_WEBHOOK_PORT,
                    use_reloader=False, threaded=True)
        except Exception as e:
            log.error("[WEBHOOK] Server error: %s", e, exc_info=True)

    t = threading.Thread(target=_run, name="tv_webhook", daemon=True)
    t.start()
    log.info("[WEBHOOK] TV Webhook thread started (port=%d, stale=%ds)",
             TV_WEBHOOK_PORT, TV_SIGNAL_STALE_SEC)


# ── ハイブリッド AI入力 解決関数 ─────────────────────────────────────────────

def resolve_hybrid_ai_inputs(
        rates_m5: np.ndarray,
        rates_m15: Optional[np.ndarray],
        spread: float,
        atr_m5: float,
        action: str,
        session_rank: str,
) -> tuple:
    """
    AI GateKeeper が必要とする全入力を解決する。

    ■ 優先順位:
       1. TradingView Webhook キャッシュ (新鮮な場合)
       2. MT5 OHLCV による Python 内計算 (フォールバック)

    Returns: (ctx, lc_dir, lc_conf, qtrend_dir, qtrend_str, qtrend_changed,
               zone, osgfc_color, osgfc_changed, m15_fvg_targets, source_tag)
    """
    with g_tv_cache_lock:
        tv = g_tv_cache  # スナップショット取得

    if _is_tv_fresh(tv):
        # ── SOURCE: TradingView ──────────────────────────────────────
        age = round(time.time() - tv.recv_ts, 1)
        log.debug("[HYBRID] Using TV data (age=%.1fs)", age)

        ctx  = _tv_cache_to_market_context(tv, rates_m5, spread, session_rank)
        zone = _tv_cache_to_zone_result(tv, atr_m5)

        return (
            ctx,
            tv.lc_direction, tv.lc_conf,
            tv.qtrend_dir, tv.qtrend_str, tv.qtrend_changed,
            zone,
            tv.osgfc_color, tv.osgfc_changed,
            tv.m15_fvg_targets,
            "TV",
        )
    else:
        # ── SOURCE: MT5 計算 (フォールバック) ───────────────────────
        reason = "no_cache" if tv is None else f"stale({time.time()-tv.recv_ts:.0f}s)"
        log.debug("[HYBRID] Fallback to MT5 calc (%s)", reason)

        ctx             = calc_market_context(rates_m5, rates_m15, spread, session_rank)
        lc_dir, lc_conf = lorentzian_classify(rates_m5)
        qtrend_dir, qtrend_str, qtrend_changed = calc_qtrend(rates_m5)
        zone            = detect_zones(rates_m5, rates_m15, atr_m5)
        osgfc_color, osgfc_changed = calc_osgfc(rates_m5)
        m15_fvg_targets = find_m15_fvg_targets(rates_m15, action)

        return (
            ctx,
            lc_dir, lc_conf,
            qtrend_dir, qtrend_str, qtrend_changed,
            zone,
            osgfc_color, osgfc_changed,
            m15_fvg_targets,
            "MT5",
        )


# =============================================================================
#  セクション 23: メインLRR シグナル生成ループ (AI評価統合版)
# =============================================================================

def try_generate_lrr_signal() -> Optional[LRRSignal]:
    """
    LRR戦略のシグナル生成を試みる。
    シグナル発生: LRRSignal を返す
    シグナルなし: None を返す

    ■ フロー:
        1. M5/M15データ取得
        2. セッションランク判定 → DeadZoneはNone
        3. ニュースフィルター
        4. SpreadMed更新 → スプレッドゲート
        5. LP検出 + Sweep検出
        6. Volumeフィルター
        7. FVG確認 (M5 + M15)
        8. MTFアライメント
        9. MarketContext計算
       10. Lorentzian Classification
       11. Q-Trend
       12. Zones Detector
       13. OSGFC
       14. AI GateKeeper → HARD_REJECT / SKIP / ENTER 判定
       15. 最終信頼度スコア計算
       16. LRRSignal生成
    """
    # ---------- データ取得 ----------
    rates_m5  = get_rates_m5(count=max(LP_LOOKBACK + VOLUME_LOOKBACK + 10,
                                       LC_MAX_BARS_BK + 30))
    rates_m15 = get_rates_m15(count=max(MTF_LOOKBACK + 5, M15_FVG_LOOKBACK + 5,
                                        SMA20_PERIOD + 10))

    if rates_m5 is None or len(rates_m5) < LP_LOOKBACK + 2:
        log.debug("[LRR] Insufficient M5 data.")
        return None

    # ---------- バー確定チェック ----------
    global g_last_bar_time
    bar_ts = datetime.fromtimestamp(int(rates_m5[-2]["time"]), tz=timezone.utc)
    if g_last_bar_time is not None and bar_ts <= g_last_bar_time:
        return None
    g_last_bar_time = bar_ts

    bar = {k: float(rates_m5[-2][k]) for k in ("open", "high", "low", "close")}
    bar["tick_volume"] = float(rates_m5[-2]["tick_volume"])

    # ---------- セッションランク ----------
    now_jst = get_jst_now()
    session_rank, session_mult = get_session_rank(now_jst)
    if session_rank == "INVALID":
        log.debug("[GATE] DeadZone(12:00-16:59 JST) -> skip")
        return None

    # ---------- ニュースフィルター ----------
    news_blocked, news_reason = is_news_blocked(now_jst)
    if news_blocked:
        log.info("[GATE] News filter: %s", news_reason)
        return None

    # ---------- スプレッド ----------
    spread = get_symbol_spread()
    update_spread_med(spread)

    # ---------- ATR ----------
    atr_m5 = calc_atr(rates_m5, period=14)
    if atr_m5 <= 0:
        log.debug("[GATE] ATR unavailable.")
        return None

    # スプレッドゲート (Python側予備チェック)
    sp_blocked, sp_reason = is_spread_blocked(spread, atr_m5)
    if sp_blocked:
        log.debug("[GATE] Spread: %s", sp_reason)
        return None

    # ---------- 流動性プール検出 ----------
    prev_rates = rates_m5[:-1]
    swing_high, swing_low = find_liquidity_pools(prev_rates[:-1], LP_LOOKBACK)

    # ---------- Sweep検出 ----------
    action, sweep_extreme = detect_sweep(bar, swing_high, swing_low, atr_m5)
    if action is None:
        return None

    log.info("[LRR] *** SWEEP DETECTED *** action=%s extreme=%.3f "
             "swingH=%.3f swingL=%.3f bar_ts=%s",
             action, sweep_extreme, swing_high, swing_low,
             bar_ts.strftime("%Y-%m-%d %H:%M"))

    # ---------- Volumeフィルター ----------
    vol_passed, vol_ratio = check_volume_filter(rates_m5[:-1])
    if not vol_passed:
        log.info("[GATE] Volume filter: vol_ratio=%.2f < %.1f -> skip",
                 vol_ratio, VOLUME_MULT_FILTER)
        return None

    # ---------- FVG確認 (M5) ----------
    fvg_price = find_fvg(rates_m5[:-1], action, MAX_FVG_BARS_BACK)
    fvg_m5_found = fvg_price is not None

    # ---------- MTFアライメント (15m) ----------
    mtf_ok, mtf_mult, mtf_reason = check_mtf_alignment(action, rates_m15)
    if not mtf_ok:
        log.info("[GATE] MTF alignment blocked: %s", mtf_reason)
        return None

    # ---------- ボラレジーム (H1 ATR) ----------
    rates_h1 = mt5.copy_rates_from_pos(SYMBOL, TIMEFRAME_H1, 0, 20)
    atr_h1   = calc_atr(rates_h1, period=14) if rates_h1 is not None else 0.0
    vol_regime, vol_mult = get_vol_regime(atr_m5, atr_h1)

    # =====================================================
    # AI評価フェーズ: §33 ハイブリッド入力 → §31 GateKeeper
    # =====================================================

    # --- §33: ハイブリッド AI入力解決 (TV優先 / MT5フォールバック) ---
    (ctx, lc_dir, lc_conf,
     qtrend_dir, qtrend_str, qtrend_changed,
     zone, osgfc_color, osgfc_changed,
     m15_fvg_targets, ai_source) = resolve_hybrid_ai_inputs(
        rates_m5=rates_m5, rates_m15=rates_m15,
        spread=spread, atr_m5=atr_m5,
        action=action, session_rank=session_rank,
    )
    log.debug("[HYBRID] AI inputs resolved from: %s", ai_source)

    # --- §31: AI GateKeeper ---
    gate = ai_gate_keeper(
        action         = action,
        ctx            = ctx,
        lc_dir         = lc_dir,
        lc_conf        = lc_conf,
        qtrend_dir     = qtrend_dir,
        qtrend_str     = qtrend_str,
        qtrend_changed = qtrend_changed,
        zone           = zone,
        osgfc_color    = osgfc_color,
        osgfc_changed  = osgfc_changed,
        fvg_m5_found   = fvg_m5_found,
        m15_fvg_targets = m15_fvg_targets,
        session_rank   = session_rank,
        mtf_mult       = mtf_mult,
    )

    if gate.verdict in ("SKIP", "HARD_REJECT"):
        log.info("[GATE_AI] %s: %s", gate.verdict, gate.reason)
        return None

    # =====================================================
    # 最終信頼度スコア計算 (AI Gate得点を基にキャリブレーション)
    # =====================================================
    risk_pct    = calc_risk_pct(session_mult, vol_mult, spread)
    # AI gate スコアを 0〜100 にマッピング (gate range: -100〜150 → clamp 0〜100)
    ai_score_norm = max(0.0, min(100.0, (gate.score + 30) / 180.0 * 100.0))
    # 旧calc_confidenceと統合 (重み: AI 70% / 旧ロジック 30%)
    base_conf, base_reason = calc_confidence(
        session_rank, vol_regime, vol_ratio, mtf_mult, fvg_m5_found
    )
    confidence = ai_score_norm * 0.70 + base_conf * 0.30

    ai_reason = (f"[{ai_source}]gate={gate.score:.0f}|{gate.reason[:80]}"
                 f"|lc={lc_dir}({lc_conf:.2f})"
                 f"|qt={qtrend_dir}_{qtrend_str}"
                 f"|osgfc={osgfc_color}"
                 f"|dist={ctx.distance_atr_ratio:.2f}"
                 f"|atr/sp={ctx.atr_to_spread:.1f}"
                 f"|vol={ctx.vol_state}"
                 f"|{base_reason}")

    log.info("[LRR] FINAL signal: action=%s conf=%.0f gate=%s "
             "sess=%s vol=%s riskPct=%.3f%% "
             "lc=%s qt=%s_%s osgfc=%s "
             "dist=%.2f atr/sp=%.1f vol_state=%s "
             "zone_s=%s zone_r=%s m15fvg=%d mult=%.2f",
             action, confidence, gate.verdict,
             session_rank, vol_regime, risk_pct,
             lc_dir, qtrend_dir, qtrend_str, osgfc_color,
             ctx.distance_atr_ratio, ctx.atr_to_spread, ctx.vol_state,
             zone.confirmed_support, zone.confirmed_resist,
             len(m15_fvg_targets), gate.multiplier)

    return LRRSignal(
        action        = action,
        sweep_extreme = sweep_extreme,
        atr_m5        = atr_m5,
        ai_confidence = round(confidence, 1),
        ai_reason     = ai_reason[:200],
        session_rank  = session_rank,
        vol_regime    = vol_regime,
        multiplier    = round(gate.multiplier, 3),
    )


# =============================================================================
#  セクション 24: メインループ
# =============================================================================

def main() -> None:
    log.info("=" * 60)
    log.info("fxai_lrr_brain.py  LRR Brain v3.1  starting...")
    log.info("Symbol=%s  ZMQ_PUSH=%s  ZMQ_PULL=%s",
             SYMBOL, ZMQ_PUSH_URL, ZMQ_PULL_URL)
    log.info("=" * 60)

    # --- MT5 初期化 ---
    if not mt5_init():
        log.error("[MAIN] MT5 init failed. Exiting.")
        return

    # --- ZMQ 初期化 ---
    if not zmq_init():
        log.error("[MAIN] ZMQ init failed. Exiting.")
        mt5.shutdown()
        return

    # --- ハートビート受信スレッド ---
    hb_thread = threading.Thread(target=heartbeat_receiver_thread, daemon=True)
    hb_thread.start()

    # --- TradingView Webhook サーバー ---
    start_tv_webhook_server()

    # --- 日次状態初期化 ---
    check_daily_reset()

    log.info("[MAIN] Main loop started. interval=%.1fs", MAIN_LOOP_INTERVAL_SEC)

    prev_news_blocked = False

    try:
        while True:
            loop_start = time.time()

            # 日次リセット
            check_daily_reset()

            # 日次停止チェック
            halted, halt_reason = is_daily_halted()
            if halted:
                log.info("[MAIN] Daily halt: %s. Sleeping...", halt_reason)
                time.sleep(MAIN_LOOP_INTERVAL_SEC * 5)
                continue

            # MT5ハートビート停止チェック
            if g_hb.last_recv_ts > 0 and g_hb.mt5_halt:
                log.info("[MAIN] MT5 halt detected (system=%s). Waiting...",
                         g_hb.mt5_emg_system)
                time.sleep(MAIN_LOOP_INTERVAL_SEC * 3)
                continue

            # ニュースフィルター状態をMT5に同期
            now_jst = get_jst_now()
            news_blocked, news_reason = is_news_blocked(now_jst)
            if news_blocked != prev_news_blocked:
                send_news_block_to_mt5(news_blocked)
                if news_blocked:
                    log.info("[NEWS] Sending block to MT5: %s", news_reason)
                else:
                    log.info("[NEWS] Block cleared. Notifying MT5.")
            prev_news_blocked = news_blocked

            # シグナル生成試行
            try:
                signal = try_generate_lrr_signal()
            except Exception as e:
                log.error("[MAIN] Signal generation error: %s", e, exc_info=True)
                signal = None

            if signal is not None:
                ok = send_signal(signal)
                if ok:
                    on_entry_sent(calc_risk_pct(
                        {"S": 1.5, "A": 1.0, "B": 0.7}.get(signal.session_rank, 1.0),
                        VOL_MULT_NORMAL,
                        get_symbol_spread()
                    ))

            # ループ時間調整
            elapsed = time.time() - loop_start
            sleep_sec = max(0.1, MAIN_LOOP_INTERVAL_SEC - elapsed)
            time.sleep(sleep_sec)

    except KeyboardInterrupt:
        log.info("[MAIN] KeyboardInterrupt. Shutting down...")
    finally:
        if MT5_AVAILABLE:
            mt5.shutdown()
            log.info("[MAIN] MT5 shutdown.")
        if g_zmq_push:
            g_zmq_push.close()
        if g_zmq_pull:
            g_zmq_pull.close()
        if g_zmq_ctx:
            g_zmq_ctx.term()
        log.info("[MAIN] ZMQ closed. Bye.")


if __name__ == "__main__":
    main()
