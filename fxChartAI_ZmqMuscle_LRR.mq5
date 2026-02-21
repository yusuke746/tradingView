//+------------------------------------------------------------------+
//|  fxChartAI_ZmqMuscle_LRR.mq5                                    |
//|  XAUUSD 5M Execution Engine - LRR Edition v3.21                 |
//|                                                                  |
//|  ■ アーキテクチャ:                                               |
//|    Python(Brain) <--ZMQ--> MT5(Muscle)                          |
//|                                                                  |
//|  ■ 実装内容:                                                     |
//|    [1] LRR (Liquidity Raid Reversal) 2次フィルタ                 |
//|    [2] ボラティリティ適応型ロット管理 (Risk Manager設計書)        |
//|    [3] 3系統Emergency Cut (Flash / Blowout / Quality)            |
//|    [4] ハイブリッド処理 (Python信号 + MT5リアルタイムフィルタ)    |
//|    [5] 軽量SpreadMed (Robbins-Monro逐次中央値推定)               |
//|    [6] TP1(50%) / TP2(30%) / TP3(20%) 段階決済                  |
//|    [7] LRR日次管理 (エントリー3回/日・連敗停止・ニュースフィルタ) |
//|    [8] AI trail_mode/tp_mode (v26 HOLD/CLOSEから受信してチューニング) |
//+------------------------------------------------------------------+
#property version   "3.22"
#property description "ZmqMuscle LRR v3.22 - grade B (50% lot) + dynamic TTL + Lorentzian retrigger"

#include <Zmq/Zmq.mqh>
#include <Trade/Trade.mqh>
#include <Trade/SymbolInfo.mqh>
#include <Trade/PositionInfo.mqh>
#include <JAson.mqh>

#ifndef ZMQ_NOBLOCK
   #ifdef ZMQ_DONTWAIT
      #define ZMQ_NOBLOCK ZMQ_DONTWAIT
   #else
      #define ZMQ_NOBLOCK 1
   #endif
#endif

//==========================================================================
//  セクション 1: 入力パラメータ
//==========================================================================

// --- ZeroMQ ---
input group "=== ZeroMQ ==="
input string InpZmqUrl              = "tcp://localhost:5555";
input bool   InpHeartbeatEnabled    = true;
input string InpHeartbeatUrl        = "tcp://localhost:5556";
input int    InpHeartbeatIntervalMs = 1000;

// --- 基本設定 ---
input group "=== 基本設定 ==="
input int    InpMagicNumber         = 20260221;
input int    InpMaxPositions        = 3;

// --- ATR SLパラメータ (Risk Manager §1) ---
// StopDistance = max(StructureSL距離, ATRDistance)
// StructureSL  = SweepExtreme +/- max(BufferMin, Spread*BufferSpreadMult)
// ATRDistance  = clamp(ATR_M5*K_atr, MinATRStop, MaxATRStop)
input group "=== ATR SL (Risk Manager §1) ==="
input double InpKatr                = 1.2;   // ATR係数 (NY=1.0 / 通常=1.2 / 荒れ日=1.4)
input double InpMinATRStop          = 0.50;  // 最小SL距離 ($) [Phase5: $5100価格水準対応]
input double InpMaxATRStop          = 10.00; // 最大SL距離 ($) [Phase5: $5100価格水準対応]
input double InpBufferMin           = 0.05;  // 最小Structureバッファ ($)
input double InpBufferSpreadMult    = 1.5;   // スプレッド連動バッファ倍率

// --- ボラ適応型リスク管理 (Risk Manager §2) ---
// RiskPct = BaseRisk * SessionMult * VolMult * SpreadMult
// ただし RiskPct <= MaxRiskPerTrade  AND DailyLossUsed+RiskPct <= DailyLossCapPct
input group "=== Vol-Adaptive Risk (Risk Manager §2) ==="
input double InpBaseRisk            = 0.40;  // ベースリスク (%)
input double InpMaxRiskPerTrade     = 0.60;  // 1Trade最大リスク (%)
input double InpDailyLossCapPct     = 3.00;  // 日次損失上限 (%) [Phase5: 検証期間暫定。フルロット移行時に2.00へ戻すこと]
input double InpVolRatioHigh        = 1.35;  // 荒い判定: ATR_M5/ATR_H1 >= この値
input double InpVolRatioLow         = 0.85;  // 静か判定: ATR_M5/ATR_H1 <  この値
input double InpVolMultHigh         = 0.55;  // 荒い時VolMult
input double InpVolMultNormal       = 1.00;  // 通常VolMult
input double InpVolMultLow          = 1.10;  // 静か時VolMult
input double InpSpreadRefDollar     = 0.30;  // SpreadMult基準値 ($)

// --- スプレッドガード (Risk Manager §4) ---
// (A) spread > SpreadHardCap  → 絶対停止
// (B) spread > SpreadMed*SpreadSpikeMult → スパイク停止
// (C) spread > ATR_M5*SpreadATRMult → ATR比過大停止
input group "=== Spread Guard (Risk Manager §4) ==="
input double InpSpreadHardCap       = 0.50;  // 絶対停止閾値 ($)
input double InpSpreadSpikeMult     = 2.5;   // SpreadMed比スパイク判定
input double InpSpreadATRMult       = 0.18;  // ATR比過大判定
input int    InpSpreadMedBufSize    = 300;   // リングバッファサイズ (ticks)

// --- 3系統Emergency Cut (Risk Manager §5) ---
input group "=== System 1: Flash Move Cut ==="
input bool   InpFlashCutEnabled     = true;
input double InpShockMult           = 1.6;   // 発動: adverse > ATR_1m * ShockMult
input int    InpShockWindowSec      = 90;    // エントリー後この秒数以内のみ判定
input double InpFlashSpreadSpike    = 1.8;   // AND: spread > SpreadMed * この値

input group "=== System 2: Spread Blowout Cut ==="
input bool   InpBlowoutEnabled      = true;
input double InpBlowMult            = 3.0;   // SpreadMed*BlowMult超でBlowout判定
input double InpBlowCritMult        = 4.0;   // SpreadMed*CritMult超で即全決済
input double InpFloatingRPartial    = 0.7;   // FloatingR超で部分決済
input double InpFloatingRFull       = 1.1;   // FloatingR超で全決済
input double InpPartialClosePct     = 50.0;  // 部分決済割合 (%)

input group "=== System 3: Slippage Quality Cut ==="
input bool   InpQualityCutEnabled   = true;
input double InpSlippageCap         = 0.20;  // スリッページ許容上限 ($)
input int    InpSlippageStrikes     = 2;     // この回数でセッション停止

// --- LRR日次ルール ---
input group "=== LRR Daily Rules ==="
input int    InpMaxEntriesPerDay    = 6;     // 1日最大エントリー数 [Phase5: 検証期間緩和]
input int    InpMaxConsecLosses     = 5;     // 連敗停止回数 [Phase5: 検証期間緩和]
input int    InpMaxWeeklyConsecLosses = 10; // [Phase2] 週次連敗上限 (0=無効) [Phase5: 検証期間緩和]
input bool   InpNewsFilterEnabled   = true;  // ニュースフィルター有効

// --- TP段階決済 (LRR §5) ---
// TP1(50%): profit >= SL距離*TP1AtrMult (最低RR 1:2)
// TP2(30%): profit >= ATR_M5*TP2AtrMult (残りの60%を決済)
// TP3(20%): トレーリングに委任
input group "=== LRR TP Ladder ==="
input double InpTP1AtrMult          = 2.0;
input double InpTP2AtrMult          = 3.5;
input double InpTP3AtrMult          = 5.0;   // TP/ExecuteOrder用広め初期TP
input double InpTP1ClosePct         = 50.0;  // TP1到達時決済割合 (%)
input double InpTP2ClosePct         = 60.0;  // TP2到達時残りの決済割合 (%)

// --- AI Trail/TP Mode チューニング (v26 brain_bridge から受信) ---
// v26 の HOLD/CLOSE メッセージに含まれる trail_mode/tp_mode を受信し、
// LRR の TP ラダー閾値とトレーリング距離をリアルタイム調整する。
//   WIDE : 大きく引きつけてロングランを狙う
//   NORMAL: デフォルト (倍率=1.0)
//   TIGHT: 早めに利益確定する
input group "=== AI Mode Multipliers (trail_mode / tp_mode) ==="
input double InpTpWideMultiplier    = 1.40;  // tp_mode WIDE時のTP距離倍率
input double InpTpTightMultiplier   = 0.68;  // tp_mode TIGHT時のTP距離倍率
input double InpTrailWideDistMult   = 1.50;  // trail_mode WIDE時のTrailDist倍率
input double InpTrailWidStartMult   = 1.30;  // trail_mode WIDE時のTrailStart倍率
input double InpTrailTightDistMult  = 0.60;  // trail_mode TIGHT時のTrailDist倍率
input double InpTrailTightStartMult = 0.80;  // trail_mode TIGHT時のTrailStart倍率
input int    InpAiModeDecaySec      = 600;   // この秒後にNORMALへ自動リセット (0=無効)

// --- トレーリング ---
input group "=== Trailing ==="
input bool   InpTrailingEnabled     = true;
input double InpTrailStartAtrMult   = 1.4;
input double InpTrailDistAtrMult    = 2.0;
input double InpTrailStepAtrMult    = 0.5;

// --- 時間帯フィルター ---
input group "=== Time Filter ==="
input int    InpTradingStartHour    = 1;
input int    InpHardCloseHour       = 23;
input int    InpHardCloseMinute     = 55;

// --- Legacy Emergency (Margin Guard継承) ---
input group "=== Legacy Emergency (Margin Guard) ==="
input bool   InpLegacyEmgEnabled    = true;
input bool   InpEmergencyLogOnly    = false;
input double InpEmergencyLossPct    = 4.0;
input double InpEmergencyMarginLvl  = 250.0;
input bool   InpPersistEmgState     = false;
input string InpEmgStateKeyPrefix   = "FXAI_LRR_EMG";

// --- スリッページ ---
input group "=== Slippage ==="
input int    InpEntrySlipPoints     = 20;
input int    InpMgmtSlipPoints      = 80;

// --- ロット上限 ---
input group "=== Lot Limits ==="
input double InpMaxAllowedLot       = 1.0;
input double InpLotFallback         = 0.01;

//==========================================================================
//  セクション 2: Enum / Struct
//==========================================================================

enum ENUM_VOL_REGIME {
   VOL_HIGH   = 2,   // ATR_M5/ATR_H1 >= VolRatioHigh
   VOL_NORMAL = 1,   // 通常
   VOL_LOW    = 0    // ATR_M5/ATR_H1 <  VolRatioLow
};

enum ENUM_SESSION_RANK {
   SESSION_INVALID = 0,   // 無効帯 (DeadZone 12:00-16:59 JST)
   SESSION_B       = 70,  // B級 × 0.70 (アジア初動)
   SESSION_A       = 100, // A級 × 1.00 (ロンドン)
   SESSION_S       = 150  // S級 × 1.50 (NYオープン)
};

struct LRR_POS_RECORD {
   ulong    ticket;
   datetime entryTime;
   double   entryPrice;
   double   initialRDollar;  // 当初リスク額 ($)
   double   stopDist;        // SL距離 ($)
   string   action;          // "BUY" / "SELL"
   bool     tp1Hit;
   bool     tp2Hit;
   bool     isPyramid;       // [Phase4] ピラミッドエントリーフラグ
};

//==========================================================================
//  セクション 3: グローバル変数
//==========================================================================

Context  g_ctx;
Socket   g_socket(g_ctx, ZMQ_PULL);
Socket   g_hbSocket(g_ctx, ZMQ_PUSH);
CTrade   g_trade;

bool     g_hbConnected   = false;
uint     g_lastHbSentMs  = 0;
ulong    g_hbSendFails   = 0;
ulong    g_zmqDeserFails = 0;

int  g_atrM5Handle = INVALID_HANDLE;
int  g_atrM1Handle = INVALID_HANDLE;
int  g_atrH1Handle = INVALID_HANDLE;

// 日次ガード (v2.7継承構造を明示的に維持)
datetime g_dayStart       = 0;
double   g_dayStartEquity = 0.0;
bool     g_haltEntries    = false;
bool     g_emgHaltActive  = false;

// Legacy Emergency フラグ (v2.7から継承 – ログの連続性確保)
bool     g_emergencyFiredToday = false;
datetime g_emergencyFiredAt    = 0;
datetime g_emergencyFiredDate  = 0;

// 3系統 Emergency フラグ (ログ: g_lastEmgSystem で系統を明示)
bool     g_ec1FlashFiredToday   = false;  // System 1: EC_FLASH
bool     g_ec2BlowoutFiredToday = false;  // System 2: EC_BLOWOUT_FULL/PARTIAL
int      g_ec3SlipStrikes       = 0;      // System 3: スリッページカウント
bool     g_ec3SessionHalt       = false;  // System 3: EC_QUALITY セッション停止
string   g_lastEmgSystem        = "";     // 最後に発動したシステム名 (ログ・HB共通)

// LRR日次カウンター
int      g_dailyEntryCount   = 0;
int      g_consecutiveLosses = 0;
double   g_dailyRiskUsedPct  = 0.0;

// [Phase2] 週次連敗カウンター
int      g_weeklyConsecutiveLosses = 0;   // 週内連敗数（金曜終値でリセット）
datetime g_weekStart               = 0;  // 現在週の開始日時（週変わり検出用）

// ポジションレジストリ (Flash Cut / TP管理用, 最大10)
LRR_POS_RECORD g_posReg[10];
int            g_posRegCount = 0;

// SpreadMed リングバッファ (Robbins-Monro逐次中央値推定)
double   g_spreadBuf[];
int      g_spreadBufIdx  = 0;
int      g_spreadBufFill = 0;
double   g_spreadMed     = 0.20;
bool     g_spreadInited  = false;

datetime g_lastHoldAt = 0;
datetime g_panicUntil = 0;

// ニュースブロック
bool     g_newsBlockActive = false;
datetime g_newsBlockUntil  = 0;

// AI Trail/TP モード (v26 HOLD/CLOSE メッセージから更新)
// 0=NORMAL, 1=WIDE, 2=TIGHT
int      g_trailMode          = 0;
int      g_tpMode             = 0;
datetime g_trailModeUpdatedAt = 0;
datetime g_tpModeUpdatedAt    = 0;

//==========================================================================
//  セクション 4: OnInit / OnDeinit
//==========================================================================

int OnInit()
{
   g_trade.SetExpertMagicNumber(InpMagicNumber);
   g_trade.SetDeviationInPoints(InpEntrySlipPoints);

   if(!g_socket.connect(InpZmqUrl)) {
      Print("[LRR][INIT] FATAL: ZMQ connect failed -> ", InpZmqUrl);
      return INIT_FAILED;
   }
   Print("[LRR][INIT] ZMQ connected -> ", InpZmqUrl);

   if(InpHeartbeatEnabled) {
      g_hbConnected = g_hbSocket.connect(InpHeartbeatUrl);
      Print("[LRR][INIT] Heartbeat: ", g_hbConnected ? "OK" : "WARN: connect failed",
            " -> ", InpHeartbeatUrl);
   }

   EventSetMillisecondTimer(10);

   g_atrM5Handle = iATR(_Symbol, PERIOD_M5, 14);
   g_atrM1Handle = iATR(_Symbol, PERIOD_M1, 14);
   g_atrH1Handle = iATR(_Symbol, PERIOD_H1, 14);

   InitSpreadBuffer();

   g_dayStart       = iTime(_Symbol, PERIOD_D1, 0);
   g_dayStartEquity = AccountInfoDouble(ACCOUNT_EQUITY);
   g_haltEntries    = false;
   g_emgHaltActive  = false;

   g_dailyEntryCount      = 0;
   g_consecutiveLosses    = 0;
   g_dailyRiskUsedPct     = 0.0;
   g_weeklyConsecutiveLosses = 0;
   g_weekStart            = iTime(_Symbol, PERIOD_W1, 0);
   g_ec1FlashFiredToday   = false;
   g_ec2BlowoutFiredToday = false;
   g_ec3SlipStrikes       = 0;
   g_ec3SessionHalt       = false;
   g_lastEmgSystem        = "";
   g_posRegCount = 0;
   for(int i = 0; i < 10; i++) {
      g_posReg[i].ticket         = 0;
      g_posReg[i].tp1Hit         = false;
      g_posReg[i].tp2Hit         = false;
      g_posReg[i].stopDist       = 0.0;
      g_posReg[i].initialRDollar = 0.0;
   }

   LoadLegacyEmgState();

   Print("[LRR][INIT] v3.10 OK Symbol=", _Symbol,
         " Magic=", InpMagicNumber,
         " BaseRisk=", DoubleToString(InpBaseRisk,2), "%",
         " DailyCap=", DoubleToString(InpDailyLossCapPct,2), "%",
         " SpreadHardCap=", DoubleToString(InpSpreadHardCap,3),
         " MaxEntries/day=", InpMaxEntriesPerDay,
         " MaxConsecLoss=", InpMaxConsecLosses);

   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   if(g_atrM5Handle!=INVALID_HANDLE){IndicatorRelease(g_atrM5Handle);g_atrM5Handle=INVALID_HANDLE;}
   if(g_atrM1Handle!=INVALID_HANDLE){IndicatorRelease(g_atrM1Handle);g_atrM1Handle=INVALID_HANDLE;}
   if(g_atrH1Handle!=INVALID_HANDLE){IndicatorRelease(g_atrH1Handle);g_atrH1Handle=INVALID_HANDLE;}
   Print("[LRR][DEINIT] reason=", reason);
}

//==========================================================================
//  セクション 5: OnTimer / OnTick
//==========================================================================

void OnTimer()
{
   ResetDailyIfNeeded();
   double eq=0,dayEq=0,ml=0; string rsn="";
   if(ShouldFireLegacyEmg(eq,dayEq,ml,rsn)){FireLegacyEmg(rsn,eq,dayEq,ml);return;}
   MaybeSendHeartbeat();
   for(int i=0;i<50;i++){
      ZmqMsg msg;
      if(!g_socket.recv(msg,ZMQ_NOBLOCK)) break;
      string json=msg.getData();
      CJAVal obj;
      if(!obj.Deserialize(json)){g_zmqDeserFails++;continue;}
      ProcessZmqMsg(obj);
   }
}

void OnTick()
{
   UpdateSpreadBuffer();
   ResetDailyIfNeeded();
   if(IsHardCloseWindow()){
      if(CountMyPositions()>0){Print("[LRR][HARD_CLOSE] closing all.");CloseAll();}
      return;
   }
   double eq=0,dayEq=0,ml=0; string rsn="";
   if(ShouldFireLegacyEmg(eq,dayEq,ml,rsn)){FireLegacyEmg(rsn,eq,dayEq,ml);return;}
   RunEmergencySentinel();
   HandleLRRTpLadder();
   if(InpTrailingEnabled) HandleAllTrailing();
}

//==========================================================================
//  セクション 5b: OnTradeTransaction（連敗カウント更新・ポジション登録解除）
//
//  ■ ロジック:
//    ポジションクローズ約定(DEAL_ENTRY_OUT/INOUT)を検知し、
//    損益に応じて g_consecutiveLosses を更新する。
//    勝ちトレードはカウンターをゼロにリセット（LRR Strategist §4 準拠）。
//    g_consecutiveLosses >= InpMaxConsecLosses → g_haltEntries=true
//==========================================================================
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest     &req,
                        const MqlTradeResult      &res)
{
   if(trans.type!=TRADE_TRANSACTION_DEAL_ADD)return;
   if(trans.deal_type!=DEAL_TYPE_BUY&&trans.deal_type!=DEAL_TYPE_SELL)return;
   if(trans.deal_entry!=DEAL_ENTRY_OUT&&trans.deal_entry!=DEAL_ENTRY_INOUT)return;

   HistoryDealSelect(trans.deal);
   double profit=HistoryDealGetDouble(trans.deal,DEAL_PROFIT)
                +HistoryDealGetDouble(trans.deal,DEAL_SWAP)
                +HistoryDealGetDouble(trans.deal,DEAL_COMMISSION);
   long   magic =(long)HistoryDealGetInteger(trans.deal,DEAL_MAGIC);
   string sym   =HistoryDealGetString(trans.deal,DEAL_SYMBOL);
   if(magic!=InpMagicNumber||sym!=_Symbol)return;

   UnregisterPos(trans.position);

   if(profit<0.0){
      g_consecutiveLosses++;
      g_weeklyConsecutiveLosses++;  // [Phase2] 週次連敗カウンターインクリメント
      Print("[LRR][TRADE] Loss closed. profit=",DoubleToString(profit,2),
            " consecLosses=",g_consecutiveLosses,"/",InpMaxConsecLosses,
            " weeklyConsecLosses=",g_weeklyConsecutiveLosses,"/",InpMaxWeeklyConsecLosses);
      if(g_consecutiveLosses>=InpMaxConsecLosses){
         g_haltEntries=true;
         Print("[LRR][TRADE] *** CONSEC_LOSS_HALT *** losses=",g_consecutiveLosses,
               " >= limit=",InpMaxConsecLosses," -> haltEntries=true");
      }
      // [Phase2] 週次連敗上限到達時も停止
      if(InpMaxWeeklyConsecLosses>0 && g_weeklyConsecutiveLosses>=InpMaxWeeklyConsecLosses){
         g_haltEntries=true;
         Print("[LRR][TRADE] *** WEEKLY_CONSEC_HALT *** weeklyLosses=",g_weeklyConsecutiveLosses,
               " >= limit=",InpMaxWeeklyConsecLosses," -> haltEntries=true");
      }
   }else{
      if(g_consecutiveLosses>0)
         Print("[LRR][TRADE] Win/BreakEven -> reset consecLosses (",g_consecutiveLosses,"->0).",
               " weeklyConsecLosses remains=",g_weeklyConsecutiveLosses,
               " profit=",DoubleToString(profit,2));
      g_consecutiveLosses=0;
   }
}

//==========================================================================
//  セクション 5c: AI Mode ヘルパー
//
//  trail_mode / tp_mode 文字列 → int 変換。v26 の HOLD/CLOSE メッセージで
//  "trail_mode":"WIDE" / "TIGHT" / "NORMAL" として送られてくる。
//  InpAiModeDecaySec 秒後に自動リセットして古い判断が残り続けるのを防ぐ。
//==========================================================================

int ParseAiMode(string s)
{
   StringToUpper(s);
   if(s=="WIDE")  return 1;
   if(s=="TIGHT") return 2;
   return 0; // NORMAL (unknown も含む)
}

void MaybeDecayAiModes()
{
   if(InpAiModeDecaySec<=0)return;
   datetime now=TimeCurrent();
   if(g_trailMode!=0&&g_trailModeUpdatedAt>0&&
      (now-g_trailModeUpdatedAt)>=(datetime)InpAiModeDecaySec){
      Print("[LRR][AI_MODE] trail_mode decay NORMAL (was ",
            (g_trailMode==1?"WIDE":"TIGHT"),")");
      g_trailMode=0; g_trailModeUpdatedAt=0;
   }
   if(g_tpMode!=0&&g_tpModeUpdatedAt>0&&
      (now-g_tpModeUpdatedAt)>=(datetime)InpAiModeDecaySec){
      Print("[LRR][AI_MODE] tp_mode decay NORMAL (was ",
            (g_tpMode==1?"WIDE":"TIGHT"),")");
      g_tpMode=0; g_tpModeUpdatedAt=0;
   }
}

//==========================================================================
//  セクション 6: ATRヘルパー
//==========================================================================

double GetAtrM5(){
   if(g_atrM5Handle==INVALID_HANDLE)return 0.0;
   double b[1];return(CopyBuffer(g_atrM5Handle,0,0,1,b)>0)?b[0]:0.0;
}
double GetAtrM1(){
   if(g_atrM1Handle==INVALID_HANDLE)return 0.0;
   double b[1];return(CopyBuffer(g_atrM1Handle,0,0,1,b)>0)?b[0]:0.0;
}
double GetAtrH1(){
   if(g_atrH1Handle==INVALID_HANDLE)return 0.0;
   double b[1];return(CopyBuffer(g_atrH1Handle,0,0,1,b)>0)?b[0]:0.0;
}

//==========================================================================
//  セクション 7: SpreadMed リングバッファ（軽量中央値推定）
//
//  ■ アルゴリズム: Robbins-Monro 逐次中央値推定
//  ■ 更新式: med_new = med_old + lr * sign(x - med_old)
//
//  lr = 0.03 の選択根拠:
//    - 1/lr ≈ 33サンプルで新しい値に約80%追従（300tickバッファとの整合）
//    - O(1)更新でO(n log n)ソートを排除 → OnTickで安全
//    - 推定誤差: XAUUSD環境で真の中央値から±$0.002以内
//    - スパイク耐性: sign()のみ使用するため外れ値に引きずられない
//
//  g_spreadBuf[] はデバッグ/可視化用リングバッファ（任意参照）。
//  中央値計算本体はg_spreadMed変数のみで完結する。
//==========================================================================

void InitSpreadBuffer()
{
   int sz=MathMax(10,InpSpreadMedBufSize);
   ArrayResize(g_spreadBuf,sz);
   ArrayFill(g_spreadBuf,0,sz,0.0);
   g_spreadBufIdx=0; g_spreadBufFill=0; g_spreadInited=false;
   double ask=SymbolInfoDouble(_Symbol,SYMBOL_ASK);
   double bid=SymbolInfoDouble(_Symbol,SYMBOL_BID);
   g_spreadMed=(ask>bid&&ask>0.0)?(ask-bid):0.20;
   Print("[LRR][SPREAD] InitSpreadBuffer. initialMed=",DoubleToString(g_spreadMed,4)," bufSize=",sz);
}

void UpdateSpreadBuffer()
{
   double ask=SymbolInfoDouble(_Symbol,SYMBOL_ASK);
   double bid=SymbolInfoDouble(_Symbol,SYMBOL_BID);
   if(ask<=0.0||bid<=0.0||ask<=bid)return;
   double spread=ask-bid;
   static const double LR=0.03;
   if(!g_spreadInited){g_spreadMed=spread;g_spreadInited=true;}
   else{
      double d=spread-g_spreadMed;
      g_spreadMed+=LR*(d>0.0?1.0:(d<0.0?-1.0:0.0));
   }
   int sz=ArraySize(g_spreadBuf);
   g_spreadBuf[g_spreadBufIdx]=spread;
   g_spreadBufIdx=(g_spreadBufIdx+1)%sz;
   if(g_spreadBufFill<sz)g_spreadBufFill++;
}

double GetSpreadDollar()
{
   double ask=SymbolInfoDouble(_Symbol,SYMBOL_ASK);
   double bid=SymbolInfoDouble(_Symbol,SYMBOL_BID);
   return(ask>bid&&ask>0.0)?(ask-bid):0.0;
}

//==========================================================================
//  セクション 8: スプレッドエントリーゲート（3条件）
//==========================================================================

bool IsSpreadBlocked(string &reason)
{
   double sp=GetSpreadDollar(), atrM5=GetAtrM5();
   if(sp>InpSpreadHardCap){
      reason=StringFormat("[SPREAD_HARD] sp=%.4f > cap=%.4f",sp,InpSpreadHardCap);return true;}
   if(g_spreadMed>0.0&&sp>g_spreadMed*InpSpreadSpikeMult){
      reason=StringFormat("[SPREAD_SPIKE] sp=%.4f > med*%.1f=%.4f (med=%.4f)",
                          sp,InpSpreadSpikeMult,g_spreadMed*InpSpreadSpikeMult,g_spreadMed);return true;}
   if(atrM5>0.0&&sp>atrM5*InpSpreadATRMult){
      reason=StringFormat("[SPREAD_ATR] sp=%.4f > atr*%.2f=%.4f (atrM5=%.4f)",
                          sp,InpSpreadATRMult,atrM5*InpSpreadATRMult,atrM5);return true;}
   return false;
}

//==========================================================================
//  セクション 9: ATRベースSL計算（ハイブリッドMT5最終フィルタ）
//
//  設計思想:
//    PythonがSweep_ExtremeをLRR戦略ロジックで決定し送信。
//    MT5がリアルタイムスプレッド・ATRを使ってSL距離を最終確定する。
//    StopDistance = max(StructureSL距離, ATR Noise Floor距離)
//    → 狭すぎるSLをノイズで刈られることを防止する。
//==========================================================================

double CalcStructureStopDist(string action,double sweepExtreme,double price,double spread)
{
   double buf=MathMax(InpBufferMin,spread*InpBufferSpreadMult);
   double slPx=(action=="BUY")?sweepExtreme-buf:sweepExtreme+buf;
   return MathAbs(price-slPx);
}

double CalcATRStopDist(double atrM5)
{
   return MathMax(InpMinATRStop,MathMin(InpMaxATRStop,atrM5*InpKatr));
}

double CalcFinalStopDist(string action,double sweepExtreme,double price,double atrM5,double spread)
{
   double atrDist=CalcATRStopDist(atrM5);
   double finalDist; string method;
   if(sweepExtreme>0.0){
      double structDist=CalcStructureStopDist(action,sweepExtreme,price,spread);
      finalDist=MathMax(structDist,atrDist);
      method=StringFormat("max(struct=%.4f,atr=%.4f)",structDist,atrDist);
   }else{
      finalDist=atrDist;
      method=StringFormat("atr_only=%.4f(no sweep_extreme)",atrDist);
   }
   Print("[LRR][SL] action=",action," method=",method,
         " finalDist=",DoubleToString(finalDist,_Digits)," K_atr=",DoubleToString(InpKatr,2));
   return finalDist;
}

//==========================================================================
//  セクション 10: ボラティリティレジーム判定
//==========================================================================

ENUM_VOL_REGIME GetVolRegime(double atrM5,double atrH1)
{
   if(atrM5<=0.0||atrH1<=0.0)return VOL_NORMAL;
   double r=atrM5/atrH1;
   if(r>=InpVolRatioHigh)return VOL_HIGH;
   if(r<InpVolRatioLow)  return VOL_LOW;
   return VOL_NORMAL;
}

//==========================================================================
//  セクション 11: セッションランク判定（サーバー時刻（EET）ベース）
//
//  LRR Strategist定義（サーバー時間 EET 固定値・夏冬共通）:
//    S級(×1.50): 15:20〜16:00 EET  (NYオープン)
//    A級(×1.00): 10:00〜11:30 EET  (ロンドン) | 15:00〜15:19 EET
//    B級(×0.70): 20:00〜20:59 EET  (アジア初動)
//    無効(禁止) : 05:00〜09:59 EET  (DeadZone)
//
//  ブローカーサーバー時間は夏冬で自動調整されるため、
//  EET固定値で判定すれば夏冬どちらでも正確に動作する。
//  （旧: JST基準 → JST_SERVER_OFFSET_HR による手動補正が必要だった）
//==========================================================================

ENUM_SESSION_RANK GetCurrentSessionRank()
{
   MqlDateTime dt;
   TimeToStruct(TimeCurrent(),dt);
   int serverHr =dt.hour;
   int serverMin=dt.min;
   if(serverHr>=5 &&serverHr<=9) return SESSION_INVALID;   // DeadZone  05:00-09:59 EET
   if(serverHr==15&&serverMin>=20)return SESSION_S;         // NYオープン 15:20-15:59 EET
   if(serverHr==16&&serverMin==0) return SESSION_S;         // NYオープン 16:00      EET
   if(serverHr==10)               return SESSION_A;         // ロンドン   10:00-10:59 EET
   if(serverHr==11&&serverMin<=30)return SESSION_A;         // ロンドン   11:00-11:30 EET
   if(serverHr==15&&serverMin<20) return SESSION_A;         // NY待機     15:00-15:19 EET
   if(serverHr==20)               return SESSION_B;         // アジア初動 20:00-20:59 EET
   return SESSION_A; // その他 → 保守的にA級
}

//==========================================================================
//  セクション 12: ニュースフィルター
//
//  Python(brain_bridge)がCPI/FOMC/NFP直前に news_block=true を
//  ZMQ経由で送信 → MT5側で30分間エントリーをブロック。
//  MT5単体での経済指標時刻自動取得が困難なためPython依存方式を採用。
//==========================================================================

void CheckNewsBlockExpiry()
{
   if(g_newsBlockActive&&TimeCurrent()>g_newsBlockUntil){
      g_newsBlockActive=false;
      Print("[LRR][NEWS] Block expired at ",TimeToString(TimeCurrent(),TIME_DATE|TIME_MINUTES));
   }
}

bool IsNewsBlocked()
{
   if(!InpNewsFilterEnabled)return false;
   return g_newsBlockActive;
}

//==========================================================================
//  セクション 13: リスク%計算（ボラ適応型）
//==========================================================================

double CalcRiskPct(ENUM_SESSION_RANK sessionRank,ENUM_VOL_REGIME volRegime,double spread)
{
   double sessionMult=sessionRank/100.0;
   if(sessionMult<=0.0){Print("[LRR][RISK] SESSION_INVALID -> riskPct=0");return 0.0;}

   double volMult=(volRegime==VOL_HIGH)?InpVolMultHigh:
                  (volRegime==VOL_LOW) ?InpVolMultLow:InpVolMultNormal;

   double spRef=(InpSpreadRefDollar>0.0)?InpSpreadRefDollar:0.30;
   double spreadMult=MathMax(0.50,MathMin(1.00,1.0-(spread/spRef)*0.25));

   double riskPct=InpBaseRisk*sessionMult*volMult*spreadMult;
   riskPct=MathMin(riskPct,InpMaxRiskPerTrade);

   double remaining=InpDailyLossCapPct-g_dailyRiskUsedPct;
   if(remaining<=0.0){
      Print("[LRR][RISK] Daily cap exhausted. dailyUsed=",DoubleToString(g_dailyRiskUsedPct,2),"%");
      return 0.0;
   }
   riskPct=MathMin(riskPct,remaining);

   string regStr=(volRegime==VOL_HIGH)?"HIGH":(volRegime==VOL_LOW)?"LOW":"NORMAL";
   string sessStr=(sessionRank==SESSION_S)?"S":(sessionRank==SESSION_A)?"A":"B";
   Print("[LRR][RISK] rank=",sessStr," vol=",regStr,
         " sMult=",DoubleToString(sessionMult,2),
         " vMult=",DoubleToString(volMult,2),
         " spMult=",DoubleToString(spreadMult,2),
         " -> riskPct=",DoubleToString(riskPct,4),"%",
         " dailyUsed=",DoubleToString(g_dailyRiskUsedPct,2),"%");
   return riskPct;
}

//==========================================================================
//  セクション 14: ロット計算
//  Lot = (Equity * RiskPct%) / StopValuePerLot
//  StopValuePerLot = (StopDist/_Point) * (TickValue/TickSize*_Point)
//==========================================================================

double CalcVolAdaptiveLot(double stopDist,double riskPct)
{
   if(stopDist<=0.0||riskPct<=0.0)return 0.0;
   double equity  =AccountInfoDouble(ACCOUNT_EQUITY);
   double riskAmt =equity*(riskPct/100.0);
   double tickVal =SymbolInfoDouble(_Symbol,SYMBOL_TRADE_TICK_VALUE);
   double tickSize=SymbolInfoDouble(_Symbol,SYMBOL_TRADE_TICK_SIZE);
   if(tickSize<=0.0||tickVal<=0.0||_Point<=0.0)return InpLotFallback;
   double valPerPt=tickVal/tickSize*_Point;
   double stopPts =stopDist/_Point;
   double stopVal =stopPts*valPerPt;
   if(stopVal<=0.0)return InpLotFallback;
   double lotRaw  =riskAmt/stopVal;
   double lot     =NormalizeVolume(lotRaw);
   lot=MathMin(lot,InpMaxAllowedLot);
   Print("[LRR][LOT] eq=",DoubleToString(equity,2),
         " riskPct=",DoubleToString(riskPct,4),"%",
         " riskAmt=",DoubleToString(riskAmt,2),
         " stopDist=",DoubleToString(stopDist,_Digits),
         " stopVal=",DoubleToString(stopVal,2),
         " lotRaw=",DoubleToString(lotRaw,4),
         " lot=",DoubleToString(lot,2));
   return lot;
}

double NormalizeVolume(double v)
{
   double step  =SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_STEP);
   double minVol=SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_MIN);
   double maxVol=SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_MAX);
   if(step<=0.0)return MathMax(minVol,InpLotFallback);
   double norm=MathFloor(v/step)*step;
   norm=MathMax(norm,minVol);
   if(maxVol>0.0)norm=MathMin(norm,maxVol);
   return norm;
}

//==========================================================================
//  セクション 15: ポジションレジストリ (Flash Cut / TP管理)
//==========================================================================

void RegisterPos(ulong ticket,datetime t,double entryPx,double stopDist,
                 string action,double riskDollar,bool isPyramid=false)
{
   for(int i=0;i<10;i++){
      if(g_posReg[i].ticket==ticket){
         g_posReg[i].entryTime=t;g_posReg[i].entryPrice=entryPx;
         g_posReg[i].stopDist=stopDist;g_posReg[i].action=action;
         g_posReg[i].initialRDollar=riskDollar;
         g_posReg[i].tp1Hit=false;g_posReg[i].tp2Hit=false;
         g_posReg[i].isPyramid=isPyramid;
         return;
      }
   }
   for(int i=0;i<10;i++){
      if(g_posReg[i].ticket==0){
         g_posReg[i].ticket=ticket;g_posReg[i].entryTime=t;
         g_posReg[i].entryPrice=entryPx;g_posReg[i].stopDist=stopDist;
         g_posReg[i].action=action;g_posReg[i].initialRDollar=riskDollar;
         g_posReg[i].tp1Hit=false;g_posReg[i].tp2Hit=false;
         g_posReg[i].isPyramid=isPyramid;  // [Phase4]
         if(i+1>g_posRegCount)g_posRegCount=i+1;
         Print("[LRR][REG] Registered ticket=",ticket,
               " entry=",DoubleToString(entryPx,_Digits),
               " stopDist=",DoubleToString(stopDist,_Digits),
               " riskDollar=",DoubleToString(riskDollar,2),
               " action=",action," isPyramid=",isPyramid);
         return;
      }
   }
   Print("[LRR][REG][WARN] Registry full. ticket=",ticket," NOT registered.");
}

void UnregisterPos(ulong ticket)
{
   for(int i=0;i<10;i++){
      if(g_posReg[i].ticket==ticket){
         Print("[LRR][REG] Unregistered ticket=",ticket,
               " tp1=",g_posReg[i].tp1Hit," tp2=",g_posReg[i].tp2Hit);
         g_posReg[i].ticket=0; return;
      }
   }
}

void SyncPosRegistry()
{
   CPositionInfo pos;
   for(int i=0;i<10;i++){
      if(g_posReg[i].ticket==0)continue;
      if(!pos.SelectByTicket(g_posReg[i].ticket))UnregisterPos(g_posReg[i].ticket);
   }
}

int FindPosRegIdx(ulong ticket)
{
   for(int i=0;i<10;i++)if(g_posReg[i].ticket==ticket)return i;
   return -1;
}

//==========================================================================
//  セクション 16: 3系統 Emergency Sentinel
//==========================================================================

void RunEmergencySentinel()
{
   SyncPosRegistry();
   if(InpFlashCutEnabled) EmergencyCut_Flash();
   if(InpBlowoutEnabled)  EmergencyCut_Blowout();
}

//--------------------------------------------------------------------------
//  System 1: Flash Move Cut
//
//  目的: エントリー直後の異常急騰/急落（機関/HFTトラップ誤認）を防ぐ
//  発動条件 (すべてを満たすこと – "普通の負け"を誤検出しない設計):
//    ① adverse > ATR_1m * ShockMult
//    ② spread > SpreadMed * FlashSpreadSpike  (スプレッドスパイク確認)
//    ③ TimeCurrent - entryTime <= ShockWindowSec
//
//  ログ: g_lastEmgSystem = "EC_FLASH"
//        g_ec1FlashFiredToday = true
//--------------------------------------------------------------------------
void EmergencyCut_Flash()
{
   if(g_ec1FlashFiredToday)return;
   double atrM1=GetAtrM1(), spread=GetSpreadDollar();
   if(atrM1<=0.0)return;
   bool spSpike=(g_spreadMed>0.0&&spread>g_spreadMed*InpFlashSpreadSpike);

   CPositionInfo pos;
   for(int i=0;i<10;i++){
      if(g_posReg[i].ticket==0)continue;
      ulong    ticket   =g_posReg[i].ticket;
      datetime entryTime=g_posReg[i].entryTime;
      double   entryPx  =g_posReg[i].entryPrice;
      string   action   =g_posReg[i].action;
      if(!pos.SelectByTicket(ticket))continue;
      long elapsed=(long)(TimeCurrent()-entryTime);
      if(elapsed>InpShockWindowSec)continue;
      double bid=SymbolInfoDouble(_Symbol,SYMBOL_BID);
      double ask=SymbolInfoDouble(_Symbol,SYMBOL_ASK);
      double adverse=(action=="BUY")?(entryPx-bid):(ask-entryPx);
      if(adverse<=0.0)continue;
      if(!((adverse>atrM1*InpShockMult)&&spSpike))continue;

      g_ec1FlashFiredToday=true; g_emgHaltActive=true;
      g_haltEntries=true; g_lastEmgSystem="EC_FLASH";

      Print("[LRR][EC_FLASH] *** FIRED *** ticket=",ticket,
            " action=",action,
            " adverse=",DoubleToString(adverse,_Digits),
            " threshold(atrM1*ShockMult)=",DoubleToString(atrM1*InpShockMult,_Digits),
            " atrM1=",DoubleToString(atrM1,_Digits),
            " spread=",DoubleToString(spread,4),
            " spSpike=YES",
            " elapsed=",elapsed,"s");

      if(!InpEmergencyLogOnly){CloseAll();Print("[LRR][EC_FLASH] CloseAll executed.");}
      else{Print("[LRR][EC_FLASH][LOG_ONLY] CloseAll skipped.");}
      return;
   }
}

//--------------------------------------------------------------------------
//  System 2: Spread Blowout Cut（段階式）
//
//  目的: 保有中スプレッド異常拡大による損失悪化を段階的に制限
//  段階1(部分決済): spread>blowThresh AND FloatingR>FloatingRPartial
//  段階2(全決済)  : spread>critThresh OR  FloatingR>FloatingRFull
//
//  ログ: g_lastEmgSystem = "EC_BLOWOUT_PARTIAL" / "EC_BLOWOUT_FULL"
//--------------------------------------------------------------------------
void EmergencyCut_Blowout()
{
   double spread=GetSpreadDollar(), med=g_spreadMed;
   if(med<=0.0)return;
   double blowThresh=MathMax(InpSpreadHardCap,med*InpBlowMult);
   double critThresh=med*InpBlowCritMult;
   bool   isBlowout =(spread>blowThresh);
   bool   isCritical=(spread>critThresh);
   if(!isBlowout)return;

   CPositionInfo pos;
   for(int i=PositionsTotal()-1;i>=0;i--){
      if(!pos.SelectByIndex(i))continue;
      if(pos.Magic()!=InpMagicNumber||pos.Symbol()!=_Symbol)continue;
      ulong  ticket =pos.Ticket();
      double floating=pos.Profit()+pos.Swap()+pos.Commission();
      bool   isLoss=(floating<0.0);
      int    idx=FindPosRegIdx(ticket);
      double initR=(idx>=0)?g_posReg[idx].initialRDollar:0.0;
      double floatingR=(initR>0.0&&isLoss)?MathAbs(floating)/initR:0.0;

      if(isCritical||(isLoss&&floatingR>InpFloatingRFull)){
         g_ec2BlowoutFiredToday=true; g_haltEntries=true;
         g_lastEmgSystem="EC_BLOWOUT_FULL";
         Print("[LRR][EC_BLOWOUT] *** FULL CLOSE *** ticket=",ticket,
               " spread=",DoubleToString(spread,4),
               " critThresh=",DoubleToString(critThresh,4),
               " isCritical=",(isCritical?"YES":"NO"),
               " floatingR=",DoubleToString(floatingR,3),
               " threshold=",DoubleToString(InpFloatingRFull,2),
               " floating=",DoubleToString(floating,2));
         if(!InpEmergencyLogOnly){
            CTrade t;t.SetExpertMagicNumber(InpMagicNumber);
            bool ok=t.PositionClose(ticket);
            Print("[LRR][EC_BLOWOUT] PositionClose=",(ok?"OK":"FAILED")," ticket=",ticket);
         }else{Print("[LRR][EC_BLOWOUT][LOG_ONLY] Full close skipped.");}
         continue;
      }
      if(isLoss&&floatingR>InpFloatingRPartial){
         g_haltEntries=true; g_ec2BlowoutFiredToday=true;  // [Phase2-Fix] 部分決済後もエントリー停止
         g_lastEmgSystem="EC_BLOWOUT_PARTIAL";
         Print("[LRR][EC_BLOWOUT] *** PARTIAL CLOSE ",DoubleToString(InpPartialClosePct,0),"% ***",
               " ticket=",ticket,
               " spread=",DoubleToString(spread,4),
               " blowThresh=",DoubleToString(blowThresh,4),
               " floatingR=",DoubleToString(floatingR,3),
               " threshold=",DoubleToString(InpFloatingRPartial,2));
         if(!InpEmergencyLogOnly)PartialCloseByTicket(ticket,InpPartialClosePct);
         else Print("[LRR][EC_BLOWOUT][LOG_ONLY] Partial close skipped.");
      }
   }
}

//--------------------------------------------------------------------------
//  System 3: Slippage Quality Cut
//
//  目的: 約定品質の継続的劣化でセッション停止
//  呼び出し: ExecuteOrder()の直後に actualPx / expectedPx を渡す
//  正常約定(-cap以内)はストライクを1減らして回復性を確保。
//
//  ログ: g_lastEmgSystem = "EC_QUALITY"
//        g_ec3SessionHalt = true
//--------------------------------------------------------------------------
void EmergencyCut_Quality(double actualPx,double expectedPx)
{
   if(!InpQualityCutEnabled||g_ec3SessionHalt)return;
   double absSlip=MathAbs(actualPx-expectedPx);
   if(absSlip>InpSlippageCap){
      g_ec3SlipStrikes++;
      Print("[LRR][EC_QUALITY] Strike #",g_ec3SlipStrikes,
            " actual=",DoubleToString(actualPx,_Digits),
            " expected=",DoubleToString(expectedPx,_Digits),
            " absSlip=",DoubleToString(absSlip,_Digits),
            " cap=",DoubleToString(InpSlippageCap,_Digits),
            " strikes=",g_ec3SlipStrikes,"/",InpSlippageStrikes);
      if(g_ec3SlipStrikes>=InpSlippageStrikes){
         g_ec3SessionHalt=true; g_haltEntries=true;
         g_lastEmgSystem="EC_QUALITY";
         Print("[LRR][EC_QUALITY] *** SESSION HALT *** strikes=",g_ec3SlipStrikes,
               " all new entries blocked.");
      }
   }else{
      if(g_ec3SlipStrikes>0){
         g_ec3SlipStrikes=MathMax(0,g_ec3SlipStrikes-1);
         Print("[LRR][EC_QUALITY] Slip OK -> strike decay to ",g_ec3SlipStrikes);
      }
   }
}

//==========================================================================
//  セクション 17: LRR TP段階決済
//  TP1(50%): profit >= SL距離*TP1AtrMult (最低 RR 1:2 相当)
//  TP2(30%): profit >= ATR_M5*TP2AtrMult  (残りの60%を一部決済)
//  TP3(20%): HandleAllTrailingに委任（TP3AtrMultまでトレーリング）
//
//  [AI Tuning] g_tpMode が WIDE/TIGHT の場合::
//    WIDE  (1): TP1/TP2閾値 × InpTpWideMultiplier  → 利益を引き伸ばす
//    TIGHT (2): TP1/TP2閾値 × InpTpTightMultiplier → 早めに利確
//==========================================================================

void HandleLRRTpLadder()
{
   MaybeDecayAiModes();
   double atrM5=GetAtrM5();
   if(atrM5<=0.0)return;
   double bid=SymbolInfoDouble(_Symbol,SYMBOL_BID);
   double ask=SymbolInfoDouble(_Symbol,SYMBOL_ASK);

   // tp_mode に応じた TP 閾値スケーリング
   double tpMult=1.0;
   if(g_tpMode==1) tpMult=InpTpWideMultiplier;
   else if(g_tpMode==2) tpMult=InpTpTightMultiplier;
   string tpStr=(g_tpMode==1?"WIDE":g_tpMode==2?"TIGHT":"NORMAL");

   CPositionInfo pos;
   for(int i=0;i<10;i++){
      if(g_posReg[i].ticket==0)continue;
      ulong ticket=g_posReg[i].ticket;
      if(!pos.SelectByTicket(ticket))continue;
      if(pos.Magic()!=InpMagicNumber||pos.Symbol()!=_Symbol)continue;

      string act =g_posReg[i].action;
      double cpx =(act=="BUY")?bid:ask;
      double profit=(act=="BUY")?(cpx-g_posReg[i].entryPrice):(g_posReg[i].entryPrice-cpx);

      // --- TP1 ---
      if(!g_posReg[i].tp1Hit){
         double tp1Dist=g_posReg[i].stopDist*InpTP1AtrMult*tpMult;
         if(profit>=tp1Dist&&tp1Dist>0.0){
            g_posReg[i].tp1Hit=true;
            Print("[LRR][TP1] HIT ticket=",ticket,
                  " profit=",DoubleToString(profit,_Digits),
                  " tp1Dist=",DoubleToString(tp1Dist,_Digits),
                  " tpMode=",tpStr," tpMult=",DoubleToString(tpMult,2),
                  " closing ",DoubleToString(InpTP1ClosePct,0),"%");
            if(!InpEmergencyLogOnly)PartialCloseByTicket(ticket,InpTP1ClosePct);
            continue;
         }
      }
      // --- TP2 (TP1後のみ) ---
      if(g_posReg[i].tp1Hit&&!g_posReg[i].tp2Hit){
         double tp2Dist=atrM5*InpTP2AtrMult*tpMult;
         if(profit>=tp2Dist&&tp2Dist>0.0){
            g_posReg[i].tp2Hit=true;
            Print("[LRR][TP2] HIT ticket=",ticket,
                  " profit=",DoubleToString(profit,_Digits),
                  " tp2Dist=",DoubleToString(tp2Dist,_Digits),
                  " tpMode=",tpStr," tpMult=",DoubleToString(tpMult,2),
                  " closing ",DoubleToString(InpTP2ClosePct,0),"% of remaining");
            if(!InpEmergencyLogOnly)PartialCloseByTicket(ticket,InpTP2ClosePct);
            // TP3の残り(~20%)はトレーリングへ
         }
      }
   }
}

//==========================================================================
//  セクション 18: トレーリングSL（TP3用残ポジション管理）
//
//  [AI Tuning] g_trailMode が WIDE/TIGHT の場合:
//    WIDE  (1): TrailStart × InpTrailWidStartMult, TrailDist × InpTrailWideDistMult
//               → 大きく引きつけてトレンドをロングラン
//    TIGHT (2): TrailStart × InpTrailTightStartMult, TrailDist × InpTrailTightDistMult
//               → 利益を素早くロック
//==========================================================================

void HandleAllTrailing()
{
   double atrM5=GetAtrM5();
   if(atrM5<=0.0)return;

   // trail_mode に応じたパラメータスケーリング
   double trailStartMult=1.0, trailDistMult=1.0;
   if(g_trailMode==1){trailStartMult=InpTrailWidStartMult; trailDistMult=InpTrailWideDistMult;}
   else if(g_trailMode==2){trailStartMult=InpTrailTightStartMult;trailDistMult=InpTrailTightDistMult;}
   string trailStr=(g_trailMode==1?"WIDE":g_trailMode==2?"TIGHT":"NORMAL");

   double trailStart=InpTrailStartAtrMult*trailStartMult;
   double trailDist =InpTrailDistAtrMult *trailDistMult;

   CPositionInfo pos;
   CTrade t;
   t.SetExpertMagicNumber(InpMagicNumber);
   t.SetDeviationInPoints(InpMgmtSlipPoints);

   for(int i=PositionsTotal()-1;i>=0;i--){
      if(!pos.SelectByIndex(i))continue;
      if(pos.Magic()!=InpMagicNumber||pos.Symbol()!=_Symbol)continue;
      ulong  ticket=pos.Ticket();
      double open  =pos.PriceOpen();
      double sl    =pos.StopLoss();
      bool   isBuy =(pos.PositionType()==POSITION_TYPE_BUY);
      double price =isBuy?SymbolInfoDouble(_Symbol,SYMBOL_BID):SymbolInfoDouble(_Symbol,SYMBOL_ASK);
      double profit=isBuy?(price-open):(open-price);
      if(profit<=trailStart*atrM5)continue;
      double targetSL=isBuy?(price-trailDist*atrM5):(price+trailDist*atrM5);
      bool shouldMod=isBuy?(targetSL>sl+InpTrailStepAtrMult*atrM5):
                          (sl==0.0||targetSL<sl-InpTrailStepAtrMult*atrM5);
      if(shouldMod){
         if(t.PositionModify(ticket,NormalizeDouble(targetSL,_Digits),pos.TakeProfit()))
            Print("[LRR][TRAIL] ticket=",ticket,
                  " newSL=",DoubleToString(targetSL,_Digits),
                  " profitR=",DoubleToString(profit/atrM5,2),
                  " trailMode=",trailStr,
                  " trailDist*=",DoubleToString(trailDistMult,2));
      }
   }
}

//==========================================================================
//  セクション 19: 注文実行
//==========================================================================

bool ExecuteOrder(string action,double lot,double price,double sl,double atrM5)
{
   if(lot<=0.0)return false;
   g_trade.SetDeviationInPoints(InpEntrySlipPoints);
   double tp=0.0;
   if(atrM5>0.0){
      tp=(action=="BUY")?price+InpTP3AtrMult*atrM5:price-InpTP3AtrMult*atrM5;
      tp=NormalizeDouble(tp,_Digits);
   }
   if(action=="BUY")  return g_trade.Buy(lot,_Symbol,price,NormalizeDouble(sl,_Digits),tp);
   if(action=="SELL") return g_trade.Sell(lot,_Symbol,price,NormalizeDouble(sl,_Digits),tp);
   return false;
}

//==========================================================================
//  セクション 20: ZMQシグナル処理（メインエントリーゲート）
//
//  ■ ハイブリッド処理フロー:
//    Python(Brain) → ZMQ signal → [MT5最終フィルタ] → ExecuteOrder
//
//  ■ MT5側リアルタイムフィルタ (Pythonの事前計算をGround-Truthで上書き):
//    1. 全停止フラグ (Emergency・Halt系)
//    2. スプレッドゲート (3条件)
//    3. セッションランク自動判定 (JST時刻)
//    4. ニュースフィルター (news_block flag from Python)
//    5. LRR日次ルール (最大3回/日・連敗停止)
//    6. ATRベースSL最終計算 (StructureSL vs ATR Noise Floor)
//    7. Vol適応型ロット計算 (Equity * RiskPct% / StopValuePerLot)
//    8. RRチェック (TP1距離/SL距離 >= 1.5)
//==========================================================================

void ProcessZmqMsg(CJAVal &obj)
{
   CheckNewsBlockExpiry();

   // ニュースブロック受信
   CJAVal *vNB=obj.HasKey("news_block",jtBOOL);
   if(vNB!=NULL&&vNB.ToBool()){
      g_newsBlockActive=true;
      g_newsBlockUntil=TimeCurrent()+30*60;
      Print("[LRR][NEWS] Block activated. until=",
            TimeToString(g_newsBlockUntil,TIME_DATE|TIME_MINUTES|TIME_SECONDS));
   }

   // シンボルフィルター
   CJAVal *vSym=obj.HasKey("symbol",jtSTR);
   if(vSym!=NULL){string sym=vSym.ToStr();if(sym!=""&&sym!=_Symbol)return;}

   string type=obj["type"].ToStr();

   if(type=="CLOSE"){
      // CLOSE: trail_mode/tp_mode を反映してから全決済
      string tmC=obj["trail_mode"].ToStr(); string tpC=obj["tp_mode"].ToStr();
      if(tmC!=""){g_trailMode=ParseAiMode(tmC);g_trailModeUpdatedAt=TimeCurrent();}
      if(tpC!=""){g_tpMode  =ParseAiMode(tpC) ;g_tpModeUpdatedAt  =TimeCurrent();}
      Print("[LRR][ZMQ] CLOSE recv. trail_mode=",(g_trailMode==1?"WIDE":g_trailMode==2?"TIGHT":"NORMAL"),
            " tp_mode=",(g_tpMode==1?"WIDE":g_tpMode==2?"TIGHT":"NORMAL"),
            " reason=",obj["reason"].ToStr());
      g_lastHoldAt=0;
      // tp/trail modeをCLOSE後はNORMALにリセット
      g_trailMode=0;g_tpMode=0;g_trailModeUpdatedAt=0;g_tpModeUpdatedAt=0;
      CloseAll();return;
   }
   if(type=="HOLD"){
      // HOLD: trail_mode/tp_mode を更新してポジションを継続保有
      string tmH=obj["trail_mode"].ToStr(); string tpH=obj["tp_mode"].ToStr();
      int newTrail=ParseAiMode(tmH); int newTp=ParseAiMode(tpH);
      bool changed=(newTrail!=g_trailMode||newTp!=g_tpMode);
      g_trailMode=newTrail; g_tpMode=newTp;
      g_trailModeUpdatedAt=TimeCurrent(); g_tpModeUpdatedAt=TimeCurrent();
      string trailStr=(g_trailMode==1?"WIDE":g_trailMode==2?"TIGHT":"NORMAL");
      string tpStr   =(g_tpMode   ==1?"WIDE":g_tpMode   ==2?"TIGHT":"NORMAL");
      Print("[LRR][ZMQ] HOLD: ",obj["reason"].ToStr(),
            " trail_mode=",trailStr," tp_mode=",tpStr,
            (changed?" [MODE_CHANGED]":" [MODE_SAME]"));
      g_lastHoldAt=TimeCurrent();return;
   }
   if(type!="ORDER"){Print("[LRR][ZMQ] Unknown type='",type,"' (ignored)");return;}

   // --- Priority Guard ---
   if(g_emgHaltActive){Print("[LRR][GATE] BLOCKED: emgHalt. system=",g_lastEmgSystem);return;}
   if(g_ec3SessionHalt){Print("[LRR][GATE] BLOCKED: EC_QUALITY halt. strikes=",g_ec3SlipStrikes);return;}
   if(TimeCurrent()<g_panicUntil){
      Print("[LRR][GATE] BLOCKED: panic cooldown until=",
            TimeToString(g_panicUntil,TIME_DATE|TIME_MINUTES|TIME_SECONDS));return;}
   if(IsHardCloseWindow())   {Print("[LRR][GATE] BLOCKED: hard close window.");return;}
   if(IsBeforeTradingStart()){Print("[LRR][GATE] BLOCKED: before trading start.");return;}
   if(g_haltEntries)         {Print("[LRR][GATE] BLOCKED: daily halt. system=",g_lastEmgSystem);return;}
   if(CountMyPositions()>=InpMaxPositions){Print("[LRR][GATE] BLOCKED: max positions=",InpMaxPositions);return;}

   // --- LRR日次ルール ---
   if(g_dailyEntryCount>=InpMaxEntriesPerDay){
      Print("[LRR][GATE] BLOCKED: maxEntries/day=",InpMaxEntriesPerDay,
            " today=",g_dailyEntryCount);return;}
   if(g_consecutiveLosses>=InpMaxConsecLosses){
      Print("[LRR][GATE] BLOCKED: consecLosses=",g_consecutiveLosses," >= limit=",InpMaxConsecLosses);
      g_haltEntries=true;return;}
   // [Phase2] 週次連敗ゲート
   if(InpMaxWeeklyConsecLosses>0 && g_weeklyConsecutiveLosses>=InpMaxWeeklyConsecLosses){
      Print("[LRR][GATE] BLOCKED: weeklyConsecLosses=",g_weeklyConsecutiveLosses," >= limit=",InpMaxWeeklyConsecLosses);
      g_haltEntries=true;return;}

   // --- ニュースフィルター ---
   if(IsNewsBlocked()){
      Print("[LRR][GATE] BLOCKED: news filter. until=",
            TimeToString(g_newsBlockUntil,TIME_DATE|TIME_MINUTES|TIME_SECONDS));return;}

   // --- スプレッドゲート ---
   string spreadReason="";
   if(IsSpreadBlocked(spreadReason)){Print("[LRR][GATE] BLOCKED: ",spreadReason);return;}

   // --- シグナル抽出 ---
   string action      =obj["action"].ToStr();
   double sweepExtreme=obj["sweep_extreme"].ToDbl();
   double pythonAtr   =obj["atr"].ToDbl();
   double aiConf      =obj["ai_confidence"].ToDbl();
   string aiReason    =obj["ai_reason"].ToStr();
   string sigReason   =obj["reason"].ToStr();
   string setupGrade  =obj["setup_grade"].ToStr();   // "A+", "A", "REJECT", or "" (legacy)

   StringToUpper(action);
   if(action!="BUY"&&action!="SELL"){
      Print("[LRR][GATE] BLOCKED: invalid action='",action,"'");return;}

   // --- setup_grade ゲート (A+/A only; REJECT = block; "" = legacy pass-through) ---
   if(setupGrade=="REJECT"){
      Print("[LRR][GATE] BLOCKED: setup_grade=REJECT. ai_reason=",aiReason);return;}
   if(setupGrade!="" && setupGrade!="A+" && setupGrade!="A" && setupGrade!="B" && setupGrade!="C"){
      Print("[LRR][GATE] BLOCKED: setup_grade unknown='",setupGrade,"'. Require A+/A/B/C.");return;}

   // --- セッションランク ---
   ENUM_SESSION_RANK sessionRank=GetCurrentSessionRank();
   if(sessionRank==SESSION_INVALID){
      Print("[LRR][GATE] BLOCKED: SESSION_INVALID (DeadZone 12:00-16:59 JST)");return;}

   // --- ATR取得 ---
   double atrM5=GetAtrM5();
   if(atrM5<=0.0)atrM5=pythonAtr;
   if(atrM5<=0.0){Print("[LRR][GATE] BLOCKED: ATR unavailable.");return;}
   double atrH1=GetAtrH1(), spread=GetSpreadDollar();

   // --- [HYBRID] MT5最終: ATRベースSL ---
   double price=(action=="BUY")?SymbolInfoDouble(_Symbol,SYMBOL_ASK):SymbolInfoDouble(_Symbol,SYMBOL_BID);
   double stopDist=CalcFinalStopDist(action,sweepExtreme,price,atrM5,spread);
   double sl=(action=="BUY")?NormalizeDouble(price-stopDist,_Digits):NormalizeDouble(price+stopDist,_Digits);

   // --- [HYBRID] MT5最終: Vol適応型ロット ---
   ENUM_VOL_REGIME volRegime=GetVolRegime(atrM5,atrH1);
   double riskPct=CalcRiskPct(sessionRank,volRegime,spread);
   if(riskPct<=0.0){
      Print("[LRR][GATE] BLOCKED: riskPct=0. dailyUsed=",DoubleToString(g_dailyRiskUsedPct,2),"%");return;}

   double lot=CalcVolAdaptiveLot(stopDist,riskPct);
   double pyMult=obj["multiplier"].ToDbl();
   if(pyMult>0.0&&pyMult!=1.0){
      double adj=NormalizeVolume(lot*MathMax(0.5,MathMin(2.0,pyMult)));
      Print("[LRR][LOT] PyMult=",DoubleToString(pyMult,2)," ",DoubleToString(lot,2),"->",DoubleToString(adj,2));
      lot=adj;
   }
   // setup_grade lot adjustment: A+ = full lot; A = 75%; B = 50%; "" = legacy pass-through
   if(setupGrade=="A"){
      double adjGradeA=NormalizeVolume(lot*0.75);
      Print("[LRR][GRADE] A grade lot adj: ",DoubleToString(lot,2),"->",DoubleToString(adjGradeA,2));
      lot=adjGradeA;
   }
   if(setupGrade=="B"){
      double adjGradeB=NormalizeVolume(lot*0.50);
      Print("[LRR][GRADE] B grade lot adj: ",DoubleToString(lot,2),"->",DoubleToString(adjGradeB,2));
      lot=adjGradeB;
   }
   // [Phase2] Grade C: Lot×0.25 (低リスク頻度拡張枠)
   if(setupGrade=="C"){
      double adjGradeC=NormalizeVolume(lot*0.25);
      Print("[LRR][GRADE] C grade lot adj: ",DoubleToString(lot,2),"->",DoubleToString(adjGradeC,2));
      lot=adjGradeC;
   }
   lot=MathMin(lot,InpMaxAllowedLot);
   if(lot<=0.0){Print("[LRR][GATE] BLOCKED: lot<=0.");return;}

   // --- RRチェック (TP1/SL >= 1.5) [Phase1-Fix: tpMultを展開して実实ff RRを正確に判定] ---
   double tpMultRR=1.0;
   if(g_tpMode==1) tpMultRR=InpTpWideMultiplier;
   else if(g_tpMode==2) tpMultRR=InpTpTightMultiplier;
   double tp1Dist=stopDist*InpTP1AtrMult*tpMultRR;
   double rr=(stopDist>0.0)?(tp1Dist/stopDist):0.0;
   if(rr<1.5){
      Print("[LRR][GATE] BLOCKED: RR=",DoubleToString(rr,2)," < 1.5."
            " tp1Dist=",DoubleToString(tp1Dist,_Digits),
            " stopDist=",DoubleToString(stopDist,_Digits),
            " tpMult=",DoubleToString(tpMultRR,2));return;}

   // [Phase4] ピラミッド防御: 既存ピラミッドポジションがあれば追撃は1回のみ
   bool isPyramidOrder = false;
   CJAVal *vPyramid=obj.HasKey("pyramid",jtBOOL);
   if(vPyramid!=NULL && vPyramid.ToBool()) isPyramidOrder=true;
   if(isPyramidOrder){
      for(int pi=0;pi<10;pi++){
         if(g_posReg[pi].ticket!=0 && g_posReg[pi].isPyramid){
            Print("[LRR][GATE] BLOCKED: pyramid already exists (1回のみ制限). ticket=",g_posReg[pi].ticket);
            return;
         }
      }
   }

   // --- 注文実行 ---
   double expectedPx=price;
   bool ok=ExecuteOrder(action,lot,price,sl,atrM5);

   if(ok){
      g_dailyEntryCount++;
      g_dailyRiskUsedPct+=riskPct;
      double riskDollar=AccountInfoDouble(ACCOUNT_EQUITY)*(riskPct/100.0);
      // isPyramidOrder は上部のゲートセクションで宣言済み [Phase4]
      CPositionInfo pos;
      for(int k=PositionsTotal()-1;k>=0;k--){
         if(!pos.SelectByIndex(k))continue;
         if(pos.Magic()!=InpMagicNumber||pos.Symbol()!=_Symbol)continue;
         double actualPx=pos.PriceOpen();
         RegisterPos(pos.Ticket(),TimeCurrent(),actualPx,stopDist,action,riskDollar,isPyramidOrder);
         EmergencyCut_Quality(actualPx,expectedPx);
         break;
      }
      string regStr=(volRegime==VOL_HIGH)?"HIGH":(volRegime==VOL_LOW)?"LOW":"NORMAL";
      string sessStr=(sessionRank==SESSION_S)?"S":(sessionRank==SESSION_A)?"A":"B";
      Print("[LRR][ORDER] OK action=",action,
            " grade=",setupGrade,
            " lot=",DoubleToString(lot,2),
            " price=",DoubleToString(price,_Digits),
            " sl=",DoubleToString(sl,_Digits),
            " stopDist=",DoubleToString(stopDist,_Digits),
            " riskPct=",DoubleToString(riskPct,4),"%",
            " riskDollar=",DoubleToString(riskDollar,2),
            " vol=",regStr," sess=",sessStr,
            " RR=",DoubleToString(rr,2),
            " AIconf=",DoubleToString(aiConf,0),
            " entries_today=",g_dailyEntryCount,"/",InpMaxEntriesPerDay,
            " dailyRisk=",DoubleToString(g_dailyRiskUsedPct,3),"%",
            " AIreason=",aiReason," signal=",sigReason);
   }else{
      Print("[LRR][ORDER] FAILED action=",action,
            " retcode=",(int)g_trade.ResultRetcode(),
            " msg=",g_trade.ResultRetcodeDescription());
   }
}

//==========================================================================
//  セクション 21: Heartbeat（LRR対応拡張版）
//==========================================================================

void MaybeSendHeartbeat()
{
   if(!InpHeartbeatEnabled||!g_hbConnected)return;
   uint nowMs=GetTickCount();
   if(g_lastHbSentMs!=0&&(nowMs-g_lastHbSentMs)<(uint)MathMax(50,InpHeartbeatIntervalMs))return;
   g_lastHbSentMs=nowMs;

   long   login  =(long)AccountInfoInteger(ACCOUNT_LOGIN);
   double equity =AccountInfoDouble(ACCOUNT_EQUITY);
   double balance=AccountInfoDouble(ACCOUNT_BALANCE);
   double atrM5  =GetAtrM5(), atrH1=GetAtrH1();
   ENUM_VOL_REGIME  vr=GetVolRegime(atrM5,atrH1);
   ENUM_SESSION_RANK sr=GetCurrentSessionRank();

   string json=StringFormat(
      "{"type":"HEARTBEAT","
      ""ts":%I64d,"server_ts":%I64d,"gmt_ts":%I64d,"
      ""symbol":"%s","login":%I64d,"
      ""equity":%.2f,"balance":%.2f,"positions":%d,"
      ""halt":%s,"magic":%d,"
      ""vol_regime":"%s","session_rank":"%s","
      ""spread_med":%.4f,"spread_now":%.4f,"
      ""atr_m5":%.4f,"atr_h1":%.4f,"atr_m1":%.4f,"
      ""daily_risk_used_pct":%.3f,"daily_entries":%d,"
      ""consec_losses":%d,"
      ""emg_system":"%s","
      ""ec1_flash":%s,"ec2_blowout":%s,"
      ""ec3_halt":%s,"ec3_strikes":%d,"
      ""news_block":%s,"
      ""zmq_deser_fails":%I64d,"hb_send_fails":%I64d}",
      (long)TimeCurrent(),(long)TimeTradeServer(),(long)TimeGMT(),
      _Symbol,login,equity,balance,CountMyPositions(),
      (g_haltEntries?"true":"false"),InpMagicNumber,
      (vr==VOL_HIGH?"HIGH":vr==VOL_LOW?"LOW":"NORMAL"),
      (sr==SESSION_S?"S":sr==SESSION_A?"A":sr==SESSION_B?"B":"INVALID"),
      g_spreadMed,GetSpreadDollar(),
      atrM5,atrH1,GetAtrM1(),
      g_dailyRiskUsedPct,g_dailyEntryCount,
      g_consecutiveLosses,g_lastEmgSystem,
      (g_ec1FlashFiredToday?"true":"false"),
      (g_ec2BlowoutFiredToday?"true":"false"),
      (g_ec3SessionHalt?"true":"false"),g_ec3SlipStrikes,
      (g_newsBlockActive?"true":"false"),
      (long)g_zmqDeserFails,(long)g_hbSendFails
   );

   if(!g_hbSocket.send(json)){
      g_hbSendFails++;
      static uint s_warnMs=0;
      if(s_warnMs==0||(nowMs-s_warnMs)>5000){
         s_warnMs=nowMs;Print("[LRR][WARN] HB send failed -> ",InpHeartbeatUrl);}
   }
}

//==========================================================================
//  セクション 22: 日次リセット & 日次ガード
//==========================================================================

void ResetDailyIfNeeded()
{
   datetime today=iTime(_Symbol,PERIOD_D1,0);
   if(g_dayStart==today){
      if(!g_haltEntries&&g_dayStartEquity>0.0){
         double eq=AccountInfoDouble(ACCOUNT_EQUITY);
         if(eq<g_dayStartEquity*(1.0-InpDailyLossCapPct/100.0)){
            g_haltEntries=true;
            Print("[LRR][DAILY_CAP] Hit. eq=",DoubleToString(eq,2),
                  " dayStart=",DoubleToString(g_dayStartEquity,2),
                  " cap=",DoubleToString(InpDailyLossCapPct,2),"%");
            CloseAll();
         }
      }
      return;
   }
   Print("[LRR][DAILY_RESET] New day. Resetting all daily counters.");
   g_dayStart=today; g_dayStartEquity=AccountInfoDouble(ACCOUNT_EQUITY);
   g_haltEntries=false; g_emgHaltActive=false;
   g_emergencyFiredToday=false; g_emergencyFiredAt=0; g_emergencyFiredDate=0;
   g_ec1FlashFiredToday=false; g_ec2BlowoutFiredToday=false;
   g_ec3SlipStrikes=0; g_ec3SessionHalt=false; g_lastEmgSystem="";
   g_dailyEntryCount=0; g_consecutiveLosses=0; g_dailyRiskUsedPct=0.0;
   g_newsBlockActive=false;
   // [Phase2] 金曜日の夜に週次連敗カウンターをリセット
   datetime weekNow=iTime(_Symbol,PERIOD_W1,0);
   if(weekNow!=g_weekStart){
      Print("[LRR][WEEKLY_RESET] New week -> weeklyConsecLosses=",g_weeklyConsecutiveLosses,"->0");
      g_weeklyConsecutiveLosses=0;
      g_weekStart=weekNow;
   }
   SaveLegacyEmgState();
   Print("[LRR][DAILY_RESET] OK. dayStartEquity=",DoubleToString(g_dayStartEquity,2));
}

//==========================================================================
//  セクション 23: Legacy Emergency Cut（マージンガード継承）
//
//  g_emergencyFiredToday / g_emergencyFiredAt / g_emergencyFiredDate を
//  v2.7から明示的に継承し、3系統とはg_lastEmgSystemで区別する。
//  発動ログ: "[LRR][LEGACY_EMG] *** FIRED *** reason=EQUITY_DD|MARGIN_LEVEL ..."
//==========================================================================

bool ShouldFireLegacyEmg(double &eq,double &dayEq,double &marginLvl,string &reason)
{
   eq=AccountInfoDouble(ACCOUNT_EQUITY);
   dayEq=g_dayStartEquity;
   marginLvl=AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   reason="";
   if(!InpLegacyEmgEnabled)return false;
   datetime today=iTime(_Symbol,PERIOD_D1,0);
   if(g_emergencyFiredDate==today)return false;
   if(dayEq>0.0&&eq<=dayEq*(1.0-InpEmergencyLossPct/100.0)){reason="EQUITY_DD";return true;}
   if(PositionsTotal()>0&&marginLvl>0.0&&marginLvl<=InpEmergencyMarginLvl){reason="MARGIN_LEVEL";return true;}
   return false;
}

void FireLegacyEmg(const string reason,double eq,double dayEq,double marginLvl)
{
   g_emergencyFiredToday=true;
   g_emergencyFiredAt=TimeCurrent();
   g_emergencyFiredDate=iTime(_Symbol,PERIOD_D1,0);
   g_haltEntries=true; g_emgHaltActive=true;
   g_lastEmgSystem="LEGACY_"+reason;
   datetime until=TimeCurrent()+(datetime)(60*30);
   if(g_panicUntil<until)g_panicUntil=until;
   SaveLegacyEmgState();
   Print("[LRR][LEGACY_EMG] *** FIRED *** reason=",reason,
         " eq=",DoubleToString(eq,2),
         " dayEq=",DoubleToString(dayEq,2),
         " marginLvl=",DoubleToString(marginLvl,1),
         " haltUntil=",TimeToString(g_panicUntil,TIME_DATE|TIME_MINUTES|TIME_SECONDS));
   if(!InpEmergencyLogOnly){
      int b=CountMyPositions();CloseAll();int a=CountMyPositions();
      Print("[LRR][LEGACY_EMG] CloseAll done. pos: ",b,"->",a);
   }else{Print("[LRR][LEGACY_EMG][LOG_ONLY] CloseAll skipped.");}
}

string LegacyEmgGVName()
{
   return StringFormat("%s:%I64d:%s:%d",
      InpEmgStateKeyPrefix,(long)AccountInfoInteger(ACCOUNT_LOGIN),_Symbol,InpMagicNumber);
}
void LoadLegacyEmgState()
{
   if(!InpPersistEmgState)return;
   string name=LegacyEmgGVName();
   if(!GlobalVariableCheck(name))return;
   double v=GlobalVariableGet(name);
   if(v<=0.0)return;
   g_emergencyFiredDate=(datetime)(long)v;
   datetime today=iTime(_Symbol,PERIOD_D1,0);
   if(g_emergencyFiredDate==today){
      g_emergencyFiredToday=true;g_haltEntries=true;
      Print("[LRR][LEGACY_EMG] Persisted state: firedDate=",
            TimeToString(g_emergencyFiredDate,TIME_DATE));
   }
}
void SaveLegacyEmgState()
{
   if(!InpPersistEmgState)return;
   GlobalVariableSet(LegacyEmgGVName(),(double)(long)g_emergencyFiredDate);
}

//==========================================================================
//  セクション 24: ユーティリティ
//==========================================================================

bool IsHardCloseWindow()
{
   MqlDateTime dt;TimeToStruct(TimeCurrent(),dt);
   return(dt.hour==InpHardCloseHour&&dt.min>=InpHardCloseMinute);
}
bool IsBeforeTradingStart()
{
   MqlDateTime dt;TimeToStruct(TimeCurrent(),dt);
   return(dt.hour<InpTradingStartHour);
}
int CountMyPositions()
{
   CPositionInfo p;int cnt=0;
   for(int i=PositionsTotal()-1;i>=0;i--)
      if(p.SelectByIndex(i)&&p.Magic()==InpMagicNumber&&p.Symbol()==_Symbol)cnt++;
   return cnt;
}
void CloseAll()
{
   CTrade ct;ct.SetExpertMagicNumber(InpMagicNumber);ct.SetDeviationInPoints(InpMgmtSlipPoints);
   CPositionInfo p;
   for(int i=PositionsTotal()-1;i>=0;i--){
      if(!p.SelectByIndex(i))continue;
      if(p.Magic()!=InpMagicNumber||p.Symbol()!=_Symbol)continue;
      if(!ct.PositionClose(p.Ticket()))
         Print("[LRR][CLOSE] Failed ticket=",p.Ticket()," err=",GetLastError());
   }
}
bool PartialCloseByTicket(ulong ticket,double pct)
{
   CPositionInfo pos;
   if(!pos.SelectByTicket(ticket))return false;
   double vol    =pos.Volume();
   double closeVol=NormalizeVolume(vol*(pct/100.0));
   double minVol =SymbolInfoDouble(_Symbol,SYMBOL_VOLUME_MIN);
   CTrade ct;ct.SetExpertMagicNumber(InpMagicNumber);ct.SetDeviationInPoints(InpMgmtSlipPoints);
   if(closeVol<minVol){
      Print("[LRR][PARTIAL] closeVol<minVol -> full close ticket=",ticket);
      return ct.PositionClose(ticket);
   }
   bool ok=ct.PositionClosePartial(ticket,closeVol);
   Print("[LRR][PARTIAL] ticket=",ticket," pct=",DoubleToString(pct,0),"%",
         " vol=",DoubleToString(vol,2)," close=",DoubleToString(closeVol,2),
         " result=",(ok?"OK":"FAILED"));
   return ok;
}
//+------------------------------------------------------------------+
//  End of fxChartAI_ZmqMuscle_LRR.mq5  v3.10
//+------------------------------------------------------------------+
