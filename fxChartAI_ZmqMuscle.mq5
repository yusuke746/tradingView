//+------------------------------------------------------------------+
//|                                     FxChartAI OpenEA v2.7 Muscle |
//+------------------------------------------------------------------+
#property version   "2.70"
#property description "ZeroMQ Muscle - Expectancy Max Optimized"

#include <Zmq/Zmq.mqh>
#include <Trade/Trade.mqh>
#include <Trade/SymbolInfo.mqh>
#include <Trade/PositionInfo.mqh>
#include <JAson.mqh>

// Some ZeroMQ MQL5 wrappers expose ZMQ_DONTWAIT instead of ZMQ_NOBLOCK
#ifndef ZMQ_NOBLOCK
   #ifdef ZMQ_DONTWAIT
      #define ZMQ_NOBLOCK ZMQ_DONTWAIT
   #else
      #define ZMQ_NOBLOCK 1
   #endif
#endif

// --- パラメータ ---
input string ZmqUrl             = "tcp://localhost:5555";
// --- ハートビート (EA -> Python) ---
input bool   EnableHeartbeat    = true;
input string HeartbeatUrl       = "tcp://localhost:5556";
input int    HeartbeatIntervalMs = 1000;

input double RiskPercent        = 2.0;
input double MaxAllowedLot      = 1.0;
input int    MagicNumber        = 20250110;

// --- 約定ハードニング (既定は従来挙動) ---
input bool   RejectAbnormalMultiplier = false; // false: クランプ / true: 範囲外を拒否
input double MultiplierMin            = 0.5;
input double MultiplierMax            = 2.0;

input bool   AllowLotFallbackOnCalcFail   = true;   // true: 計算失敗時は旧仕様(0.01)フォールバック
input bool   AllowLotFallbackOnZeroProfit = true;   // true: 利益0時は旧仕様(0.01)フォールバック
input double LotFallbackValue            = 0.01;

input bool   UseDynamicSlippage       = false; // false: 固定ポイントのまま
input int    EntrySlippagePoints      = 20;
input int    MgmtSlippagePoints       = 80;
input double SlippageAtrMult          = 0.0;  // 0.0: 実質固定
input int    SlippageMinPoints        = 10;
input int    SlippageMaxPoints        = 200;

input bool   PersistEmergencyState    = false; // false: 当日ガードをメモリ内のみで保持
input string EmergencyStateKeyPrefix  = "FXAI_EMERGENCY_FIRED";

input bool   LogZmqDeserializeFailures = false;
input int    ZmqDeserializeWarnEvery   = 50;

// --- 安全系 (v2.6思想: 期待値を落とす誤発注を抑止) ---
input int    MaxPositions       = 5;        // 最大ポジション数（このEAのMagic+Symbol）
input bool   CloseAllOnDailyLoss = true;    // 日次損失限界で全決済するか
input double DailyLossLimitPct  = 12.0;      // 日次損失限界(%)
// 日次ロック中でも検証目的で新規エントリを許可
input bool   AllowTradingDuringDailyLossHalt = false;

// --- パニッククローズ (生存性強化: 日次DD到達時のみ判定) ---
input bool   UsePanicCloseOnDailyLoss = true;   // 日次DD時の全決済を条件付きにする
input bool   PanicLogOnly             = false;   // true: 全決済せずログのみ
input int    PanicSpreadPoints        = 200;    // パニック判定スプレッド(ポイント)
input double PanicRangeAtrMult        = 3.5;    // M5レンジ >= ATR * 倍率
input int    PanicCooldownMin         = 30;     // パニック後の新規停止継続時間（分）

// --- 非常停止 (口座全体の保護) ---
input bool   EnableEmergencyCut   = true;   // 非常停止を有効化
input bool   EmergencyLogOnly     = false;   // true: ログのみ（CloseAllしない）/ false: CloseAllする
input double EmergencyLossPct     = 4.0;    // 当日開始EquityからのDD(%)
input double EmergencyMarginLevel = 250.0;  // 維持率(%)を下回ったら発動

// --- 時間フィルタ (任意) ---
input bool   UseTimeFilter      = false;
input int    StartHour          = 9;
input int    EndHour            = 23;
// --- サーバー時間連動の強制クローズ/取引拒否 ---
input int    HardCloseHour      = 23; // サーバー時間23時（夏冬共通の閉場1時間前）
input int    HardCloseMinute    = 55; // 55分になったら強制決済
input int    TradingStartHour   = 1;  // サーバー時間1時（スプレッド安定後）まで取引拒否

// --- トレーリング (自律動作) ---
input bool   EnableTrailing     = true;
input double TrailingStartATR   = 1.4;
input double TrailingDistATR    = 2.2;
input double TrailingStepATR    = 0.6;
// --- TPトレーリング (HOLD時のみ) ---
input bool   EnableTpTrailingOnHold = true;
input int    HoldTpWindowSec        = 60;  // HOLDを受け取ってから何秒TP更新を有効にするか
input double TpTrailingStartATR     = 2.5;
input double TpTrailingDistATR      = 2.5; // NORMAL基準
input double TpTrailingStepATR      = 0.5;
// --- 部分利確設定 (新規) ---
input bool EnablePartialClose = true;       // 部分利確を有効にする
input double PartialCloseATR = 2.2;         // 何倍のATRで部分利確するか
input double PartialClosePercent = 35.0;    // ロットの何%を決済するか

// --- 動的トレイリング用変数 (内部利用) ---
// AIから受け取ったモードを保持 (0=NORMAL, 1=WIDE, 2=TIGHT)
int g_ai_trail_mode = 0;
datetime g_trail_mode_updated_at = 0; // デバッグ用: 最終更新時刻

// TP動的調整用（TRAIL_MODEと独立）
int g_tp_mode = 0;  // 0=NORMAL, 1=WIDE, 2=TIGHT
datetime g_tp_mode_updated_at = 0; // デバッグ用: 最終更新時刻


// --- Global ---
Context context;
Socket socket(context, ZMQ_PULL);
Socket hb_socket(context, ZMQ_PUSH);
CTrade m_trade;

bool g_hb_connected = false;
uint g_lastHbSentMs = 0;
ulong g_hbSendFails = 0;

datetime g_dayStartTime = 0;
double   g_dayStartEquity = 0.0;
bool     g_haltNewEntriesToday = false;
bool     g_emergencyHaltActive = false; // Emergency Cut時はtrue（検証モード無視）

datetime g_panicUntil = 0;

bool     g_emergencyFiredToday = false; // 同一日に多重発動させない
datetime g_emergencyFiredAt    = 0;     // 発動時刻を記録（ログ用）
datetime g_emergencyFiredDate  = 0;     // 日付(=D1の始値時刻)で多重発動を防ぐ

ulong    g_zmqDeserializeFails = 0;
datetime g_lastHoldAt = 0;

// --- Cached indicator handles (performance / stability) ---
int g_atrHandleM5 = INVALID_HANDLE;         // for panic check (PERIOD_M5)
int g_atrHandleCurrent = INVALID_HANDLE;    // for trailing & ATR fallback (PERIOD_CURRENT)
int g_atrCurrentPeriod = 0;

void MaybeSendHeartbeat();
bool IsPanicMarket();
void EnsureAtrHandles();
bool IsInHardCloseWindow();
bool IsBeforeTradingStart();

double GetCurrentAtr();
int ComputeDeviationPoints(const double atr, const int basePoints);
string EmergencyGVName();
void LoadEmergencyState();
void SaveEmergencyState();

bool ShouldFireEmergencyCut(double &eq, double &dayStartEq, double &marginLevel, string &reason);
void FireEmergencyCut(const string reason, const double eq, const double dayStartEq, const double marginLevel);

//+------------------------------------------------------------------+
int OnInit() {
   m_trade.SetExpertMagicNumber(MagicNumber);
   if(!socket.connect(ZmqUrl)) return INIT_FAILED;

   if(EnableHeartbeat) {
      if(hb_socket.connect(HeartbeatUrl)) {
         g_hb_connected = true;
         Print("[Muscle] Heartbeat enabled -> connect ", HeartbeatUrl);
      } else {
         g_hb_connected = false;
         Print("[Muscle][WARN] Heartbeat connect failed: ", HeartbeatUrl);
      }
   }

   EventSetMillisecondTimer(10); // 0.01秒ごとにPythonの命令を確認

   // 日次ガード初期化
   g_dayStartTime = iTime(_Symbol, PERIOD_D1, 0);
   g_dayStartEquity = AccountInfoDouble(ACCOUNT_EQUITY);
   g_haltNewEntriesToday = false;
   g_emergencyHaltActive = false;
   g_emergencyFiredToday = false;
   g_emergencyFiredAt = 0;
   g_emergencyFiredDate = 0;

   // Indicator handles (create once)
   g_atrHandleM5 = iATR(_Symbol, PERIOD_M5, 14);
   g_atrCurrentPeriod = (int)_Period;
   g_atrHandleCurrent = iATR(_Symbol, (ENUM_TIMEFRAMES)g_atrCurrentPeriod, 14);

   LoadEmergencyState();

   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason) {
   EventKillTimer();
   if(g_atrHandleM5 != INVALID_HANDLE) { IndicatorRelease(g_atrHandleM5); g_atrHandleM5 = INVALID_HANDLE; }
   if(g_atrHandleCurrent != INVALID_HANDLE) { IndicatorRelease(g_atrHandleCurrent); g_atrHandleCurrent = INVALID_HANDLE; }
}

void EnsureAtrHandles() {
   // M5 handle (panic)
   if(g_atrHandleM5 == INVALID_HANDLE)
      g_atrHandleM5 = iATR(_Symbol, PERIOD_M5, 14);

   // PERIOD_CURRENT handle: recreate if timeframe changed (to keep behavior identical)
   int curP = (int)_Period;
   if(g_atrCurrentPeriod != curP) {
      if(g_atrHandleCurrent != INVALID_HANDLE) IndicatorRelease(g_atrHandleCurrent);
      g_atrCurrentPeriod = curP;
      g_atrHandleCurrent = iATR(_Symbol, (ENUM_TIMEFRAMES)g_atrCurrentPeriod, 14);
   }
   if(g_atrHandleCurrent == INVALID_HANDLE)
      g_atrHandleCurrent = iATR(_Symbol, (ENUM_TIMEFRAMES)curP, 14);
}

double GetCurrentAtr() {
   // Note: Caller must ensure EnsureAtrHandles() called first
   if(g_atrHandleCurrent == INVALID_HANDLE) return 0.0;
   double b[];
   if(CopyBuffer(g_atrHandleCurrent, 0, 0, 1, b) > 0) return b[0];
   return 0.0;
}

int ComputeDeviationPoints(const double atr, const int basePoints) {
   int dev = (int)MathMax(0, basePoints);
   if(UseDynamicSlippage && atr > 0.0 && _Point > 0.0 && SlippageAtrMult > 0.0) {
      double atrPts = atr / _Point;
      int dyn = (int)MathRound(atrPts * SlippageAtrMult);
      if(dyn > dev) dev = dyn;
   }
   if(SlippageMinPoints > 0 && dev < SlippageMinPoints) dev = SlippageMinPoints;
   if(SlippageMaxPoints > 0 && dev > SlippageMaxPoints) dev = SlippageMaxPoints;
   return dev;
}

string EmergencyGVName() {
   long login = (long)AccountInfoInteger(ACCOUNT_LOGIN);
   return StringFormat("%s:%I64d:%s:%d", EmergencyStateKeyPrefix, login, _Symbol, MagicNumber);
}

void LoadEmergencyState() {
   if(!PersistEmergencyState) return;
   string name = EmergencyGVName();
   if(!GlobalVariableCheck(name)) return;
   double v = GlobalVariableGet(name);
   if(v <= 0.0) return;
   g_emergencyFiredDate = (datetime)(long)v;
   datetime today = iTime(_Symbol, PERIOD_D1, 0);
   if(g_emergencyFiredDate == today) g_emergencyFiredToday = true;
}

void SaveEmergencyState() {
   if(!PersistEmergencyState) return;
   string name = EmergencyGVName();
   GlobalVariableSet(name, (double)g_emergencyFiredDate);
}

void OnTimer() {
   EnsureAtrHandles(); // 先頭で1回確保
   ResetDailyGuardsIfNeeded();

   double eq=0.0, ds=0.0, ml=0.0;
   string reason="";
   if(ShouldFireEmergencyCut(eq, ds, ml, reason)) {
      FireEmergencyCut(reason, eq, ds, ml);
      return;
   }

   MaybeSendHeartbeat();

   // 受信キューを可能な限り捌く（取りこぼし防止）
   for(int i=0; i<50; i++) {
      ZmqMsg msg;
      if(!socket.recv(msg, ZMQ_NOBLOCK)) break;
      string json = msg.getData();

      // Deserialize here (requirement: parse multiplier from JSON in OnTimer path)
      CJAVal obj;
      if(!obj.Deserialize(json)) {
         g_zmqDeserializeFails++;
         if(LogZmqDeserializeFailures) {
            int every = (int)MathMax(1, ZmqDeserializeWarnEvery);
            if((g_zmqDeserializeFails % (ulong)every) == 0) {
               Print("[Muscle][WARN] ZMQ Deserialize failed count=", (long)g_zmqDeserializeFails, " last_len=", StringLen(json));
            }
         }
         continue;
      }
      ProcessCommandObj(obj);
   }
}

void MaybeSendHeartbeat() {
   if(!EnableHeartbeat) return;
   if(!g_hb_connected) return;

   uint nowMs = GetTickCount();
   if(g_lastHbSentMs != 0 && (nowMs - g_lastHbSentMs) < (uint)MathMax(50, HeartbeatIntervalMs)) return;
   g_lastHbSentMs = nowMs;

   long login = (long)AccountInfoInteger(ACCOUNT_LOGIN);

   // Broker server time signals (for weekend-close scheduling)
   long tradeServerTs = (long)TimeTradeServer();
   long gmtTs = (long)TimeGMT();
   long offsetSec = (long)(tradeServerTs - gmtTs);

   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   double balance = AccountInfoDouble(ACCOUNT_BALANCE);
   int posCount = CountMyPositions();
   string halt = (g_haltNewEntriesToday ? "true" : "false");

   long deserFails = (long)g_zmqDeserializeFails;
   long hbSendFails = (long)g_hbSendFails;

   // keep JSON minimal to avoid escaping issues
   string msg = StringFormat(
      "{\"type\":\"HEARTBEAT\",\"ts\":%I64d,\"trade_server_ts\":%I64d,\"gmt_ts\":%I64d,\"server_gmt_offset_sec\":%I64d,\"symbol\":\"%s\",\"login\":%I64d,\"ok\":true,\"equity\":%.2f,\"balance\":%.2f,\"positions\":%d,\"halt\":%s,\"magic\":%d,\"zmq_deser_fail\":%I64d,\"hb_send_fail\":%I64d}",
      (long)TimeCurrent(),
      tradeServerTs,
      gmtTs,
      offsetSec,
      _Symbol,
      login,
      equity,
      balance,
      posCount,
      halt,
      MagicNumber,
      deserFails,
      hbSendFails
   );
   bool ok = hb_socket.send(msg);
   if(!ok) {
      g_hbSendFails++;
      static uint s_lastWarnMs = 0;
      if(s_lastWarnMs == 0 || (nowMs - s_lastWarnMs) > 5000) {
         s_lastWarnMs = nowMs;
         Print("[Muscle][WARN] Heartbeat send failed -> ", HeartbeatUrl);
      }
   }
}

void OnTick() {
   EnsureAtrHandles(); // 先頭で1回確保
   ResetDailyGuardsIfNeeded();
   
   // Priority 0: Hard close window (highest priority for account protection)
   if(IsInHardCloseWindow()) {
      if(CountMyPositions() > 0) {
         Print("[Muscle][HARD_CLOSE] Server time=", TimeToString(TimeCurrent(), TIME_DATE|TIME_MINUTES), " Closing all positions.");
         CloseAll();
      }
      return; // Block all operations during hard close window
   }
   
   double eq=0.0, ds=0.0, ml=0.0;
   string reason="";
   if(ShouldFireEmergencyCut(eq, ds, ml, reason)) {
      FireEmergencyCut(reason, eq, ds, ml);
      return;
   }
   if(EnableTrailing) HandleAllTrailing();
}

// --- Server-time window checks (DST-free) ---
bool IsInHardCloseWindow() {
   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
   return (dt.hour == HardCloseHour && dt.min >= HardCloseMinute);
}

bool IsBeforeTradingStart() {
   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
   return (dt.hour < TradingStartHour);
}

// --- Optional time filter (user-configurable) ---
bool IsWithinTradingHours() {
   if(!UseTimeFilter) return true;
   MqlDateTime dt; TimeToStruct(TimeCurrent(), dt);
   if(StartHour <= EndHour) {
      return (dt.hour >= StartHour && dt.hour <= EndHour);
   }
   // wrap-around (例: 22-6)
   return (dt.hour >= StartHour || dt.hour <= EndHour);
}

int CountMyPositions() {
   CPositionInfo p;
   int count = 0;
   for(int i=PositionsTotal()-1; i>=0; i--) {
      if(p.SelectByIndex(i) && p.Magic()==MagicNumber && p.Symbol()==_Symbol) count++;
   }
   return count;
}

void ResetDailyGuardsIfNeeded() {
   datetime t = iTime(_Symbol, PERIOD_D1, 0);
   if(g_dayStartTime != t) {
      g_dayStartTime = t;
      g_dayStartEquity = AccountInfoDouble(ACCOUNT_EQUITY);
      g_haltNewEntriesToday = false;
      g_emergencyHaltActive = false;
      g_emergencyFiredToday = false;
      g_emergencyFiredAt = 0;
      g_emergencyFiredDate = 0;
      
      // 日次リセット時にtrail_modeをNORMALに戻す（オプション）
      if(g_ai_trail_mode != 0) {
         Print("[Muscle][TRAIL] Daily reset: trail_mode NORMAL (was ", g_ai_trail_mode, ")");
         g_ai_trail_mode = 0;
         g_trail_mode_updated_at = 0;
      }
      
      // 日次リセット時にtp_modeもリセット
      if(g_tp_mode != 0) {
         Print("[Muscle][TP] Daily reset: tp_mode NORMAL (was ", g_tp_mode, ")");
         g_tp_mode = 0;
         g_tp_mode_updated_at = 0;
      }
      
      SaveEmergencyState();
   }
   if(!g_haltNewEntriesToday && g_dayStartEquity > 0.0) {
      double eq = AccountInfoDouble(ACCOUNT_EQUITY);
      if(eq < g_dayStartEquity * (1.0 - DailyLossLimitPct/100.0)) {
         g_haltNewEntriesToday = true;
         string haltMsg = AllowTradingDuringDailyLossHalt ? "[TESTING MODE ACTIVE]" : "Halt entries today.";
         Print("[Muscle] Daily loss limit hit. ", haltMsg, " equity=", DoubleToString(eq,2), " start=", DoubleToString(g_dayStartEquity,2));

         // Panic判定（M5ベース）。危険な荒れ相場のときだけクールダウン+（任意で）全決済。
         bool panic = false;
         if(UsePanicCloseOnDailyLoss) {
            panic = IsPanicMarket();
            if(panic) {
               g_panicUntil = TimeCurrent() + (datetime)(PanicCooldownMin * 60);
               Print("[Muscle][PANIC] Panic market detected. cooldown_until=", TimeToString(g_panicUntil, TIME_DATE|TIME_MINUTES|TIME_SECONDS));
            } else {
               Print("[Muscle] No panic market detected at daily loss hit; skip CloseAll (if conditional enabled).");
            }
         }

         if(CloseAllOnDailyLoss) {
            if(!UsePanicCloseOnDailyLoss) {
               // 従来通り
               CloseAll();
            } else if(panic) {
               if(PanicLogOnly) {
                  Print("[Muscle][PANIC][LOG_ONLY] CloseAllOnDailyLoss=true but PanicLogOnly=true -> skip CloseAll.");
               } else {
                  CloseAll();
               }
            }
         }
      }
   }
}

bool IsPanicMarket() {
   // ① スプレッド判定
   double ask = SymbolInfoDouble(_Symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
   if(ask > 0.0 && bid > 0.0 && _Point > 0.0) {
      double spread_points = (ask - bid) / _Point;
      if(spread_points >= (double)PanicSpreadPoints) return true;
   }

   // ② 価格変動判定（5分足・確定足 shift=1）
   double hi = iHigh(_Symbol, PERIOD_M5, 1);
   double lo = iLow(_Symbol, PERIOD_M5, 1);
   if(hi <= 0.0 || lo <= 0.0 || hi < lo) return false;
   double range = hi - lo;

   // Note: ATR handle already ensured by caller
   if(g_atrHandleM5 == INVALID_HANDLE) return false;
   double atrBuf[];
   bool ok = (CopyBuffer(g_atrHandleM5, 0, 1, 1, atrBuf) > 0);
   if(!ok) return false;
   double atr = atrBuf[0];
   if(atr <= 0.0) return false;

   if(range >= PanicRangeAtrMult * atr) return true;
   return false;
}

double CalculateLotSize(string act, double ent, double sl) {
   // Wrapper for requirement wording; internally uses existing risk lot logic
   return CalculateRiskLot(act, ent, sl);
}

bool ExecuteTrade(string action, double lot, double price, double sl, double atr=0.0) {
   if(lot <= 0) return false;
   int deviation = ComputeDeviationPoints(atr, EntrySlippagePoints);
   m_trade.SetDeviationInPoints(deviation);
   
   // --- 初期TP計算（tp_modeに応じて距離を調整） ---
   double tp = 0.0;
   if(atr > 0.0) {
      double tpModeMultiplier = 1.0;  // NORMAL: 5-7x ATR
      if(g_tp_mode == 1) tpModeMultiplier = 1.5;       // WIDE: 10x+ ATR
      else if(g_tp_mode == 2) tpModeMultiplier = 0.6;  // TIGHT: 2-3x ATR
      
      double tpDist = TpTrailingDistATR * atr * tpModeMultiplier;
      if(action == "BUY") {
         tp = price + tpDist;
      } else {
         tp = price - tpDist;
      }
      tp = NormalizeDouble(tp, _Digits);
      
      string tpModeStr = (g_tp_mode == 0 ? "NORMAL" : (g_tp_mode == 1 ? "WIDE" : "TIGHT"));
      Print("[Muscle][TP] Initial TP set: action=", action, " tp=", tp, " mode=", tpModeStr, 
            " multiplier=", DoubleToString(tpModeMultiplier, 1), " distance=", DoubleToString(tpDist/_Point, 1), "pts");
   }
   
   if(action == "BUY")  return m_trade.Buy(lot, _Symbol, price, NormalizeDouble(sl, _Digits), tp);
   if(action == "SELL") return m_trade.Sell(lot, _Symbol, price, NormalizeDouble(sl, _Digits), tp);
   return false;
}

void ProcessCommandObj(CJAVal &obj) {
   // --- REVIEW: trail_modeをメッセージ先頭で読み取り、変更時にログ出力 ---
   CJAVal *vTrailMode = obj.HasKey("trail_mode", jtSTR);
   if(vTrailMode != NULL) {
      string mode = vTrailMode.ToStr();
      int newMode = 0;
      if(mode == "WIDE")       newMode = 1;
      else if(mode == "TIGHT") newMode = 2;
      else                     newMode = 0; // NORMAL
      
      // 変更検知してログ出力（デバッグ可視化）
      if(newMode != g_ai_trail_mode) {
         string oldModeStr = (g_ai_trail_mode == 0 ? "NORMAL" : (g_ai_trail_mode == 1 ? "WIDE" : "TIGHT"));
         string newModeStr = (newMode == 0 ? "NORMAL" : (newMode == 1 ? "WIDE" : "TIGHT"));
         Print("[Muscle][TRAIL] Mode change: ", oldModeStr, " -> ", newModeStr, 
               " (multiplier: ", (newMode == 1 ? "1.5x" : (newMode == 2 ? "0.7x" : "1.0x")), ")");
         g_ai_trail_mode = newMode;
         g_trail_mode_updated_at = TimeCurrent();
      }
   }

   // --- REVIEW: tp_modeをメッセージから読み取り、変更時にログ出力 ---
   CJAVal *vTpMode = obj.HasKey("tp_mode", jtSTR);
   if(vTpMode != NULL) {
      string mode = vTpMode.ToStr();
      int newMode = 0;
      if(mode == "WIDE")       newMode = 1;
      else if(mode == "TIGHT") newMode = 2;
      else                     newMode = 0; // NORMAL
      
      // 変更検知してログ出力（デバッグ可視化）
      if(newMode != g_tp_mode) {
         string oldModeStr = (g_tp_mode == 0 ? "NORMAL" : (g_tp_mode == 1 ? "WIDE" : "TIGHT"));
         string newModeStr = (newMode == 0 ? "NORMAL" : (newMode == 1 ? "WIDE" : "TIGHT"));
         Print("[Muscle][TP] Mode change: ", oldModeStr, " -> ", newModeStr, 
               " (target: ", (newMode == 1 ? "10x+" : (newMode == 2 ? "2-3x" : "5-7x")), " ATR)");
         g_tp_mode = newMode;
         g_tp_mode_updated_at = TimeCurrent();
      }
   }

   string type = obj["type"].ToStr();

   // Optional symbol routing (safety)
   CJAVal *vSym = obj.HasKey("symbol", jtSTR);
   if(vSym != NULL) {
      string sym = vSym.ToStr();
      if(sym != "" && sym != _Symbol) {
         Print("[Muscle] Ignore message for other symbol=", sym, " this=", _Symbol, " type=", type);
         return;
      }
   }

   // Lightweight recv log
   Print("[Muscle] RX type=", type);
   
   if(type == "CLOSE") {
      g_lastHoldAt = 0;
      CloseAll();
      return;
   }

   if(type == "HOLD") {
      string holdReason = obj["reason"].ToStr();
      Print("[Muscle] AI Decision: HOLD - ", holdReason);
      g_lastHoldAt = TimeCurrent();
      // trail_modeは既に上で読み取り済み、次回OnTick()のHandleAllTrailing()で適用される
      return;
   }

   // REVIEW: CLOSE_SIGNALは不要（Python側で送信しない）、念のため保持
   if(type == "CLOSE_SIGNAL") {
      Print("[Muscle][WARN] Unexpected CLOSE_SIGNAL (treated as HOLD). reason=", obj["reason"].ToStr());
      return;
   }

   if(type == "ORDER") {
      // Priority 1: Emergency Cut (ignores testing mode)
      if(g_emergencyHaltActive) {
         Print("[Muscle] Block ORDER: EMERGENCY halt active (口座保護優先). reason=", obj["reason"].ToStr());
         return;
      }
      // Priority 2: Panic cooldown (ignores testing mode for safety)
      if(TimeCurrent() < g_panicUntil) {
         Print("[Muscle] Block ORDER: panic cooldown. until=", TimeToString(g_panicUntil, TIME_DATE|TIME_MINUTES|TIME_SECONDS), " reason=", obj["reason"].ToStr());
         return;
      }
      // Priority 2.5: Server-time hard close window
      if(IsInHardCloseWindow()) {
         Print("[Muscle] Block ORDER: hard close window (server_time=", TimeToString(TimeCurrent(), TIME_DATE|TIME_MINUTES), "). reason=", obj["reason"].ToStr());
         return;
      }
      // Priority 2.6: Before trading start (pre-market block)
      if(IsBeforeTradingStart()) {
         Print("[Muscle] Block ORDER: before trading start hour (server_time=", TimeToString(TimeCurrent(), TIME_DATE|TIME_MINUTES), "). reason=", obj["reason"].ToStr());
         return;
      }
      // Priority 3: Daily loss halt (respects testing mode)
      if(g_haltNewEntriesToday && !AllowTradingDuringDailyLossHalt) {
         Print("[Muscle] Block ORDER: daily loss halt. reason=", obj["reason"].ToStr());
         return;
      }
      if(g_haltNewEntriesToday && AllowTradingDuringDailyLossHalt) {
         Print("[Muscle][TESTING MODE] Daily loss halt bypassed. equity=", DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY),2));
      }
      if(!IsWithinTradingHours()) {
         Print("[Muscle] Block ORDER: out of trading hours. reason=", obj["reason"].ToStr());
         return;
      }
      if(CountMyPositions() >= MaxPositions) {
         Print("[Muscle] Block ORDER: max positions reached. reason=", obj["reason"].ToStr());
         return;
      }

      string action = obj["action"].ToStr();
      double mult   = obj["multiplier"].ToDbl();
      double liveAtr = obj["atr"].ToDbl();
      string reason = obj["reason"].ToStr();

      // --- Multiplier validation (defaults keep existing clamp behavior) ---
      if(mult <= 0) mult = 1.0;
      if(RejectAbnormalMultiplier) {
         if(mult < MultiplierMin || mult > MultiplierMax) {
            Print("[Muscle] Block ORDER: abnormal multiplier=", DoubleToString(mult, 2), " range=", DoubleToString(MultiplierMin,2), "..", DoubleToString(MultiplierMax,2), " reason=", reason);
            return;
         }
      } else {
         if(mult < MultiplierMin) mult = MultiplierMin;
         if(mult > MultiplierMax) mult = MultiplierMax;
      }

      StringToUpper(action);
      if(action != "BUY" && action != "SELL") {
         Print("[Muscle] Block ORDER: invalid action=", action, " reason=", reason);
         return;
      }
      
      // ATRフォールバック（Python側の市場データが取れないと atr=0 で全ブロックされるため）
      if(liveAtr <= 0) {
         // Note: ATR handle already ensured by OnTimer
         if(g_atrHandleCurrent != INVALID_HANDLE) {
            double atrBuf[];
            if(CopyBuffer(g_atrHandleCurrent, 0, 0, 1, atrBuf) > 0) liveAtr = atrBuf[0];
         }
      }
      if(liveAtr <= 0) {
         Print("[Muscle] Block ORDER: atr unavailable. reason=", reason);
         return;
      }

      // Optional AI metadata
      double aiConf = obj["ai_confidence"].ToDbl();
      string aiReason = obj["ai_reason"].ToStr();

      double price = (action == "BUY") ? SymbolInfoDouble(_Symbol, SYMBOL_ASK) : SymbolInfoDouble(_Symbol, SYMBOL_BID);
      double sl = (action == "BUY") ? price - (liveAtr * 1.5) : price + (liveAtr * 1.5);

      // Requirement: CalculateLotSize(sl) * multiplier
      double lot = CalculateLotSize(action, price, sl);
      double finalLot = NormalizeVolume(lot * mult);
      if(finalLot > MaxAllowedLot) finalLot = MaxAllowedLot;

      if(finalLot <= 0) {
         Print("[Muscle] Block ORDER: lot<=0. reason=", reason);
         return;
      }

      ExecuteTrade(action, finalLot, price, sl, liveAtr);

      Print("[Muscle] Order Executed: ", action,
            " Lot:", finalLot,
            " mult:", DoubleToString(mult, 2),
            " AIconf:", DoubleToString(aiConf, 0),
            " AIreason:", aiReason,
            " Reason:", reason,
            " atr:", DoubleToString(liveAtr, _Digits));
   }
}

// Backward-compatible wrapper
void ProcessCommand(string json) {
   CJAVal obj;
   if(!obj.Deserialize(json)) return;
   ProcessCommandObj(obj);
}

// --- v2.6継承ロジック ---
double CalculateRiskLot(string act, double ent, double sl) {
   double risk = AccountInfoDouble(ACCOUNT_EQUITY) * (RiskPercent/100.0);
   double profit = 0.0;

   ENUM_ORDER_TYPE type = (act=="BUY" ? ORDER_TYPE_BUY : ORDER_TYPE_SELL);
   if(!OrderCalcProfit(type, _Symbol, 1.0, ent, sl, profit)) {
      Print("[Muscle] OrderCalcProfit failed. err=", GetLastError(), " symbol=", _Symbol, " ent=", ent, " sl=", sl);
      return (AllowLotFallbackOnCalcFail ? LotFallbackValue : 0.0);
   }

   if(profit == 0.0) return (AllowLotFallbackOnZeroProfit ? LotFallbackValue : 0.0);
   return MathAbs(risk/profit);
}

double NormalizeVolume(double v) {
   double s = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   return MathMax(MathFloor(v/s)*s, SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN));
}

double NormalizeLot(double v) {
   return NormalizeVolume(v);
}

void HandleAllTrailing() {
   // Note: ATR handle already ensured by OnTick
   if(g_atrHandleCurrent == INVALID_HANDLE) return;
   double atr = 0;
   double b[];
   if(CopyBuffer(g_atrHandleCurrent, 0, 0, 1, b)>0) atr = b[0];
   if(atr <= 0) return;

   // --- AIモードによるSL倍率の動的変化 (TRAIL_MODE) ---
   double slModeMultiplier = 1.0;
   string trailModeStr = "NORMAL";
   if(g_ai_trail_mode == 1) {
      slModeMultiplier = 1.5; // WIDE: 広く取って大きな波を狙う
      trailModeStr = "WIDE";
   }
   if(g_ai_trail_mode == 2) {
      slModeMultiplier = 0.7; // TIGHT: 狭めて利益を守る
      trailModeStr = "TIGHT";
   }

   // --- AIモードによるTP倍率の動的変化 (TP_MODE) ---
   // TP距離 = TpTrailingDistATR * modeMultiplier
   double tpModeMultiplier = 1.0;
   string tpModeStr = "NORMAL";
   if(g_tp_mode == 1) {
      tpModeMultiplier = 1.5; // WIDE: 遠くに置いて10x+狙う
      tpModeStr = "WIDE";
   }
   if(g_tp_mode == 2) {
      tpModeMultiplier = 0.6; // TIGHT: 手前に置いて早期利食い（2-3x狙い）
      tpModeStr = "TIGHT";
   }

   // REVIEW: トレーリング実行時にモード情報をログ出力（デバッグ可視化）
   static int s_lastLoggedMode = -1;
   if(s_lastLoggedMode != g_ai_trail_mode) {
      Print("[Muscle][TRAIL] HandleAllTrailing: trail_mode=", trailModeStr, " sl_multiplier=", DoubleToString(slModeMultiplier, 2), 
            " last_updated=", (g_trail_mode_updated_at > 0 ? TimeToString(g_trail_mode_updated_at, TIME_DATE|TIME_MINUTES|TIME_SECONDS) : "never"));
      s_lastLoggedMode = g_ai_trail_mode;
   }

   // TP_MODEの変更もログ出力
   static int s_lastLoggedTpMode = -1;
   if(s_lastLoggedTpMode != g_tp_mode) {
      Print("[Muscle][TP] TP mode change: tp_mode=", tpModeStr, " tp_multiplier=", DoubleToString(tpModeMultiplier, 2),
            " last_updated=", (g_tp_mode_updated_at > 0 ? TimeToString(g_tp_mode_updated_at, TIME_DATE|TIME_MINUTES|TIME_SECONDS) : "never"));
      s_lastLoggedTpMode = g_tp_mode;
   }

   CPositionInfo pos;
   CTrade trade;
   trade.SetExpertMagicNumber(MagicNumber);
   trade.SetDeviationInPoints(ComputeDeviationPoints(atr, MgmtSlippagePoints));

   for(int i=PositionsTotal()-1; i>=0; i--) {
      if(pos.SelectByIndex(i) && pos.Magic() == MagicNumber && pos.Symbol() == _Symbol) {
         ulong  ticket = pos.Ticket(); 
         double open   = pos.PriceOpen();
         double sl     = pos.StopLoss();
         double tp     = pos.TakeProfit();
         double vol    = pos.Volume();
         double price  = (pos.PositionType()==POSITION_TYPE_BUY)?SymbolInfoDouble(_Symbol,SYMBOL_BID):SymbolInfoDouble(_Symbol,SYMBOL_ASK);
         double profit = (pos.PositionType()==POSITION_TYPE_BUY)?(price-open):(open-price);

         // --- 1. 部分利確 (Partial Close) & 建値移動 ---
         // まだSLが建値に移動されていない場合のみ判定
         // BUY: sl >= open なら建値以上（移動済み）
         // SELL: sl <= open かつ sl > 0 なら建値以下（移動済み）※SLはopenより上が初期値
         bool isAlreadyBE = (pos.PositionType()==POSITION_TYPE_BUY) ? (sl >= open) : (sl <= open && sl > 0);
         
         if(EnablePartialClose && !isAlreadyBE && profit > PartialCloseATR * atr) {
            double closeVol = NormalizeLot(vol * (PartialClosePercent / 100.0));
            if(closeVol >= SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN)) {
               if(trade.PositionClosePartial(ticket, closeVol)) {
                  // 部分利確成功後、ポジション情報を再取得（volume/TP等が変わっている）
                  if(!pos.SelectByTicket(ticket)) {
                     Print("[Muscle][TRAIL][ERROR] Failed to reselect position after partial close: ticket=", ticket);
                     continue;
                  }
                  
                  // SLを建値(微益)へ移動
                  double beSL = (pos.PositionType()==POSITION_TYPE_BUY) ? (open + 20*_Point) : (open - 20*_Point);
                  bool modOk = trade.PositionModify(ticket, NormalizeDouble(beSL, _Digits), pos.TakeProfit());
                  
                  if(!modOk) {
                     Print("[Muscle][TRAIL][ERROR] Failed to move SL to BE after partial close: ticket=", ticket, 
                           " retcode=", trade.ResultRetcode(), " ", trade.ResultRetcodeDescription());
                  }
                  
                  // 強制WIDE移行（リスクゼロ化後は利益最大化モード）
                  // 注意: グローバル変数なので全ポジションに影響
                  if(g_ai_trail_mode != 1) {
                     g_ai_trail_mode = 1;
                     g_trail_mode_updated_at = TimeCurrent();
                     Print("[Muscle][TRAIL] Auto-switch to WIDE mode after partial close");
                  }
                  Print("[Muscle][TRAIL] Partial Close & BE: ticket=", ticket, " mode=WIDE be_mod=", (modOk?"OK":"FAIL"));
                  continue;
               } else {
                  Print("[Muscle][TRAIL][ERROR] PositionClosePartial failed: ticket=", ticket,
                        " retcode=", trade.ResultRetcode(), " ", trade.ResultRetcodeDescription());
               }
            }
         }

         // --- 2. 動的トレーリングストップ (slModeMultiplier適用) ---
         if(profit > TrailingStartATR * atr) {
            // slModeMultiplierを適用して距離を調整
            double targetSL = (pos.PositionType()==POSITION_TYPE_BUY) ? (price - TrailingDistATR * atr * slModeMultiplier) : (price + TrailingDistATR * atr * slModeMultiplier);
            
            if(pos.PositionType()==POSITION_TYPE_BUY) {
               if(targetSL > sl + TrailingStepATR * atr) {
                  if(trade.PositionModify(ticket, NormalizeDouble(targetSL, _Digits), pos.TakeProfit())) {
                     Print("[Muscle][TRAIL] BUY trailing updated: ticket=", ticket, " new_sl=", targetSL, " trail_mode=", trailModeStr);
                     // SL更新成功→次のTPトレーリングで使用するため変数を更新
                     sl = targetSL;
                  } else {
                     Print("[Muscle][TRAIL][ERROR] BUY trailing modify failed: ticket=", ticket,
                           " retcode=", trade.ResultRetcode(), " ", trade.ResultRetcodeDescription());
                  }
               }
            } else {
               if(sl == 0 || targetSL < sl - TrailingStepATR * atr) {
                  if(trade.PositionModify(ticket, NormalizeDouble(targetSL, _Digits), pos.TakeProfit())) {
                     Print("[Muscle][TRAIL] SELL trailing updated: ticket=", ticket, " new_sl=", targetSL, " trail_mode=", trailModeStr);
                     // SL更新成功→次のTPトレーリングで使用するため変数を更新
                     sl = targetSL;
                  } else {
                     Print("[Muscle][TRAIL][ERROR] SELL trailing modify failed: ticket=", ticket,
                           " retcode=", trade.ResultRetcode(), " ", trade.ResultRetcodeDescription());
                  }
               }
            }
         }

         // --- 3. TPトレーリング (HOLD時のみ & tpModeMultiplier適用) ---
         bool holdActive = (EnableTpTrailingOnHold && g_lastHoldAt > 0 && (TimeCurrent() - g_lastHoldAt) <= HoldTpWindowSec);
         if(holdActive && profit > TpTrailingStartATR * atr) {
            // TP距離に tp_mode の倍率を適用
            double targetTP = (pos.PositionType()==POSITION_TYPE_BUY)
                              ? (price + TpTrailingDistATR * atr * tpModeMultiplier)
                              : (price - TpTrailingDistATR * atr * tpModeMultiplier);

            // TP更新時は、動的トレーリングで更新済みの可能性があるSL変数を使用
            // （ステップ2で更新されていれば最新値、されていなければ元の値）
            if(pos.PositionType()==POSITION_TYPE_BUY) {
               if(tp == 0 || targetTP > tp + TpTrailingStepATR * atr) {
                  if(trade.PositionModify(ticket, NormalizeDouble(sl, _Digits), NormalizeDouble(targetTP, _Digits))) {
                     Print("[Muscle][TP] BUY TP updated: ticket=", ticket, " new_tp=", targetTP, " sl=", sl, " tp_mode=", tpModeStr, " target_ratio=", DoubleToString(tpModeMultiplier, 1));
                  } else {
                     Print("[Muscle][TP][ERROR] BUY TP modify failed: ticket=", ticket,
                           " retcode=", trade.ResultRetcode(), " ", trade.ResultRetcodeDescription());
                  }
               }
            } else {
               if(tp == 0 || targetTP < tp - TpTrailingStepATR * atr) {
                  if(trade.PositionModify(ticket, NormalizeDouble(sl, _Digits), NormalizeDouble(targetTP, _Digits))) {
                     Print("[Muscle][TP] SELL TP updated: ticket=", ticket, " new_tp=", targetTP, " sl=", sl, " tp_mode=", tpModeStr, " target_ratio=", DoubleToString(tpModeMultiplier, 1));
                  } else {
                     Print("[Muscle][TP][ERROR] SELL TP modify failed: ticket=", ticket,
                           " retcode=", trade.ResultRetcode(), " ", trade.ResultRetcodeDescription());
                  }
               }
            }
         }
      }
   }
}

void CloseAll() {
   CTrade close_trade;
   close_trade.SetExpertMagicNumber(MagicNumber);
   double atr = (UseDynamicSlippage ? GetCurrentAtr() : 0.0);
   close_trade.SetDeviationInPoints(ComputeDeviationPoints(atr, MgmtSlippagePoints));
   CPositionInfo p;
   for(int i=PositionsTotal()-1; i>=0; i--) {
      if(p.SelectByIndex(i) && p.Magic()==MagicNumber && p.Symbol()==_Symbol) close_trade.PositionClose(p.Ticket());
   }
}

bool ShouldFireEmergencyCut(double &eq, double &dayStartEq, double &marginLevel, string &reason) {
   eq = AccountInfoDouble(ACCOUNT_EQUITY);
   dayStartEq = g_dayStartEquity;
   marginLevel = AccountInfoDouble(ACCOUNT_MARGIN_LEVEL);
   reason = "";

   if(!EnableEmergencyCut) return false;
   datetime today = iTime(_Symbol, PERIOD_D1, 0);
   if(g_emergencyFiredDate == today) return false;

   bool hasPositions = (PositionsTotal() > 0);

   // A) Equity drawdown from day start
   if(dayStartEq > 0.0) {
      double threshEq = dayStartEq * (1.0 - EmergencyLossPct/100.0);
      if(eq <= threshEq) {
         reason = "EQUITY_DD";
         return true;
      }
   } else {
      // dayStartEq<=0: do not evaluate equity condition (avoid misfire)
      static datetime s_lastWarnDayStart = 0;
      if(s_lastWarnDayStart != g_dayStartTime) {
         s_lastWarnDayStart = g_dayStartTime;
         Print("[Muscle][EMERGENCY][WARN] g_dayStartEquity<=0 -> skip equity condition until daily init.");
      }
   }

   // B) Margin level (only if positions exist)
   if(hasPositions) {
      if(marginLevel > 0.0 && marginLevel <= EmergencyMarginLevel) {
         reason = "MARGIN_LEVEL";
         return true;
      }
   }

   return false;
}

void FireEmergencyCut(const string reason, const double eq, const double dayStartEq, const double marginLevel) {
   g_emergencyFiredToday = true;
   g_emergencyFiredAt = TimeCurrent();
   g_emergencyFiredDate = iTime(_Symbol, PERIOD_D1, 0);
   SaveEmergencyState();

   // 当日は新規停止（検証モード無視の強制停止フラグ）
   g_haltNewEntriesToday = true;
   g_emergencyHaltActive = true;

   // panic cooldown を延長（既存の仕組み流用）
   datetime until = TimeCurrent() + (datetime)(PanicCooldownMin * 60);
   if(g_panicUntil < until) g_panicUntil = until;

   Print("[Muscle][EMERGENCY] Fired reason=", reason,
         " eq=", DoubleToString(eq, 2),
         " dayStartEq=", DoubleToString(dayStartEq, 2),
         " marginLevel=", DoubleToString(marginLevel, 1),
         " EmergencyLossPct=", DoubleToString(EmergencyLossPct, 2),
         " EmergencyMarginLevel=", DoubleToString(EmergencyMarginLevel, 1),
         " EmergencyLogOnly=", (EmergencyLogOnly ? "true" : "false"),
         " panicUntil=", TimeToString(g_panicUntil, TIME_DATE|TIME_MINUTES|TIME_SECONDS));

   if(!EmergencyLogOnly) {
      int before = CountMyPositions();
      CloseAll();
      int after = CountMyPositions();
      Print("[Muscle][EMERGENCY] CloseAll called. positions(before/after)=", before, "/", after,
            " last_retcode=", (int)m_trade.ResultRetcode(),
            " last_retmsg=", m_trade.ResultRetcodeDescription());
   } else {
      Print("[Muscle][EMERGENCY][LOG_ONLY] CloseAll skipped.");
   }
}
