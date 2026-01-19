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

// --- Parameters ---
input string ZmqUrl             = "tcp://localhost:5555";
// --- Heartbeat (EA -> Python) ---
input bool   EnableHeartbeat    = true;
input string HeartbeatUrl       = "tcp://localhost:5556";
input int    HeartbeatIntervalMs = 1000;

input double RiskPercent        = 1.0;
input double MaxAllowedLot      = 0.20;
input int    MagicNumber        = 20250110;

// --- Safety (v2.6思想: 期待値を落とす誤発注を抑止) ---
input int    MaxPositions       = 5;        // 最大ポジション数（このEAのMagic+Symbol）
input bool   CloseAllOnDailyLoss = true;    // 日次損失限界で全決済するか
input double DailyLossLimitPct  = 2.0;      // 日次損失限界(%)

// --- Time filter (任意) ---
input bool   UseTimeFilter      = false;
input int    StartHour          = 9;
input int    EndHour            = 23;

// --- Trailing (自律動作) ---
input bool   EnableTrailing     = true;
input double TrailingStartATR   = 1.0;
input double TrailingDistATR    = 1.5;
input double TrailingStepATR    = 0.2;
// --- 部分利確設定 (New) ---
input bool EnablePartialClose = true;       // 部分利確を有効にする
input double PartialCloseATR = 1.5;         // 何倍のATRで部分利確するか
input double PartialClosePercent = 50.0;    // ロットの何%を決済するか

// --- 動的トレイリング用変数 (内部利用) ---
// AIから受け取ったモードを保持 (0=NORMAL, 1=WIDE, 2=TIGHT)
int g_ai_trail_mode = 0;


// --- Global ---
Context context;
Socket socket(context, ZMQ_PULL);
Socket hb_socket(context, ZMQ_PUSH);
CTrade m_trade;

bool g_hb_connected = false;
uint g_lastHbSentMs = 0;

datetime g_dayStartTime = 0;
double   g_dayStartEquity = 0.0;
bool     g_haltNewEntriesToday = false;

void MaybeSendHeartbeat();

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

   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason) { EventKillTimer(); }

void OnTimer() {
   MaybeSendHeartbeat();

   // 受信キューを可能な限り捌く（取りこぼし防止）
   for(int i=0; i<50; i++) {
      ZmqMsg msg;
      if(!socket.recv(msg, ZMQ_NOBLOCK)) break;
      string json = msg.getData();

      // Deserialize here (requirement: parse multiplier from JSON in OnTimer path)
      CJAVal obj;
      if(!obj.Deserialize(json)) continue;
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

   // keep JSON minimal to avoid escaping issues
   string msg = StringFormat(
      "{\"type\":\"HEARTBEAT\",\"ts\":%I64d,\"trade_server_ts\":%I64d,\"gmt_ts\":%I64d,\"server_gmt_offset_sec\":%I64d,\"symbol\":\"%s\",\"login\":%I64d,\"ok\":true,\"equity\":%.2f,\"balance\":%.2f,\"positions\":%d,\"halt\":%s,\"magic\":%d}",
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
      MagicNumber
   );
   hb_socket.send(msg);
}

void OnTick() {
   ResetDailyGuardsIfNeeded();
   if(EnableTrailing) HandleAllTrailing();
}

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
   }
   if(!g_haltNewEntriesToday && g_dayStartEquity > 0.0) {
      double eq = AccountInfoDouble(ACCOUNT_EQUITY);
      if(eq < g_dayStartEquity * (1.0 - DailyLossLimitPct/100.0)) {
         g_haltNewEntriesToday = true;
         Print("[Muscle] Daily loss limit hit. Halt entries today. equity=", DoubleToString(eq,2), " start=", DoubleToString(g_dayStartEquity,2));
         if(CloseAllOnDailyLoss) CloseAll();
      }
   }
}

double CalculateLotSize(string act, double ent, double sl) {
   // Wrapper for requirement wording; internally uses existing risk lot logic
   return CalculateRiskLot(act, ent, sl);
}

bool ExecuteTrade(string action, double lot, double price, double sl) {
   if(lot <= 0) return false;
   ulong deviation = 20; // 許容範囲 (points)
   m_trade.SetDeviationInPoints(deviation);
   if(action == "BUY")  return m_trade.Buy(lot, _Symbol, price, NormalizeDouble(sl, _Digits), 0.0);
   if(action == "SELL") return m_trade.Sell(lot, _Symbol, price, NormalizeDouble(sl, _Digits), 0.0);
   return false;
}

void ProcessCommandObj(CJAVal &obj) {
   // --- 追加: trail_modeを読み取る ---

   CJAVal *vTrailMode = obj.HasKey("trail_mode", jtSTR);
   if(vTrailMode != NULL) {
      string mode = vTrailMode.ToStr();
      if(mode == "WIDE")       g_ai_trail_mode = 1;
      else if(mode == "TIGHT") g_ai_trail_mode = 2;
      else                      g_ai_trail_mode = 0; // NORMAL
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
        CloseAll();
        return;
    }

   if(type == "HOLD") {
      Print("[Muscle] AI Decision: HOLD - ", obj["reason"].ToStr());
        return;
    }

   // Python側がtrail_modeだけ送る場合があるので、HOLDとして扱う
   if(type == "CLOSE_SIGNAL") {
      Print("[Muscle] CLOSE_SIGNAL treated as HOLD. reason=", obj["reason"].ToStr());
      return;
   }

    if(type == "ORDER") {
        if(g_haltNewEntriesToday) {
            Print("[Muscle] Block ORDER: daily loss halt. reason=", obj["reason"].ToStr());
            return;
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

      // --- Default + Safety clamp: final multiplier (Python-side) is clamped up to 2.0 ---
        if(mult <= 0) mult = 1.0;
        if(mult < 0.5) mult = 0.5;
      if(mult > 2.0) mult = 2.0;

        StringToUpper(action);
        if(action != "BUY" && action != "SELL") {
         Print("[Muscle] Block ORDER: invalid action=", action, " reason=", reason);
            return;
        }
      // ATRフォールバック（Python側の市場データが取れないと atr=0 で全ブロックされるため）
      if(liveAtr <= 0) {
         double atrBuf[];
         int hATR = iATR(_Symbol, PERIOD_CURRENT, 14);
         if(hATR != INVALID_HANDLE) {
            if(CopyBuffer(hATR, 0, 0, 1, atrBuf) > 0) liveAtr = atrBuf[0];
            IndicatorRelease(hATR);
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

        ExecuteTrade(action, finalLot, price, sl);

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
      return 0.01;
   }

   return (profit == 0.0) ? 0.01 : MathAbs(risk/profit);
}

double NormalizeVolume(double v) {
   double s = SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_STEP);
   return MathMax(MathFloor(v/s)*s, SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN));
}

double NormalizeLot(double v) {
   return NormalizeVolume(v);
}

void HandleAllTrailing() {
   double atr = 0;
   double b[]; int h = iATR(_Symbol, PERIOD_CURRENT, 14);
   if(CopyBuffer(h, 0, 0, 1, b)>0) atr = b[0];
   IndicatorRelease(h);
   if(atr <= 0) return;

   // --- AIモードによる倍率の動的変化 ---
   double modeMultiplier = 1.0;
   if(g_ai_trail_mode == 1) modeMultiplier = 1.5; // WIDE: 広く取って大きな波を狙う
   if(g_ai_trail_mode == 2) modeMultiplier = 0.7; // TIGHT: 狭めて利益を守る

   CPositionInfo pos;
   CTrade trade;
   trade.SetExpertMagicNumber(MagicNumber);
   trade.SetDeviationInPoints(80); // ★追加: 許容スリッページを設定(例: 50ポイント)

   for(int i=PositionsTotal()-1; i>=0; i--) {
      if(pos.SelectByIndex(i) && pos.Magic() == MagicNumber && pos.Symbol() == _Symbol) {
         ulong  ticket = pos.Ticket(); 
         double open   = pos.PriceOpen();
         double sl     = pos.StopLoss();
         double vol    = pos.Volume();
         double price  = (pos.PositionType()==POSITION_TYPE_BUY)?SymbolInfoDouble(_Symbol,SYMBOL_BID):SymbolInfoDouble(_Symbol,SYMBOL_ASK);
         double profit = (pos.PositionType()==POSITION_TYPE_BUY)?(price-open):(open-price);

         // --- 1. 部分利確 (Partial Close) & 建値移動 ---
         // まだSLが建値に移動されていない場合のみ判定
         bool isAlreadyBE = (pos.PositionType()==POSITION_TYPE_BUY) ? (sl >= open) : (sl <= open && sl > 0);
         
         if(EnablePartialClose && !isAlreadyBE && profit > PartialCloseATR * atr) {
            double closeVol = NormalizeLot(vol * (PartialClosePercent / 100.0));
            if(closeVol >= SymbolInfoDouble(_Symbol, SYMBOL_VOLUME_MIN)) {
               if(trade.PositionClosePartial(ticket, closeVol)) {
                  // 部分利確成功後、SLを建値(微益)へ移動
                  double beSL = (pos.PositionType()==POSITION_TYPE_BUY) ? (open + 10*_Point) : (open - 10*_Point);
                  trade.PositionModify(ticket, NormalizeDouble(beSL, _Digits), pos.TakeProfit());
                  Print("[FXAI] Partial Close Done & SL moved to Break-Even");
                  continue; // この足ではトレイルをスキップ
               }
            }
         }

         // --- 2. 動的トレイリングストップ ---
         if(profit > TrailingStartATR * atr) {
            // modeMultiplierを適用して距離を調整
            double targetSL = (pos.PositionType()==POSITION_TYPE_BUY)?(price - TrailingDistATR * atr * modeMultiplier) : (price + TrailingDistATR * atr * modeMultiplier);
            
            if(pos.PositionType()==POSITION_TYPE_BUY) {
               if(targetSL > sl + TrailingStepATR * atr) {
                  trade.PositionModify(ticket, NormalizeDouble(targetSL, _Digits), pos.TakeProfit());
               }
            } else {
               if(sl == 0 || targetSL < sl - TrailingStepATR * atr) {
                  trade.PositionModify(ticket, NormalizeDouble(targetSL, _Digits), pos.TakeProfit());
               }
            }
         }
      }
   }
}

void CloseAll() {
   CPositionInfo p;
   for(int i=PositionsTotal()-1; i>=0; i--) {
      if(p.SelectByIndex(i) && p.Magic()==MagicNumber && p.Symbol()==_Symbol) m_trade.PositionClose(p.Ticket());
   }
}