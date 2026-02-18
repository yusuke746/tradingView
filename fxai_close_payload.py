from __future__ import annotations

from typing import Any, Dict, List, Optional


def build_close_logic_payload(
    *,
    symbol: str,
    phase_name: str,
    breakeven_band_points: float,
    profit_protect_threshold_points: float,
    holding_sec: float,
    move_points_pts: Optional[float],
    is_breakeven_like: bool,
    in_profit_protect: bool,
    in_development: bool,
    pos_summary: Dict[str, Any],
    net_avg_open: float,
    move_points_price_delta: float,
    q_age_sec: int,
    stats: Dict[str, Any],
    market: Dict[str, Any],
    zones_context: Optional[Dict[str, Any]],
    sma_context: Optional[Dict[str, Any]],
    volatility_context: Optional[Dict[str, Any]],
    spread_context: Optional[Dict[str, Any]],
    session_context: Optional[Dict[str, Any]],
    latest_signal: Dict[str, Any],
    recent_signals_clean: List[Dict[str, Any]],
    min_close_confidence: Optional[int] = None,
) -> Dict[str, Any]:
    """Build the ContextJSON payload for CLOSE/HOLD prompt.

    Behavior-preserving extraction from the main bridge.
    Callers own all computations; this function only assembles the dict.
    """

    payload: Dict[str, Any] = {
        "symbol": symbol,
        "mode": "POSITION_MANAGEMENT",
        "phase": {
            "name": phase_name,
            "rules": {
                "development_holding_sec_lt": 15 * 60,
                "breakeven_band_points": round(float(breakeven_band_points), 3),
                "profit_protect_threshold_points": round(float(profit_protect_threshold_points), 3),
                "phase1_close_confidence_min_hint": 80,
                "notes": "Phase is a hint for AI behavior. Use POSITION + MARKET context to decide; do not override hard risk realities.",
            },
            "inputs": {
                "holding_sec": round(float(holding_sec), 3),
                "move_points": round(float(move_points_pts), 3) if move_points_pts is not None else None,
                "is_breakeven_like": bool(is_breakeven_like),
                "in_profit_protect": bool(in_profit_protect),
                "in_development": bool(in_development),
                "note": "Phase is computed from points/ATR/spread/time only; do not infer phase from money PnL.",
            },
        },
        "position": {
            "positions_open": int(pos_summary.get("positions_open") or 0),
            "net_side": pos_summary.get("net_side"),
            "net_volume": pos_summary.get("net_volume"),
            "total_profit": pos_summary.get("total_profit"),
            "max_holding_sec": pos_summary.get("max_holding_sec"),
            "buy_count": int(pos_summary.get("buy_count") or 0),
            "sell_count": int(pos_summary.get("sell_count") or 0),
            "buy_avg_open": pos_summary.get("buy_avg_open"),
            "sell_avg_open": pos_summary.get("sell_avg_open"),
            "buy_profit": pos_summary.get("buy_profit"),
            "sell_profit": pos_summary.get("sell_profit"),
            "net_avg_open": round(float(net_avg_open), 6),
            # Price delta from avg open to current mid (units: price, NOT broker points).
            "net_move_price_delta": round(float(move_points_price_delta), 6),
            # Preferred: move in broker points (positive means favorable move for the current net_side).
            "net_move_points": round(float(move_points_pts), 3) if move_points_pts is not None else None,
        },
        "qtrend_context": {
            "latest_q_side": stats.get("q_side"),
            "latest_q_age_sec": int(q_age_sec),
            "latest_q_source": stats.get("q_source"),
            "trigger_type": stats.get("q_trigger_type"),
            "is_strong_momentum": bool(stats.get("is_strong_momentum")),
            "momentum_factor": stats.get("momentum_factor"),
            "confirm_unique_sources": int(stats.get("confirm_unique_sources") or 0),
            "oppose_unique_sources": int(stats.get("opp_unique_sources") or 0),
            "weighted_confirm_score": float(stats.get("weighted_confirm_score") or 0.0),
            "weighted_oppose_score": float(stats.get("weighted_oppose_score") or 0.0),
            "fvg_touch_opp": int(stats.get("fvg_touch_opp") or 0),
            "zones_touch_opp": int(stats.get("zones_touch_opp") or 0),
            "zones_confirmed_recent": int(stats.get("zones_confirmed_recent") or stats.get("zones_confirmed") or 0),
            "zones_confirmed_after_q": int(stats.get("zones_confirmed_after_q") or 0),
        },
        "market": {
            "bid": market.get("bid"),
            "ask": market.get("ask"),
            "m15_sma20": market.get("m15_ma"),
            "atr_m5_approx": market.get("atr"),
            "spread_points": market.get("spread"),
        },
        "zones_context": zones_context,
        "sma_context": sma_context,
        "volatility_context": volatility_context,
        "spread_context": spread_context,
        "session_context": session_context,
        "latest_signal": {
            "source": latest_signal.get("source"),
            "side": latest_signal.get("side"),
            "event": latest_signal.get("event"),
            "signal_type": latest_signal.get("signal_type"),
            "confirmed": latest_signal.get("confirmed"),
            "signal_time": latest_signal.get("signal_time"),
        },
        "recent_signals": recent_signals_clean,
        "recent_signals_count": int(len(recent_signals_clean)),
        "constraints": {
            "close_confidence_range": [0, 100],
            "min_close_confidence": min_close_confidence,  # 呼び出し元で int 変換済み
            "trail_mode_range": ["WIDE", "NORMAL", "TIGHT"],
            "tp_mode_range": ["WIDE", "NORMAL", "TIGHT"],
            "note": "System will CLOSE only when confidence >= min_close_confidence; otherwise HOLD. trail_mode scales SLs dynamically. tp_mode scales TPs dynamically.",
        },
        "tp_mode_guide": {
            "WIDE": {"description": "Aggressive (10x+ ATR target)", "use_when": "Strong momentum, low opp, SMA<3.0"},
            "NORMAL": {"description": "Balanced (5-7x ATR target)", "use_when": "Most situations, healthy conditions"},
            "TIGHT": {"description": "Defensive (2-3x ATR target)", "use_when": "Extended SMA >3.0, high opp, chop detected"},
            "note": "trail_mode WIDE + tp_mode TIGHT = rare combo (strong momentum but fragile structure)",
        },
        "notes": {
            "opposition_handling": "Opposition signals can be noise. Prioritize confirmed/structural reversal signs (e.g., strong opposite Zones context) over single touch events.",
            "day_trading_goal": "Loss-cut small, Profit-target large. If profit is available and reversal risk rises, take profit. If loss grows and reversal evidence increases, exit early.",
            "phase_1_exception": "Even in Phase 1 (Development), if sma_context.distance_atr_ratio > 4.0 AND momentum weakens, raise confidence to 70+ and set tp_mode=TIGHT. Do NOT blindly HOLD when SMA is overextended.",
        },
    }

    return payload
