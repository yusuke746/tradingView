from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


def build_window_signals_payload(
    *,
    snapshot: List[Dict[str, Any]],
    symbol: str,
    center_ts: float,
    trigger_side: str,
    window_sec: float,
    is_qtrend_source: Callable[[Any], bool],
) -> Dict[str, Any]:
    """Build a compact ±window payload around a trigger time.

    This is a behavior-preserving extraction from the main bridge.
    Caller is responsible for providing `snapshot` (a list of dict signals).
    """

    sym = (symbol or "").strip().upper()
    if not sym:
        return {
            "center_ts": float(center_ts or 0.0),
            "window_sec": float(window_sec or 0.0),
            "aligned": [],
            "opposed": [],
            "neutral": [],
        }

    try:
        center = float(center_ts)
    except Exception:
        center = float(center_ts or 0.0)

    try:
        w = float(window_sec)
    except Exception:
        w = 300.0
    if w <= 0:
        w = 300.0

    trig_side = (trigger_side or "").strip().lower()

    def _sig_ts(s: Dict[str, Any]) -> float:
        try:
            return float(s.get("signal_time") or s.get("receive_time") or 0.0)
        except Exception:
            return 0.0

    window_raw: List[Dict[str, Any]] = []
    for s in snapshot:
        try:
            if (s.get("symbol") or "").strip().upper() != sym:
                continue
        except Exception:
            continue
        st = _sig_ts(s)
        if st <= 0:
            continue
        if abs(st - center) > w:
            continue
        window_raw.append(s)

    window_raw.sort(key=_sig_ts)

    # Strict allow-listing to keep the window payload high-signal.
    # User requirement: include Q-Trend (context) in the ±5min window.
    allowed_sources = {"Q-Trend", "Q-Trend Strong", "Zones", "FVG"}
    allowed_events_by_source: Dict[str, Optional[set]] = {
        # Q-Trend context may not have a strong event taxonomy; allow any.
        "Q-Trend": None,
        "Q-Trend Strong": None,
        # Zones: keep only key touch/structure events.
        "Zones": {"zone_retrace_touch", "zone_touch", "new_zone_confirmed", "zone_confirmed"},
        # FVG: keep touch only.
        "FVG": {"fvg_touch"},
    }

    # De-duplicate by (source,event,side) keeping the latest by time.
    best_by_key: Dict[tuple, Dict[str, Any]] = {}

    for s in window_raw:
        src = (s.get("source") or "")
        evt = (s.get("event") or "")
        side = (s.get("side") or "").strip().lower()
        st = _sig_ts(s)

        if not src:
            continue

        # Normalize Q-Trend variants that might slip in.
        try:
            if is_qtrend_source(src):
                src = (
                    "Q-Trend Strong"
                    if (str(s.get("strength") or "").lower() == "strong" or "strong" in str(src).lower())
                    else "Q-Trend"
                )
        except Exception:
            pass

        if src not in allowed_sources:
            continue

        allowed_events = allowed_events_by_source.get(src)
        if isinstance(allowed_events, set):
            if (evt or "").strip().lower() not in allowed_events:
                continue

        # Avoid duplicating the Lorentzian trigger itself in the window list.
        if src == "Lorentzian" and side == trig_side and abs(st - center) <= 1.0:
            continue

        compact = {
            "source": src,
            "event": evt,
            "side": side or None,
            "signal_type": s.get("signal_type"),
            "strength": s.get("strength"),
            "confirmed": s.get("confirmed"),
            "signal_time": st,
        }

        key = (compact.get("source"), compact.get("event"), compact.get("side"))
        prev = best_by_key.get(key)
        if (prev is None) or (float(compact.get("signal_time") or 0.0) >= float(prev.get("signal_time") or 0.0)):
            best_by_key[key] = compact

    # Split into aligned/opposed/neutral after de-dup.
    aligned: List[Dict[str, Any]] = []
    opposed: List[Dict[str, Any]] = []
    neutral: List[Dict[str, Any]] = []

    for compact in sorted(best_by_key.values(), key=lambda x: float(x.get("signal_time") or 0.0)):
        side = (compact.get("side") or "")
        if side in {"buy", "sell"} and trig_side in {"buy", "sell"}:
            (aligned if side == trig_side else opposed).append(compact)
        else:
            neutral.append(compact)

    # Hard cap payload sizes to keep prompts stable.
    aligned = aligned[-30:]
    opposed = opposed[-30:]
    neutral = neutral[-20:]

    return {
        "center_ts": float(center),
        "window_sec": float(w),
        "trigger_side": trig_side,
        "aligned": aligned,
        "opposed": opposed,
        "neutral": neutral,
        "counts": {"aligned": len(aligned), "opposed": len(opposed), "neutral": len(neutral)},
    }
