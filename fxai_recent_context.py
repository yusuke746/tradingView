from __future__ import annotations

from typing import Any, Dict, List, Optional


def compute_recent_context_signals(
    *,
    normalized: List[Dict[str, Any]],
    now: float,
    zone_lookback_sec: Any,
) -> Dict[str, Any]:
    """Compute compact recent Zones/FVG context.

    Behavior-preserving extraction from the main bridge.
    `normalized` should already be normalized + freshness-filtered.
    """

    zones_confirmed_recent = 0
    latest_zone_touch: Optional[Dict[str, Any]] = None
    latest_fvg_touch: Optional[Dict[str, Any]] = None

    # We also keep a small list of recent context events for transparency/debugging.
    recent_events: List[Dict[str, Any]] = []

    for s in sorted(
        normalized,
        key=lambda x: float(x.get("signal_time") or x.get("receive_time") or 0.0),
        reverse=True,
    ):
        src = s.get("source")
        evt = s.get("event")
        side = s.get("side")
        st = float(s.get("signal_time") or s.get("receive_time") or 0.0)

        if src == "Zones" and (s.get("signal_type") == "structure") and (evt == "new_zone_confirmed"):
            rt = float(s.get("receive_time") or 0.0)
            if rt > 0 and (now - rt) <= float(zone_lookback_sec):
                zones_confirmed_recent += 1

        if src == "Zones" and evt in {"zone_retrace_touch", "zone_touch"} and side in {"buy", "sell"}:
            if latest_zone_touch is None or st > float(latest_zone_touch.get("signal_time") or 0.0):
                latest_zone_touch = {
                    "side": side,
                    "event": evt,
                    "signal_time": st,
                    "confirmed": s.get("confirmed"),
                    "strength": s.get("strength"),
                }

        if src == "FVG" and evt == "fvg_touch" and side in {"buy", "sell"}:
            if latest_fvg_touch is None or st > float(latest_fvg_touch.get("signal_time") or 0.0):
                latest_fvg_touch = {
                    "side": side,
                    "event": evt,
                    "signal_time": st,
                    "confirmed": s.get("confirmed"),
                    "strength": s.get("strength"),
                }

        if len(recent_events) < 12:
            if src in {"Zones", "FVG"}:
                recent_events.append(
                    {
                        "source": src,
                        "event": evt,
                        "side": side,
                        "signal_time": st,
                        "confirmed": s.get("confirmed"),
                        "strength": s.get("strength"),
                        "signal_type": s.get("signal_type"),
                    }
                )

    return {
        "zones_confirmed_recent": int(zones_confirmed_recent),
        "latest_zone_touch": latest_zone_touch,
        "latest_fvg_touch": latest_fvg_touch,
        "recent_events": list(recent_events),
    }
