from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple


def is_zone_presence_signal(s: Dict[str, Any]) -> bool:
    src = (s.get("source") or "").strip().lower()
    event = (s.get("event") or "").strip().lower()
    sig_type = (s.get("signal_type") or "").strip().lower()

    src_norm = src.replace(" ", "").replace("_", "")
    is_zones = (src_norm in {"zones", "zonesdetector"}) or ("zone" in src_norm)
    if not is_zones:
        return False

    zone_presence_events = {
        "new_zone_confirmed",
        "zone_confirmed",
        "new_zone",
        "zone_created",
        "zone_breakout",
    }
    return (event in zone_presence_events) or (sig_type == "structure" and event in zone_presence_events)


def is_zone_touch_signal(s: Dict[str, Any]) -> bool:
    src = (s.get("source") or "").strip().lower()
    event = (s.get("event") or "").strip().lower()
    src_norm = src.replace(" ", "").replace("_", "")
    is_zones = (src_norm in {"zones", "zonesdetector"}) or ("zone" in src_norm)
    if not is_zones:
        return False
    # match the canonical touch events used elsewhere
    return event in {"zone_retrace_touch", "zone_touch"} or ("touch" in event)


def is_fvg_signal(s: Dict[str, Any]) -> bool:
    """Detect FVG signals (15-min timeframe support).
    
    Uses strict allowlist to prevent false positives from partial matches.
    """
    src = (s.get("source") or "").strip().lower()
    src_norm = src.replace(" ", "").replace("_", "")
    # Strict allowlist: only known FVG sources
    return src_norm in {"fvg", "luxalgofvg"}


# Default FVG lookback: 20 minutes to cover 15-min candle context.
DEFAULT_FVG_LOOKBACK_SEC = 1200


def prune_signals_cache(
    *,
    signals_cache: List[Dict[str, Any]],
    now: float,
    zone_lookback_sec: Any,
    zone_touch_lookback_sec: Any,
    fvg_lookback_sec: Any,
    signal_lookback_sec: Any,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Return (kept_signals, changed).

    Behavior is intended to match the inlined logic in the main bridge file.
    Caller is responsible for holding any locks and for updating indexes.
    """

    keep_list: List[Dict[str, Any]] = []

    for s in signals_cache:
        src_raw = (s.get("source") or "").strip().lower()
        event = (s.get("event") or "").strip().lower()
        sig_type = (s.get("signal_type") or "").strip().lower()

        src_norm = src_raw.replace(" ", "").replace("_", "")
        is_zones = (src_norm in {"zones", "zonesdetector"}) or ("zone" in src_norm)

        # Zoneの「構造/存在」イベント（長期保持）
        zone_presence_events = {
            "new_zone_confirmed",
            "zone_confirmed",
            "new_zone",
            "zone_created",
            "zone_breakout",
        }

        # Zoneの「接触」イベント（短期保持）
        zone_touch_markers = {
            "zone_retrace_touch",
            "zone_touch",
            "touch",
            "retrace",
            "bounce",
        }

        rt = float(s.get("receive_time", now) or now)
        age = now - rt

        if is_zones:
            if (sig_type in {"structure", "zones", "zone"} and event in zone_presence_events) or event in zone_presence_events:
                limit_sec = zone_lookback_sec
                if age < float(limit_sec or zone_lookback_sec):
                    keep_list.append(s)
                continue
            if any(m in event for m in zone_touch_markers):
                limit_sec = zone_touch_lookback_sec
                if age < float(limit_sec or zone_touch_lookback_sec):
                    keep_list.append(s)
                continue

            # Unknown Zones-like events: keep short to avoid false confluence.
            limit_sec = signal_lookback_sec
            if age < float(limit_sec or signal_lookback_sec):
                keep_list.append(s)
            continue

        # FVG: keep longer to cover 15-min candle context.
        if is_fvg_signal(s):
            limit_sec = float(fvg_lookback_sec or DEFAULT_FVG_LOOKBACK_SEC)
        else:
            # Non-Zones/FVG: simple time-based retention (no Q-Trend anchoring).
            # We keep recent evidence so Lorentzian triggers can reference it.
            limit_sec = float(signal_lookback_sec or 1200)
        if age < limit_sec:
            keep_list.append(s)

    changed = (len(keep_list) != len(signals_cache))
    return keep_list, changed


def filter_fresh_signals_from_normalized(
    *,
    normalized: List[Dict[str, Any]],
    now: float,
    signal_max_age_sec: Any,
    zone_lookback_sec: Any,
    zone_touch_lookback_sec: Any,
    fvg_lookback_sec: Any,
) -> List[Dict[str, Any]]:
    """Filter a normalized list by freshness.

    Caller is responsible for building the normalized list.
    """

    fresh: List[Dict[str, Any]] = []
    for s in normalized:
        # Zones presence signals are intentionally long-lived (structure context).
        # Use receive_time-based retention rather than signal_time freshness.
        if is_zone_presence_signal(s):
            rt = float(s.get("receive_time") or 0.0)
            if rt > 0 and (now - rt) <= float(zone_lookback_sec):
                fresh.append(s)
            continue

        # Zones touch is a momentary event; keep it short-lived.
        # Prefer receive_time for zone touch (reliable for touch events).
        if is_zone_touch_signal(s):
            rt = float(s.get("receive_time") or 0.0)
            if rt > 0:  # receive_time available: use it exclusively
                if (now - rt) <= float(zone_touch_lookback_sec):
                    fresh.append(s)
            # If receive_time missing/invalid: skip signal (zone touch requires receive_time)
            continue

        st = float(s.get("signal_time") or 0.0)
        if st <= 0:
            continue
        age = now - st
        
        # FVG uses dedicated lookback (typically longer than other signals for 15-min TF).
        if is_fvg_signal(s):
            if abs(age) > float(fvg_lookback_sec or DEFAULT_FVG_LOOKBACK_SEC):
                continue
        # Non-FVG signals use standard max_age_sec.
        elif abs(age) > signal_max_age_sec:
            # future/old both drop（v2.6 DebugReplayCsv の代替はここでは省略）
            continue
        
        fresh.append(s)

    return fresh


def stable_round_time(t: Optional[float], resolution_sec: float = 1.0) -> Optional[float]:
    try:
        if t is None:
            return None
        tt = float(t)
        if resolution_sec <= 0:
            return tt
        return round(tt / float(resolution_sec)) * float(resolution_sec)
    except Exception:
        return None


def signal_dedupe_key(s: Dict[str, Any]) -> str:
    """Create a stable de-duplication key from a (preferably normalized) signal."""
    symbol = (s.get("symbol") or "").strip().upper()
    source = (s.get("source") or "").strip()
    event = (s.get("event") or "").strip().lower()
    sig_type = (s.get("signal_type") or "").strip().lower()
    confirmed = (s.get("confirmed") or "").strip().lower()
    side = (s.get("side") or "").strip().lower()

    t = s.get("signal_time")
    if t is None:
        t = s.get("receive_time")
    t_rounded = stable_round_time(t, 1.0)
    if t_rounded is None:
        t_rounded = 0.0

    return f"{symbol}|{source}|{event}|{sig_type}|{confirmed}|{side}|{t_rounded:.0f}"


def signal_ts_for_index(s: Dict[str, Any]) -> float:
    try:
        return float(s.get("signal_time") or s.get("receive_time") or 0.0)
    except Exception:
        return 0.0


def rebuild_signal_indexes(
    *,
    signals_cache: List[Dict[str, Any]],
    bucket_sec: int,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Dict[int, List[Dict[str, Any]]]]]:
    """Rebuild optional signal cache indexes.

    Caller is responsible for holding any locks.
    """

    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    buckets_by_symbol: Dict[str, Dict[int, List[Dict[str, Any]]]] = {}

    bs = int(bucket_sec or 60)
    if bs <= 0:
        bs = 60

    for s in signals_cache:
        if not isinstance(s, dict):
            continue
        sym = (s.get("symbol") or "").strip().upper()
        if not sym:
            continue

        by_symbol.setdefault(sym, []).append(s)

        st = signal_ts_for_index(s)
        if st <= 0:
            continue
        b = int(st // float(bs))
        sym_b = buckets_by_symbol.get(sym)
        if sym_b is None:
            sym_b = {}
            buckets_by_symbol[sym] = sym_b
        sym_b.setdefault(b, []).append(s)

    return by_symbol, buckets_by_symbol


def ensure_signal_indexes(
    *,
    signal_index_enabled: bool,
    signals_cache: List[Dict[str, Any]],
    signals_by_symbol: Dict[str, List[Dict[str, Any]]],
    bucket_sec: int,
) -> Optional[Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Dict[int, List[Dict[str, Any]]]]]]:
    """Lazy build indexes if enabled and missing.

    Returns rebuilt indexes if performed, else None.
    """

    if not signal_index_enabled:
        return None
    if signals_by_symbol:
        return None
    if not signals_cache:
        return None
    return rebuild_signal_indexes(signals_cache=signals_cache, bucket_sec=bucket_sec)


def append_signal_dedup(
    *,
    signals_cache: List[Dict[str, Any]],
    signal: Dict[str, Any],
    dedupe_window_sec: float,
    signal_index_enabled: bool,
    bucket_sec: int,
    signals_by_symbol: Dict[str, List[Dict[str, Any]]],
    signals_buckets_by_symbol: Dict[str, Dict[int, List[Dict[str, Any]]]],
) -> bool:
    """Append a signal into cache with de-duplication.

    Mutates signals_cache and (when enabled) the passed-in index maps.
    """

    if not isinstance(signal, dict):
        return False

    now = time.time()
    key = signal_dedupe_key(signal)

    for prev in reversed(signals_cache):
        try:
            prev_key = signal_dedupe_key(prev)
            if prev_key != key:
                continue
            prt = float(prev.get("receive_time") or 0.0)
            if prt > 0 and (now - prt) <= float(dedupe_window_sec or 0.0):
                return False
            return False
        except Exception:
            continue

    signals_cache.append(signal)

    if signal_index_enabled:
        sym = (signal.get("symbol") or "").strip().upper()
        if sym:
            signals_by_symbol.setdefault(sym, []).append(signal)
            st = signal_ts_for_index(signal)
            if st > 0:
                bs = int(bucket_sec or 60)
                if bs <= 0:
                    bs = 60
                b = int(st // float(bs))
                sym_b = signals_buckets_by_symbol.get(sym)
                if sym_b is None:
                    sym_b = {}
                    signals_buckets_by_symbol[sym] = sym_b
                sym_b.setdefault(b, []).append(signal)

    return True
