from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


def utc_day_key(ts: Optional[float] = None) -> str:
    if ts is None:
        ts = time.time()
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%d")


def metrics_bucket_score(score: int) -> str:
    try:
        s = int(score)
    except Exception:
        s = 0
    if s < 50:
        return "0-49"
    if s < 60:
        return "50-59"
    if s < 70:
        return "60-69"
    if s < 80:
        return "70-79"
    if s < 90:
        return "80-89"
    return "90-100"


def metrics_prune(metrics: Dict[str, Any], *, keep_days: int, now: Optional[float] = None) -> None:
    if now is None:
        now = time.time()
    keep_days_i = max(1, int(keep_days or 14))
    try:
        cutoff = datetime.fromtimestamp(float(now), tz=timezone.utc) - timedelta(days=keep_days_i)
        cutoff_key = cutoff.strftime("%Y-%m-%d")
    except Exception:
        cutoff_key = "0000-00-00"

    by_day = metrics.get("by_day")
    if not isinstance(by_day, dict):
        metrics["by_day"] = {}
        return

    for day in list(by_day.keys()):
        try:
            if str(day) < str(cutoff_key):
                by_day.pop(day, None)
        except Exception:
            continue


def metrics_get_bucket(
    metrics: Dict[str, Any],
    *,
    day_key: str,
    symbol: str,
    default_symbol: str,
) -> Dict[str, Any]:
    by_day = metrics.setdefault("by_day", {})
    if not isinstance(by_day, dict):
        by_day = {}
        metrics["by_day"] = by_day

    day = by_day.setdefault(day_key, {})
    if not isinstance(day, dict):
        day = {}
        by_day[day_key] = day

    sym = (symbol or "").strip().upper() or (default_symbol or "GOLD")
    b = day.setdefault(sym, {})
    if not isinstance(b, dict):
        b = {}
        day[sym] = b

    # initialize common fields
    b.setdefault("webhooks", 0)
    b.setdefault("duplicates", 0)
    b.setdefault("context_updates", 0)
    b.setdefault("entry_triggers", 0)
    b.setdefault("entry_attempts", 0)
    b.setdefault("entry_ok", 0)
    b.setdefault("blocked", {})
    b.setdefault("ai_score_hist", {})
    b.setdefault("guard_stats", {})
    b.setdefault("examples", [])

    # error/health counters (observability)
    b.setdefault("openai_calls", 0)
    b.setdefault("openai_success", 0)
    b.setdefault("openai_fail", 0)
    b.setdefault("openai_attempts", 0)
    b.setdefault("openai_retries", 0)
    b.setdefault("openai_timeouts", 0)
    b.setdefault("openai_calls_by_kind", {})
    b.setdefault("openai_fail_by_kind", {})
    b.setdefault("openai_fail_by_type", {})
    b.setdefault("ai_validation_fail", 0)
    b.setdefault("ai_validation_fail_by_kind", {})

    b.setdefault("zmq_send_ok", 0)
    b.setdefault("zmq_send_fail", 0)
    b.setdefault("zmq_send_by_kind", {})
    b.setdefault("zmq_send_fail_by_type", {})

    return b


def inc(b: Dict[str, Any], key: str, n: int = 1) -> None:
    try:
        b[key] = int(b.get(key) or 0) + int(n)
    except Exception:
        b[key] = int(n)


def inc_map(m: Dict[str, Any], key: str, n: int = 1) -> None:
    if not isinstance(m, dict):
        return
    try:
        m[key] = int(m.get(key) or 0) + int(n)
    except Exception:
        m[key] = int(n)


def update_guard_stat(guard_stats: Dict[str, Any], name: str, value: Optional[float]) -> None:
    if value is None:
        return
    try:
        v = float(value)
    except Exception:
        return
    if not isinstance(guard_stats, dict):
        return
    st = guard_stats.get(name)
    if not isinstance(st, dict):
        st = {"count": 0, "sum": 0.0, "min": None, "max": None}
        guard_stats[name] = st
    st["count"] = int(st.get("count") or 0) + 1
    st["sum"] = float(st.get("sum") or 0.0) + v
    mn = st.get("min")
    mx = st.get("max")
    st["min"] = v if (mn is None or v < float(mn)) else float(mn)
    st["max"] = v if (mx is None or v > float(mx)) else float(mx)


def append_example(examples: List[Dict[str, Any]], ex: Dict[str, Any], *, max_n: int) -> None:
    if not isinstance(examples, list):
        return
    try:
        examples.append(ex)
        mx = max(10, int(max_n or 80))
        if len(examples) > mx:
            del examples[: max(0, len(examples) - mx)]
    except Exception:
        return
