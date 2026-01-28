from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional


def compute_qtrend_anchor_stats(
    *,
    target_symbol: str,
    normalized: List[Dict[str, Any]],
    now: float,
    confluence_window_sec: Any,
    min_other_signals_for_entry: Any,
    zone_lookback_sec: Any,
    zone_touch_lookback_sec: Any,
    confluence_debug: bool,
    confluence_debug_max_lines: Any,
    weight_confirmed: Callable[[Any], float],
) -> Optional[Dict[str, Any]]:
    """Compute Q-Trend anchored confluence stats.

    This function is a behavior-preserving extraction from the main bridge.
    It assumes signals are already normalized and freshness-filtered.
    """

    if not normalized:
        return None

    # Allowed evidence sources under the new spec.
    # NOTE: Q-Trend itself is an anchor, not a confluence evidence.
    # Primary: normalized names used by _normalize_signal_fields.
    # Compatibility: keep older/raw template names to avoid accidental drops.
    allowed_evidence_sources = {"Zones", "FVG", "OSGFC", "ZonesDetector", "LuxAlgo_FVG"}

    # Q-Trend: Normal/Strong が混在する場合は Strong を優先
    q_candidates = []
    for s in normalized:
        if s.get("source") in {"Q-Trend Strong", "Q-Trend", "Q-Trend-Strong", "Q-Trend-Normal"} and s.get("side") in {
            "buy",
            "sell",
        }:
            st = float(s.get("signal_time") or s.get("receive_time") or 0.0)
            is_strong = (s.get("source") in {"Q-Trend Strong", "Q-Trend-Strong"}) or (s.get("strength") == "strong")
            q_candidates.append((st, 1 if is_strong else 0, s))

    latest_q = None
    if q_candidates:
        # sort by time then strong priority
        q_candidates.sort(key=lambda x: (x[0], x[1]))
        latest_q = q_candidates[-1][2]
    if not latest_q:
        return None

    q_time = float(latest_q.get("signal_time") or latest_q.get("receive_time") or now)
    q_side = (latest_q.get("side") or "").lower()
    q_source = (latest_q.get("source") or "")
    q_is_strong = (q_source in {"Q-Trend Strong", "Q-Trend-Strong"}) or (latest_q.get("strength") == "strong")
    q_trigger_type = "Strong" if q_is_strong else "Normal"
    momentum_factor = 1.5 if q_is_strong else 1.0
    is_strong_momentum = bool(q_is_strong)

    confirm_unique_sources = set([q_source or "Q-Trend"])
    opp_unique_sources = set([q_source or "Q-Trend"])
    confirm_signals = 0
    opp_signals = 0
    strong_after_q = q_is_strong

    # 旧EA互換: 反対側のbar_close(=entry/structure)が来たらトリガー無効化
    cancel_due_to_opposite_bar_close = False
    cancel_detail = None

    # richer context for AI (trend filter / S-R / event quality)
    osgfc_latest_side = ""
    osgfc_latest_time = None
    fvg_same = 0
    fvg_opp = 0
    zones_touch_same = 0
    zones_touch_opp = 0
    zones_confirmed_after_q = 0
    zones_confirmed_recent = 0
    weighted_confirm_score = 0.0
    weighted_oppose_score = 0.0

    dbg_rows = [] if confluence_debug else None

    # Zones presence is meaningful even if it happened before Q-Trend.
    # Count recent zone confirmations (within ZONE_LOOKBACK_SEC by receive_time).
    for s in normalized:
        if (s.get("source") == "Zones") and (s.get("signal_type") == "structure") and (s.get("event") == "new_zone_confirmed"):
            rt = float(s.get("receive_time") or 0.0)
            if rt > 0 and (now - rt) <= float(zone_lookback_sec):
                zones_confirmed_recent += 1

    window = max(0, int(confluence_window_sec or 300))
    for s in normalized:
        st = float(s.get("signal_time") or s.get("receive_time") or 0)
        # Source of Truth: Q-Trend ± 5 minutes
        if st < (q_time - window):
            if dbg_rows is not None:
                dbg_rows.append(
                    {
                        "st": st,
                        "src": s.get("source"),
                        "side": s.get("side"),
                        "evt": s.get("event"),
                        "conf": s.get("confirmed"),
                        "sig_type": s.get("signal_type"),
                        "counted": False,
                        "bucket": "SKIP",
                        "reason": "before_pre_window",
                    }
                )
            continue
        if st > (q_time + window):
            if dbg_rows is not None:
                dbg_rows.append(
                    {
                        "st": st,
                        "src": s.get("source"),
                        "side": s.get("side"),
                        "evt": s.get("event"),
                        "conf": s.get("confirmed"),
                        "sig_type": s.get("signal_type"),
                        "counted": False,
                        "bucket": "SKIP",
                        "reason": "after_post_window",
                    }
                )
            continue

        src = s.get("source")
        side = s.get("side")
        event = s.get("event")
        sig_type = s.get("signal_type")
        confirmed = s.get("confirmed")

        try:
            w = float(weight_confirmed(confirmed))
        except Exception:
            w = 0.0

        # touch系は「瞬間イベント」なので合流/逆行の重みを落とす（ノイズ過剰反応防止）
        event_weight = 1.0
        if event in {"fvg_touch", "zone_retrace_touch", "zone_touch"}:
            event_weight = 0.7
        if src in {"Q-Trend Strong", "Q-Trend", "Q-Trend-Strong", "Q-Trend-Normal"}:
            # 同一トレンド内で Strong が追加で来るケースを拾う（後追い含む）
            if src in {"Q-Trend Strong", "Q-Trend-Strong"}:
                strong_after_q = True
                is_strong_momentum = True
                q_trigger_type = "Strong"
                momentum_factor = 1.5
            continue

        # Allowlist: ignore unknown sources to avoid unintended confluence votes.
        if src not in allowed_evidence_sources:
            if dbg_rows is not None:
                dbg_rows.append(
                    {
                        "st": st,
                        "src": src,
                        "side": side,
                        "evt": event,
                        "conf": confirmed,
                        "sig_type": sig_type,
                        "strength": s.get("strength"),
                        "counted": False,
                        "bucket": "SKIP",
                        "reason": "source_not_allowed",
                    }
                )
            continue

        # 旧EA互換: 反対側のbar_closeが来たらキャンセル（レンジ往復ビンタ回避）
        if (
            (not cancel_due_to_opposite_bar_close)
            and st >= q_time
            and (confirmed or "").lower() == "bar_close"
            and side in {"buy", "sell"}
            and side != q_side
            and sig_type in {"entry_trigger", "structure"}
        ):
            cancel_due_to_opposite_bar_close = True
            cancel_detail = {
                "source": src,
                "side": side,
                "signal_type": sig_type,
                "event": event,
                "signal_time": st,
            }

        # OSGFC trend filter is useful even when used as a filter (strength of context)
        if src == "OSGFC" and side in {"buy", "sell"}:
            # keep latest within window
            if osgfc_latest_time is None or st >= float(osgfc_latest_time):
                osgfc_latest_time = st
                osgfc_latest_side = side

        # Zones: neutral confirmations (no side) indicate S/R reliability
        if src == "Zones" and sig_type == "structure" and event == "new_zone_confirmed":
            zones_confirmed_after_q += 1

        # Directional confluence buckets by source/event
        if src == "FVG" and event == "fvg_touch" and side in {"buy", "sell"}:
            if side == q_side:
                fvg_same += 1
            else:
                fvg_opp += 1

        if src == "Zones" and event == "zone_retrace_touch" and side in {"buy", "sell"}:
            if side == q_side:
                zones_touch_same += 1
            else:
                zones_touch_opp += 1

        # Ignore unnamed sources to avoid counting malformed alerts as confluence.
        if not src:
            if dbg_rows is not None:
                dbg_rows.append(
                    {
                        "st": st,
                        "src": src,
                        "side": side,
                        "evt": event,
                        "conf": confirmed,
                        "sig_type": sig_type,
                        "counted": False,
                        "bucket": "SKIP",
                        "reason": "missing_source",
                    }
                )
            continue

        # 合流カウント
        # - bar_close は常に合流OK
        # - intrabar は原則 strength=strong のみ
        #   ただし「Zones/FVGのtouch系」は normal でも合流として数える（直近タッチ→反発→Qトリガーの再現）
        # - trend_filter は原則除外。ただし OSGFC は「1票」として合流に含める
        conf_l = (confirmed or "").lower()
        strength_l = (s.get("strength") or "").lower()
        is_touch_event = (event in {"fvg_touch", "zone_retrace_touch", "zone_touch"})
        is_zone_or_fvg_touch = (src in {"Zones", "FVG"}) and is_touch_event
        intrabar_ok = (conf_l == "intrabar") and (strength_l == "strong" or is_zone_or_fvg_touch)
        confluence_ok = (conf_l == "bar_close") or intrabar_ok
        exclude_from_confluence_count = (sig_type == "trend_filter") and (src != "OSGFC")

        counted = False
        bucket = "SKIP"
        reason = ""

        if side == q_side:
            if confluence_ok and (not exclude_from_confluence_count):
                confirm_signals += 1
                confirm_unique_sources.add(src)
                counted = True
                bucket = "CONFIRM"
                reason = "counted"
            weighted_confirm_score += (w * event_weight)
            if s.get("strength") == "strong":
                strong_after_q = True
            if dbg_rows is not None and (not counted):
                if exclude_from_confluence_count:
                    reason = "excluded_trend_filter"
                elif not confluence_ok:
                    reason = "not_confluence_ok"
                else:
                    reason = "same_side_not_counted"
        elif side in {"buy", "sell"}:
            if confluence_ok and (not exclude_from_confluence_count):
                opp_signals += 1
                opp_unique_sources.add(src)
                counted = True
                bucket = "OPPOSE"
                reason = "counted"
            weighted_oppose_score += (w * event_weight)
            if dbg_rows is not None and (not counted):
                if exclude_from_confluence_count:
                    reason = "excluded_trend_filter"
                elif not confluence_ok:
                    reason = "not_confluence_ok"
                else:
                    reason = "opp_side_not_counted"
        else:
            if dbg_rows is not None:
                reason = "no_side"

        if dbg_rows is not None:
            dbg_rows.append(
                {
                    "st": st,
                    "src": src,
                    "side": side,
                    "evt": event,
                    "conf": confirmed,
                    "sig_type": sig_type,
                    "strength": s.get("strength"),
                    "counted": bool(counted),
                    "bucket": bucket,
                    "reason": reason,
                }
            )

    if dbg_rows is not None:
        try:
            dbg_rows_sorted = sorted(dbg_rows, key=lambda r: float(r.get("st") or 0.0))
        except Exception:
            dbg_rows_sorted = dbg_rows
        print(
            "[FXAI][CONFLUENCE][DBG] "
            + json.dumps(
                {
                    "symbol": target_symbol,
                    "q_time": q_time,
                    "q_side": q_side,
                    "window_sec": int(window),
                    "min_other_needed": int(min_other_signals_for_entry or 0),
                    "confirm_unique_sources": max(0, len(confirm_unique_sources) - 1),
                    "confirm_signals": confirm_signals,
                    "opp_unique_sources": max(0, len(opp_unique_sources) - 1),
                    "opp_signals": opp_signals,
                    "lines": dbg_rows_sorted[: max(1, int(confluence_debug_max_lines or 80))],
                    "truncated": len(dbg_rows_sorted) > max(1, int(confluence_debug_max_lines or 80)),
                },
                ensure_ascii=False,
            )
        )

    return {
        "q_time": q_time,
        "q_side": q_side,
        "q_source": q_source,
        "q_is_strong": q_is_strong,
        "q_trigger_type": q_trigger_type,
        "momentum_factor": momentum_factor,
        "is_strong_momentum": is_strong_momentum,
        "confirm_unique_sources": max(0, len(confirm_unique_sources) - 1),
        "confirm_signals": confirm_signals,
        "opp_unique_sources": max(0, len(opp_unique_sources) - 1),
        "opp_signals": opp_signals,
        "cancel_due_to_opposite_bar_close": cancel_due_to_opposite_bar_close,
        "cancel_detail": cancel_detail,
        "strong_after_q": strong_after_q,
        "osgfc_latest_side": osgfc_latest_side,
        "osgfc_latest_time": osgfc_latest_time,
        "fvg_touch_same": fvg_same,
        "fvg_touch_opp": fvg_opp,
        "zones_touch_same": zones_touch_same,
        "zones_touch_opp": zones_touch_opp,
        "zones_confirmed_after_q": zones_confirmed_after_q,
        "zones_confirmed_recent": zones_confirmed_recent,
        "weighted_confirm_score": round(weighted_confirm_score, 3),
        "weighted_oppose_score": round(weighted_oppose_score, 3),
    }
