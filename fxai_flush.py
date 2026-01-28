import time
from typing import Callable


def compute_sleep_sec(interval_sec: float) -> float:
    try:
        return max(0.5, float(interval_sec or 0.0))
    except Exception:
        return 0.5


def should_flush(*, now: float, last_save: float, last_dirty: float, interval_sec: float, force_sec: float) -> bool:
    try:
        if last_dirty > 0 and (now - float(last_dirty)) >= float(interval_sec or 0.0):
            return True
    except Exception:
        pass
    try:
        if (now - float(last_save or 0.0)) >= float(force_sec or 0.0):
            return True
    except Exception:
        pass
    return False


def run_flush_loop(
    *,
    sleep_sec: float,
    is_enabled: Callable[[], bool],
    flush_cache_once: Callable[[], None],
    flush_metrics_once: Callable[[], None],
    warn: Callable[[str], None],
) -> None:
    """Generic background flush loop.

    Caller supplies the per-iteration flush functions which handle their own locking.
    Exceptions are caught and forwarded to warn().
    """

    while True:
        time.sleep(float(sleep_sec or 0.5))
        try:
            if not bool(is_enabled()):
                continue
        except Exception:
            continue

        try:
            flush_cache_once()
        except Exception as e:
            try:
                warn(f"Cache flush loop error: {e}")
            except Exception:
                pass

        try:
            flush_metrics_once()
        except Exception as e:
            try:
                warn(f"Metrics flush loop error: {e}")
            except Exception:
                pass
