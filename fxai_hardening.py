import os
import time
from threading import Lock
from typing import Any, Dict, Optional, Union


def _env_get(env: Optional[Dict[str, Any]], name: str, default: str) -> Any:
    if isinstance(env, dict):
        return env.get(name, default)
    return os.getenv(name, default)


def _parse_env_args(env_or_name: Union[Dict[str, Any], str], name_or_default: str, default: str) -> tuple[Optional[Dict[str, Any]], str, str]:
    """Parse backwards-compatible env function signatures."""
    if isinstance(env_or_name, dict):
        return env_or_name, str(name_or_default), str(default)
    return None, str(env_or_name), str(name_or_default)


def env_bool(env_or_name: Union[Dict[str, Any], str], name_or_default: str = "0", default: str = "0") -> bool:
    # Backwards compatible signatures:
    # - env_bool(name, default)
    # - env_bool(env_dict, name, default)
    env, name, dflt = _parse_env_args(env_or_name, name_or_default, default)
    v = _env_get(env, name, dflt)
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def env_int(env_or_name: Union[Dict[str, Any], str], name_or_default: str = "0", default: str = "0") -> int:
    # Backwards compatible signatures:
    # - env_int(name, default)
    # - env_int(env_dict, name, default)
    env, name, dflt = _parse_env_args(env_or_name, name_or_default, default)
    v = _env_get(env, name, dflt)
    try:
        return int(str(v).strip())
    except (ValueError, AttributeError):
        try:
            return int(float(str(v).strip()))
        except (ValueError, AttributeError):
            try:
                return int(str(dflt).strip())
            except (ValueError, AttributeError):
                return 0


def sanitize_untrusted_text(value: Any, *, max_len: int = 160) -> str:
    """Sanitize untrusted text (e.g., TradingView fields) before logging/prompt inclusion.

    Goal: reduce prompt-injection surface and prevent log flooding.
    Keep semantics for expected short tokens (source/event/signal_type/etc).
    """

    if value is None:
        return ""
    
    s = str(value)
    # Remove NULLs and control characters; normalize whitespace.
    s = s.replace("\x00", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = "".join(ch for ch in s if ch.isprintable())
    s = " ".join(s.split())
    
    return s[:max_len] if max_len > 0 and len(s) > max_len else s


def request_is_https(req) -> bool:
    try:
        if bool(getattr(req, "is_secure", False)):
            return True
    except Exception:
        pass
    try:
        proto = (req.headers.get("X-Forwarded-Proto") or req.headers.get("X-Forwarded-Protocol") or "").strip().lower()
        if proto == "https":
            return True
    except Exception:
        pass
    return False


def get_client_ip(req) -> str:
    # Prefer X-Forwarded-For when behind a proxy; take the left-most.
    try:
        xff = (req.headers.get("X-Forwarded-For") or "").strip()
        if xff:
            ip = xff.split(",", 1)[0].strip()
            if ip:
                return ip
    except Exception:
        pass
    try:
        return str(getattr(req, "remote_addr", "") or "")
    except Exception:
        return ""


_rate_limit_lock = Lock()
_rate_limit_state: Dict[str, Dict[str, Any]] = {}
_RATE_LIMIT_MAX_KEYS = 10000  # Prevent unbounded memory growth


def rate_limit_allow(key: str, *, limit_per_min: int) -> bool:
    """Simple in-memory rate limiter (fixed 60s window).

    Disabled when limit_per_min <= 0.
    Automatically cleans up stale entries to prevent memory leak.
    """

    try:
        lim = int(limit_per_min)
    except (ValueError, TypeError):
        lim = 0
    if lim <= 0:
        return True

    now = time.time()
    with _rate_limit_lock:
        # Memory protection: clean up stale entries if too many keys
        if len(_rate_limit_state) > _RATE_LIMIT_MAX_KEYS:
            _rate_limit_state.clear()
        
        st = _rate_limit_state.get(key)
        if st is None or not isinstance(st, dict):
            _rate_limit_state[key] = {"start": now, "count": 1}
            return True
        
        start = float(st.get("start", 0.0))
        # Reset window if expired
        if start <= 0.0 or (now - start) >= 60.0:
            _rate_limit_state[key] = {"start": now, "count": 1}
            return True
        
        # Increment and check limit
        count = int(st.get("count", 0)) + 1
        st["count"] = count
        return count <= lim
