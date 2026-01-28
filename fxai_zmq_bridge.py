from typing import Any, Callable, Dict, Optional


def send_json(socket: Any, payload: Dict[str, Any]) -> None:
    """Thin wrapper around ZMQ socket.send_json."""
    socket.send_json(payload)


def send_json_with_hooks(
    socket: Any,
    payload: Dict[str, Any],
    *,
    on_ok: Optional[Callable[[], None]] = None,
    on_error: Optional[Callable[[Exception], None]] = None,
) -> None:
    """Send JSON and optionally invoke hooks.

    Notes:
    - Re-raises the original exception so caller behavior stays identical.
    """
    try:
        send_json(socket, payload)
        if on_ok is not None:
            on_ok()
    except Exception as e:
        if on_error is not None:
            try:
                on_error(e)
            except Exception:
                pass
        raise
