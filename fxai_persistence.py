import json
import os
from typing import Any, Optional


def read_json_if_exists(path: str, *, default: Any = None, encoding: str = "utf-8") -> Any:
    if not path:
        return default
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding=encoding) as f:
            return json.load(f)
    except Exception:
        return default


def atomic_write_json(path: str, data: Any, *, ensure_ascii: bool = False, encoding: str = "utf-8") -> Optional[str]:
    """Write JSON to path using temp file + os.replace.

    Returns error string on failure, else None.
    """
    if not path:
        return "empty_path"
    try:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding=encoding) as f:
            json.dump(data, f, ensure_ascii=ensure_ascii)
        os.replace(tmp, path)
        return None
    except Exception as e:
        return str(e)
