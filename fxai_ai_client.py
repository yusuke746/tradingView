import json
import time
from typing import Any, Dict, Optional, Tuple


def call_openai_json_with_retry(
    *,
    client: Any,
    model: str,
    prompt: str,
    timeout_sec: float,
    retry_count: int,
    retry_wait_sec: float,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, int], int, int, Optional[Exception]]:
    """Call OpenAI chat.completions and parse JSON response.

    Returns: (data_or_none, err_counts_by_exc_name, timeout_attempts, attempts, last_err)

    Notes:
    - Strips ```json fences if present.
    - Adds _openai_response_id and _ai_latency_ms when possible.
    - Does NOT log; caller decides logging/metrics.
    """

    attempts = 0
    timeout_attempts = 0
    err_counts: Dict[str, int] = {}
    last_err: Optional[Exception] = None

    if client is None:
        return None, err_counts, timeout_attempts, attempts, None

    for i in range(max(1, int(retry_count))):
        attempts += 1
        try:
            t0 = time.time()
            res = client.chat.completions.create(
                model=model,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": "You are a strict trading engine. Output ONLY JSON."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                timeout=timeout_sec,
                store=True,
            )
            raw_content = (res.choices[0].message.content or "").strip()
            if raw_content.startswith("```"):
                raw_content = raw_content.replace("```json", "").replace("```", "").strip()

            data = json.loads(raw_content)
            if isinstance(data, dict):
                try:
                    data["_openai_response_id"] = getattr(res, "id", None)
                except Exception:
                    data["_openai_response_id"] = None
                try:
                    data["_ai_latency_ms"] = int(round((time.time() - t0) * 1000.0))
                except Exception:
                    data["_ai_latency_ms"] = None
            return data if isinstance(data, dict) else None, err_counts, timeout_attempts, attempts, None
        except Exception as e:
            last_err = e
            try:
                name = type(e).__name__
                err_counts[name] = int(err_counts.get(name) or 0) + 1
            except Exception:
                pass

            try:
                if "timeout" in type(e).__name__.lower():
                    timeout_attempts += 1
                elif isinstance(e, TimeoutError):
                    timeout_attempts += 1
            except Exception:
                pass

            if i < (max(1, int(retry_count)) - 1):
                time.sleep(max(0.0, float(retry_wait_sec)))

    return None, err_counts, timeout_attempts, attempts, last_err
