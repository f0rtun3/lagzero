from __future__ import annotations

import time
from collections.abc import Callable


def retry_with_backoff(
    operation: Callable[[], None],
    *,
    max_retries: int,
    base_delay_sec: float = 0.25,
) -> None:
    attempt = 0
    while True:
        try:
            operation()
            return
        except Exception:
            attempt += 1
            if attempt > max_retries:
                raise
            time.sleep(base_delay_sec * (2 ** (attempt - 1)))
