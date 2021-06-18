from __future__ import annotations

import time
from typing import Optional


__all__ = ["Deadline"]


class Deadline:
    def __init__(self, timeout: Optional[float]) -> None:
        self.deadline: Optional[float]
        if timeout is None:
            self.deadline = None
        else:
            self.deadline = time.monotonic() + timeout

    def timeout(self) -> Optional[float]:
        if self.deadline is None:
            return None
        else:
            return self.deadline - time.monotonic()
