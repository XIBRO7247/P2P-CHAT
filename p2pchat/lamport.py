import threading


class LamportClock:
    """Lamport logical clock."""
    def __init__(self, initial: int = 0):
        self._time = initial
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_time: int) -> int:
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time

    @property
    def value(self) -> int:
        with self._lock:
            return self._time
