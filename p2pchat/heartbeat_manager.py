"""
Heartbeat Manager:
- Tracks timestamps for partner peer/node heartbeats
- Useful for detecting offline peers and triggering replication
"""

from typing import Dict
import time


class HeartbeatManager:
    def __init__(self, timeout_sec: float):
        self.timeout = timeout_sec
        self.last_seen: Dict[str, float] = {}  # peer username -> timestamp

    def register_heartbeat(self, peer: str):
        self.last_seen[peer] = time.time()

    def is_peer_alive(self, peer: str) -> bool:
        ts = self.last_seen.get(peer)
        if not ts:
            return False
        return (time.time() - ts) <= self.timeout

    def dead_peers(self) -> Dict[str, float]:
        """Return peers whose heartbeats have expired."""
        now = time.time()
        return {
            peer: ts
            for peer, ts in self.last_seen.items()
            if (now - ts) > self.timeout
        }
