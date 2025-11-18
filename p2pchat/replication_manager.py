"""
Replication Manager:
- Track chunk replicas
- Determine when peer falls below replication threshold
"""

from typing import Dict, Set


class ReplicationManager:
    def __init__(self, min_replicas: int):
        self.min_replicas = min_replicas
        self.chunk_locations: Dict[str, Set[str]] = {}  # chunk_id -> set(peers)

    def register_replica(self, chunk_id: str, peer: str):
        locs = self.chunk_locations.setdefault(chunk_id, set())
        locs.add(peer)

    def remove_replica(self, chunk_id: str, peer: str):
        locs = self.chunk_locations.get(chunk_id)
        if locs:
            locs.discard(peer)

    def needs_more_replicas(self, chunk_id: str) -> bool:
        return len(self.chunk_locations.get(chunk_id, [])) < self.min_replicas

    def choose_replication_targets(self, chunk_id: str, available_peers: Set[str]) -> Set[str]:
        """Choose peers to add more replicas to, aimed for redundancy."""
        current = self.chunk_locations.get(chunk_id, set())
        needed = self.min_replicas - len(current)
        if needed <= 0:
            return set()
        return set(list(available_peers - current)[:needed])
