"""
Replication Manager:
- Track which peers hold replicas of which chunks (room_id + chunk_id)
- Decide when a chunk falls below the replication threshold
- Suggest / apply replica targets when new copies should be created
"""

from typing import Dict, Set, Iterable, Tuple

ChunkKey = Tuple[str, str]  # (room_id, chunk_id)


class ReplicationManager:
    def __init__(self, min_replicas: int):
        """
        :param min_replicas: Desired minimum number of replicas per chunk.
        """
        self.min_replicas = int(min_replicas)
        # Mapping: (room_id, chunk_id) -> set(peer_username)
        self.chunk_locations: Dict[ChunkKey, Set[str]] = {}

    # ---------- internal helpers ----------

    def _key(self, room_id: str, chunk_id: str) -> ChunkKey:
        return (room_id, chunk_id)

    # ---------- registration ----------

    def register_replica(self, room_id: str, chunk_id: str, peer: str) -> None:
        """
        Record that `peer` holds a replica of room_id/chunk_id.
        """
        key = self._key(room_id, chunk_id)
        locs = self.chunk_locations.setdefault(key, set())
        locs.add(peer)

    def register_replicas(self, room_id: str, chunk_id: str, peers: Iterable[str]) -> None:
        """
        Convenience: record multiple peers as holders of the same chunk.
        """
        key = self._key(room_id, chunk_id)
        locs = self.chunk_locations.setdefault(key, set())
        locs.update(peers)

    def remove_replica(self, room_id: str, chunk_id: str, peer: str) -> None:
        """
        Remove one peer from a chunk's replica set.
        """
        key = self._key(room_id, chunk_id)
        locs = self.chunk_locations.get(key)
        if not locs:
            return
        locs.discard(peer)
        if not locs:
            self.chunk_locations.pop(key, None)

    def remove_peer_from_all(self, peer: str) -> None:
        """
        Remove `peer` from all chunks (e.g. when a peer leaves permanently).
        """
        empty = []
        for key, peers in self.chunk_locations.items():
            if peer in peers:
                peers.discard(peer)
                if not peers:
                    empty.append(key)
        for key in empty:
            self.chunk_locations.pop(key, None)

    # ---------- introspection ----------

    def get_replicas(self, room_id: str, chunk_id: str) -> Set[str]:
        """
        Return a copy of the set of peers that hold this chunk.
        """
        key = self._key(room_id, chunk_id)
        return set(self.chunk_locations.get(key, set()))

    def all_chunks(self) -> Set[ChunkKey]:
        """
        Return the set of all (room_id, chunk_id) keys tracked.
        """
        return set(self.chunk_locations.keys())

    # ---------- threshold checks ----------

    def needs_more_replicas(self, room_id: str, chunk_id: str) -> bool:
        """
        Return True if replica count < min_replicas.
        """
        return len(self.get_replicas(room_id, chunk_id)) < self.min_replicas

    # ---------- target selection ----------

    def choose_replication_targets(
        self,
        room_id: str,
        chunk_id: str,
        available_peers: Set[str]
    ) -> Set[str]:
        """
        Decide which peers should become new replicas.
        """
        current = self.get_replicas(room_id, chunk_id)
        needed = self.min_replicas - len(current)
        if needed <= 0:
            return set()

        candidates = available_peers - current
        if not candidates:
            return set()

        # Take first N peers, simple strategy
        return set(list(candidates)[:needed])

    def ensure_minimum_replicas(
        self,
        room_id: str,
        chunk_id: str,
        available_peers: Set[str]
    ) -> Set[str]:
        """
        Fully update internal state to indicate new replicas exist.
        (Actual sending of chunk data is caller's job.)
        """
        targets = self.choose_replication_targets(room_id, chunk_id, available_peers)
        if targets:
            self.register_replicas(room_id, chunk_id, targets)
        return targets
