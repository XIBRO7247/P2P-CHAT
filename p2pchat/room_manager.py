"""
Room Manager:
- Tracks room membership
- Tracks known peers in each room
"""

from typing import Dict, Set, Iterable


class RoomManager:
    """Manages room subscriptions and peer membership."""

    def __init__(self):
        # room_id -> set of usernames
        self.room_members: Dict[str, Set[str]] = {}

    def update_members(self, room_id: str, members: Iterable[str]) -> None:
        """
        Replace the membership set for a room with the given iterable of usernames.
        Used when we receive JOIN_ROOM_ACK from the supernode with the full member list.
        """
        self.room_members[room_id] = set(members)

    def add_peer_to_room(self, room_id: str, username: str) -> None:
        self.room_members.setdefault(room_id, set()).add(username)

    def remove_peer_from_room(self, room_id: str, username: str) -> None:
        if room_id in self.room_members:
            self.room_members[room_id].discard(username)
            if not self.room_members[room_id]:
                del self.room_members[room_id]

    def get_peers_in_room(self, room_id: str) -> Set[str]:
        return self.room_members.get(room_id, set())

    def all_room_peers(self) -> Set[str]:
        """
        Convenience: union of all usernames in all rooms we’re currently in.
        Useful for room-based address resolution later.
        """
        peers: Set[str] = set()
        for members in self.room_members.values():
            peers.update(members)
        return peers
