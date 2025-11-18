"""
Room Manager:
- Tracks room membership
- Tracks known peers in each room
"""

from typing import Dict, Set

class RoomManager:
    """Manages room subscriptions and peer membership."""

    def __init__(self):
        self.room_members: Dict[str, Set[str]] = {}

    def add_peer_to_room(self, room_id: str, username: str):
        self.room_members.setdefault(room_id, set()).add(username)

    def remove_peer_from_room(self, room_id: str, username: str):
        if room_id in self.room_members:
            self.room_members[room_id].discard(username)
            if not self.room_members[room_id]:
                del self.room_members[room_id]

    def get_peers_in_room(self, room_id: str):
        return self.room_members.get(room_id, set())
