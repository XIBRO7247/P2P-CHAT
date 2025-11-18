# p2pchat/friend_manager.py

from typing import Dict, Set, Tuple, Any, Optional

from .user_state import UserStateStore
from .protocol import MessageType, make_envelope


class FriendManager:
    """
    Owns friend state and persistence.
    Middleware calls methods here and then sends any returned envelopes.
    """

    def __init__(self, username: str, local_ip: str, listen_port: int):
        self.username = username
        self.local_ip = local_ip
        self.listen_port = listen_port

        self.state_store = UserStateStore(username)
        # Load persisted friends + addresses
        self.friends, self.user_cache = self.state_store.load()

        # In-memory only
        self.outgoing_friend_requests: Set[str] = set()
        self.incoming_friend_requests: Set[str] = set()

    # ------------------------------------------------------------------ helpers

    def _save(self) -> None:
        self.state_store.save(self.friends, self.user_cache)

    # ------------------------------------------------------------------ CLI actions (called by middleware)

    def build_friend_request(
        self, target: str, lamport: int
    ) -> tuple[Optional[bytes], Optional[str]]:
        """
        Returns (env_bytes, message_str) for a FRIEND_REQUEST to send to supernode.
        """
        if target == self.username:
            return None, "You cannot friend yourself."
        if target in self.friends:
            return None, f"{target} is already your friend."
        if target in self.outgoing_friend_requests:
            return None, f"Friend request already sent to {target}."

        self.outgoing_friend_requests.add(target)
        payload = {"target": target}
        env = make_envelope(
            MessageType.FRIEND_REQUEST,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        return env, f"Sent friend request to {target}."

    def build_friend_accept(
        self, user: str, lamport: int
    ) -> tuple[Optional[bytes], Optional[str]]:
        if user not in self.incoming_friend_requests:
            return None, f"No pending friend request from {user}."

        self.incoming_friend_requests.discard(user)
        self.friends.add(user)
        self._save()

        payload = {
            "target": user,
            "accepted": True,
            "friend_ip": self.local_ip,
            "friend_port": self.listen_port,
        }
        env = make_envelope(
            MessageType.FRIEND_RESPONSE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        return env, f"Accepted friend request from {user}."

    def build_friend_reject(
        self, user: str, lamport: int
    ) -> tuple[Optional[bytes], Optional[str]]:
        if user not in self.incoming_friend_requests:
            return None, f"No pending friend request from {user}."

        self.incoming_friend_requests.discard(user)

        payload = {
            "target": user,
            "accepted": False,
        }
        env = make_envelope(
            MessageType.FRIEND_RESPONSE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        return env, f"Rejected friend request from {user}."

    def unfriend(self, user: str) -> str:
        if user not in self.friends:
            return f"{user} is not in your friend list."

        self.friends.discard(user)
        self.user_cache.pop(user, None)
        self._save()
        return f"Unfriended {user}."

    # ------------------------------------------------------------------ network events (called by middleware)

    def on_friend_request(self, payload: Dict[str, Any]) -> Optional[str]:
        """
        Handle incoming FRIEND_REQUEST from supernode.
        Payload: {"from_user": ..., "friend_ip"?, "friend_port"?}
        """
        from_user = payload.get("from_user")
        if not from_user:
            return None

        self.incoming_friend_requests.add(from_user)

        ip = payload.get("friend_ip")
        port = payload.get("friend_port")
        if ip and port:
            self.user_cache[from_user] = (ip, int(port))
            self._save()

        return f"New friend request from {from_user}."

    def on_friend_response(self, payload: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Handle incoming FRIEND_RESPONSE from supernode.
        Returns (accepted, message_str).
        """
        from_user = payload.get("from_user")
        accepted = payload.get("accepted", False)

        if not from_user:
            return False, None

        self.outgoing_friend_requests.discard(from_user)

        if not accepted:
            return False, f"{from_user} rejected your friend request."

        # Accepted
        self.friends.add(from_user)
        ip = payload.get("friend_ip")
        port = payload.get("friend_port")
        if ip and port:
            self.user_cache[from_user] = (ip, int(port))
        self._save()

        return True, f"{from_user} accepted your friend request."
