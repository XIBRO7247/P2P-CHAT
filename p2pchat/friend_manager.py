# p2pchat/friend_manager.py

from typing import Dict, Set, Tuple, Any, Optional

from .user_state import UserStateStore
from .protocol import MessageType, make_envelope


class FriendManager:
    """
    Owns friend state and persistence.
    Middleware calls methods here and then sends any returned envelopes.

    This manager is agnostic to how FRIEND_REQUEST / FRIEND_RESPONSE
    actually travel across the network:
    - via supernode (standard case),
    - P2P via a mutual friend (FRIEND_INTRO path), or
    - relayed/queued while peers are offline (same concept as DM relay).

    All of that routing / retry / relay logic lives in the middleware +
    supernode; here we just handle local state and envelopes.
    """

    def __init__(self, username: str, local_ip: str, listen_port: int):
        self.username = username
        self.local_ip = local_ip
        self.listen_port = listen_port

        self.state_store = UserStateStore(username)
        # Load persisted friends + addresses
        self.friends, self.user_cache = self.state_store.load()

        # In-memory only (not persisted)
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
        Returns (env_bytes, message_str) for a FRIEND_REQUEST to send.

        Middleware decides where to send it:
        - normally: to the supernode,
        - or via an intro/relay path (FRIEND_INTRO),
        - or via some queued/relay mechanism if the target is offline.

        From this manager's POV, we just:
        - enforce local invariants,
        - remember that we have an outgoing request to 'target'.
        """
        if target == self.username:
            return None, "You cannot friend yourself."
        if target in self.friends:
            return None, f"{target} is already your friend."
        if target in self.outgoing_friend_requests:
            return None, f"Friend request already sent to {target}."
        if target in self.incoming_friend_requests:
            return None, f"{target} has already sent you a friend request."

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
        """
        Build a FRIEND_RESPONSE(accepted=True) envelope.

        The middleware will:
        - send this to the supernode (for ledger + routing),
        - optionally also send it P2P directly if it has a fresh address.

        However it travels, when the requester finally receives the
        FRIEND_RESPONSE, *that's* when the friendship is considered live.
        """
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
        """
        Build a FRIEND_RESPONSE(accepted=False) envelope.

        Again, the transport may route/relay this however it wants
        (supernode, P2P, queued until requester is online, etc.).
        """
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
        """
        Local unfriend. We don't currently notify the supernode on unfriend;
        edges are logged when created (on accept).
        """
        if user not in self.friends:
            return f"{user} is not in your friend list."

        self.friends.discard(user)
        self.user_cache.pop(user, None)
        self._save()
        return f"Unfriended {user}."

    # ------------------------------------------------------------------ network events (called by middleware)

    def on_friend_request(self, payload: Dict[str, Any]) -> Optional[str]:
        """
        Handle incoming FRIEND_REQUEST.

        Possible sources:

        1) Via supernode:
           payload = {"from_user": "<name>", "friend_ip"?, "friend_port"?}

        2) Via P2P friend-of-a-friend relay:
           payload = {"from_user": "<name>", "via": "<helper_name>"}
           (no IP/port, because the packet's network origin is the helper.)

        3) Eventually: same envelopes could be delivered after being relayed /
           queued while we were offline; that doesn't change this logic.

        In all cases we:
        - add 'from_user' to incoming_friend_requests (unless already a friend),
        - store optional IP/port if present.
        """
        from_user = payload.get("from_user")
        if not from_user:
            return None

        # If we're already friends, just ignore this request.
        if from_user in self.friends:
            return None

        # Avoid re-adding duplicates
        self.incoming_friend_requests.add(from_user)

        ip = payload.get("friend_ip")
        port = payload.get("friend_port")
        if ip and port:
            self.user_cache[from_user] = (ip, int(port))
            self._save()

        # 'via' field (mutual friend) is purely informational here.
        return f"New friend request from {from_user}."

    def on_friend_response(self, payload: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Handle incoming FRIEND_RESPONSE from supernode / peer.
        Returns (accepted, message_str).

        This is called regardless of how the original FRIEND_REQUEST travelled
        or how long it was queued/relayed:
        - direct to supernode,
        - via friend-of-a-friend,
        - via some offline relay/holder mechanism.
        """
        from_user = payload.get("from_user")
        accepted = payload.get("accepted", False)

        if not from_user:
            return False, None

        # We are no longer "waiting" on this user.
        self.outgoing_friend_requests.discard(from_user)

        if not accepted:
            # This is a genuine user-level reject.
            return False, f"{from_user} rejected your friend request."

        # Accepted
        self.friends.add(from_user)
        ip = payload.get("friend_ip")
        port = payload.get("friend_port")
        if ip and port:
            self.user_cache[from_user] = (ip, int(port))
        self._save()

        return True, f"{from_user} accepted your friend request."
