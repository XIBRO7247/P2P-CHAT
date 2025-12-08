import threading
import socket
import os
import traceback
import queue
import json
from typing import Dict, Any, List, Tuple, Set

from p2pchat.config_loader import Config
from p2pchat.protocol import (
    MessageType,
    make_envelope,
    parse_envelope,
    md5_checksum,
)
from p2pchat.lamport import LamportClock

# --------- force working directory to this file's folder ---------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)
# -----------------------------------------------------------------


class Supernode:
    def __init__(self, host: str, port: int):
        # Bind on the requested host/port, but advertise a concrete IP
        # if we were given 0.0.0.0 (wildcard).
        self.bind_host = host
        self.port = port

        if host == "0.0.0.0":
            # Use a real LAN/loopback IP for telling clients/supernodes where we are
            self.host = self._guess_lan_ip()
        else:
            self.host = host

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.bind_host, port))

        self.lamport = LamportClock()

        self.peer_supernodes: set[tuple[str, int]] = set()

        # username -> (ip, port)
        self.users: Dict[str, tuple[str, int]] = {}
        # room_id -> set(username)
        self.rooms: Dict[str, set[str]] = {}
        # chunk_id -> metadata dict
        self.chunks: Dict[str, Dict[str, Any]] = {}

        # Presence: username -> "ONLINE" / "OFFLINE"
        self.user_status: Dict[str, str] = {}

        # Offline DM orchestration:
        #   for_user -> set(holder_usernames)
        self.dm_holders: Dict[str, Set[str]] = {}
        #   for_user -> list[{from_user, to_user, text, timestamp}]
        # Only used as a last resort when no peer-holder is available.
        self.dm_super_store: Dict[str, List[Dict[str, Any]]] = {}

        # Offline friend orchestration (simple queues via supernode):
        #   target_user -> set(requester_usernames)
        # These are pending FRIEND_REQUEST edges (requester -> target).
        self.pending_friend_requests: Dict[str, Set[str]] = {}
        #   requester_user -> list[{from_user, accepted, friend_ip?, friend_port?}]
        # These are pending FRIEND_RESPONSE messages destined for 'requester_user'.
        self.pending_friend_responses: Dict[str, List[Dict[str, Any]]] = {}

        # Last-resort friend events held by the supernode itself.
        # for_user -> list[{
        #   "kind": "request" | "response",
        #   "from_user": str,
        #   "accepted"?: bool,     # for responses
        #   "timestamp": int,
        # }]
        self.friend_super_store: Dict[str, List[Dict[str, Any]]] = {}

        self.files = {}  # file_id -> metadata

        self._running = threading.Event()
        # Thread-safe queue for received envelopes
        self._recv_queue: "queue.Queue[tuple[Dict[str, Any], tuple[str, int]]]" = queue.Queue()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        self._running.set()
        threading.Thread(target=self._recv_loop, daemon=True).start()
        threading.Thread(target=self._worker_loop, daemon=True).start()

        lan_ip = self._guess_lan_ip()
        print(f"[supernode] Listening on {self.host}:{self.port}")
        print(f"[supernode] LAN address (for clients): {lan_ip}:{self.port}")

    def stop(self):
        self._running.clear()
        try:
            self.sock.close()
        except OSError:
            pass

    # ------------------------------------------------------------------
    # Networking loops
    # ------------------------------------------------------------------

    def _recv_loop(self):
        while self._running.is_set():
            try:
                data, addr = self.sock.recvfrom(65535)
            except OSError:
                break
            try:
                env = parse_envelope(data)
                # Put into thread-safe queue
                self._recv_queue.put((env, addr))
            except Exception:
                # Malformed JSON etc. – just drop
                continue

    def _worker_loop(self):
        while self._running.is_set():
            try:
                env, addr = self._recv_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            except Exception:
                continue
            try:
                self._handle(env, addr)
            except Exception:
                traceback.print_exc()

    # ------------------------------------------------------------------
    # Core send helper
    # ------------------------------------------------------------------

    def _send(self, env: Dict[str, Any], addr) -> None:
        """
        Wrapper for sending a UDP datagram with basic error handling.

        'env' is a logical envelope dict as produced by make_envelope().
        We serialise it to JSON bytes here.
        """
        try:
            data = json.dumps(env).encode("utf-8")
            self.sock.sendto(data, addr)
        except OSError:
            # Could log this if desired
            pass

    # ------------------------------------------------------------------
    # Hash-error helper
    # ------------------------------------------------------------------

    def _send_hash_error(
        self,
        addr: Tuple[str, int],
        src_user: str | None,
        reason: str,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        Send a HASH_ERROR back to the sender when checksum validation fails.
        """
        payload: Dict[str, Any] = {
            "reason": reason,
        }
        if src_user:
            payload["original_src_user"] = src_user
        if details:
            payload["details"] = details

        env = make_envelope(
            MessageType.HASH_ERROR,
            "supernode",
            self.host,
            self.port,
            self.lamport.tick(),
            payload,
        )
        self._send(env, addr)

    # ------------------------------------------------------------------
    # Top-level handler
    # ------------------------------------------------------------------

    def _handle(self, env: Dict[str, Any], addr):
        # ----- Lamport clock -----
        self.lamport.update(env.get("lamport", 0))

        # ----- Integrity check for ALL incoming messages -----
        payload = env.get("payload", {}) or {}
        expected_checksum = env.get("checksum")
        src_user = env.get("src_user")
        msg_type = env.get("type")
        msg_id = env.get("msg_id")

        # HASH_ERROR itself we *still* integrity-check; if its checksum is bad
        # we just drop it silently (no point in replying with another HASH_ERROR).
        if msg_type != MessageType.HASH_ERROR.value:
            if expected_checksum is None:
                # Missing checksum: treat as integrity failure.
                print(
                    f"[supernode] Dropping message from {src_user} at {addr}: "
                    f"missing checksum."
                )
                self._send_hash_error(
                    addr,
                    src_user,
                    reason="missing_checksum",
                    details={"msg_type": msg_type, "msg_id": msg_id},
                )
                return

            # Recompute checksum over the payload (same scheme as make_envelope)
            try:
                payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
                computed = md5_checksum(payload_bytes)
            except Exception as e:
                print(
                    f"[supernode] Error recomputing checksum for message from "
                    f"{src_user} at {addr}: {e!r}"
                )
                self._send_hash_error(
                    addr,
                    src_user,
                    reason="checksum_compute_error",
                    details={"msg_type": msg_type, "msg_id": msg_id},
                )
                return

            if computed != expected_checksum:
                print(
                    f"[supernode] Checksum mismatch from {src_user} at {addr}: "
                    f"expected={expected_checksum}, computed={computed}. Dropping."
                )
                self._send_hash_error(
                    addr,
                    src_user,
                    reason="checksum_mismatch",
                    details={
                        "msg_type": msg_type,
                        "msg_id": msg_id,
                        "expected": expected_checksum,
                        "computed": computed,
                    },
                )
                return

        # At this point, integrity is OK (or it's a HASH_ERROR we decided to accept).
        msg_type = env.get("type")

        if msg_type == MessageType.REGISTER_USER.value:
            self._handle_register(src_user, addr, payload)
        elif msg_type == MessageType.JOIN_ROOM.value:
            self._handle_join_room(src_user, addr, payload)
        elif msg_type == MessageType.LEAVE_ROOM.value:
            self._handle_leave_room(src_user, addr, payload)
        elif msg_type == MessageType.LIST_ROOMS.value:
            self._handle_list_rooms(src_user, addr, payload)
        elif msg_type == MessageType.CHAT.value:
            self._handle_chat(src_user, payload)
        elif msg_type == MessageType.NEW_CHUNK.value:
            self._handle_new_chunk(src_user, payload)
        elif msg_type == MessageType.FRIEND_REQUEST.value:
            self._handle_friend_request(src_user, payload)
        elif msg_type == MessageType.FRIEND_RESPONSE.value:
            self._handle_friend_response(src_user, payload)
        elif msg_type == MessageType.LOOKUP_USER.value:
            self._handle_lookup_user(src_user, addr, payload)

        # --- Presence + offline DM orchestration ---
        elif msg_type == MessageType.USER_STATUS.value:
            self._handle_user_status(src_user, payload)
        elif msg_type == MessageType.PENDING_DM_HOLDER_UPDATE.value:
            self._handle_pending_dm_holder_update(src_user, payload)
        elif msg_type == MessageType.PENDING_DM_STORE.value:
            self._handle_pending_dm_store(src_user, payload)
        elif msg_type == MessageType.PENDING_DM_DELIVERED.value:
            self._handle_pending_dm_delivered(src_user, payload)

        # --- Offline friend orchestration (last-resort store) ---
        elif msg_type == MessageType.PENDING_FRIEND_STORE.value:
            self._handle_pending_friend_store(src_user, payload)
        elif msg_type == MessageType.PENDING_FRIEND_DELIVERED.value:
            self._handle_pending_friend_delivered(src_user, payload)

        # --- Hash error reporting from peers ---
        elif msg_type == MessageType.HASH_ERROR.value:
            reason = payload.get("reason")
            details = payload.get("details")
            print(
                f"[supernode] Received HASH_ERROR from {src_user} at {addr}: "
                f"reason={reason}, details={details}"
            )
        elif msg_type == MessageType.SUPERNODE_HELLO.value:
            self._handle_supernode_hello(payload)
        elif msg_type == MessageType.SUPERNODE_REPLICA_REQUEST.value:
            # payload comes from another supernode; sender addr is in `addr`
            self._handle_supernode_replica_request(payload, addr)

        elif msg_type == MessageType.SUPERNODE_REPLICA_STATE.value:
            self._handle_supernode_replica_state(payload)

        elif msg_type == MessageType.FILE_REGISTER.value:
            self._handle_file_register(src_user, addr, payload)
        elif msg_type == MessageType.FILE_LIST.value:
            self._handle_file_list(src_user, addr, payload)
        elif msg_type == MessageType.FILE_INFO.value:
            self._handle_file_info(src_user, addr, payload)
        elif msg_type == MessageType.FILE_SEED_UPDATE.value:
            self._handle_file_seed_update(src_user, addr, payload)


        else:
            # Ignore unknown or peer-only types such as FRIEND_INTRO, heartbeats, etc.
            pass

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _handle_register(self, username, addr, payload):
        if not username:
            return

        listen_port = payload.get("listen_port", addr[1])
        new_endpoint = (addr[0], listen_port)

        old_endpoint = self.users.get(username)
        self.users[username] = new_endpoint
        # A REGISTER is effectively a "I'm online here now"
        self.user_status[username] = "ONLINE"

        if old_endpoint is None:
            print(f"[supernode] Registered: {username} at {addr[0]}:{listen_port}")
        else:
            print(
                f"[supernode] Re-registered username {username}: "
                f"{old_endpoint} -> {new_endpoint}"
            )

        env = make_envelope(
            MessageType.REGISTER_ACK,
            "supernode",
            self.host,
            self.port,
            self.lamport.tick(),
            {},
        )
        self._send(env, addr)

    def _handle_join_room(self, username, addr, payload):
        room_id = payload.get("room_id")
        if not room_id or not username:
            return
        members = self.rooms.setdefault(room_id, set())
        members.add(username)
        print(f"[supernode] {username} joined room {room_id}")

        env = make_envelope(
            MessageType.JOIN_ROOM_ACK,
            "supernode",
            self.host,
            self.port,
            self.lamport.tick(),
            {"room_id": room_id, "members": list(members)},
        )
        self._send(env, addr)

    def _handle_leave_room(self, username: str | None, addr, payload: Dict[str, Any]) -> None:
        """
        Handle LEAVE_ROOM from a client: remove them from the room set, if present.
        No ACK needed; the operation is idempotent.
        """
        if not username:
            return
        room_id = payload.get("room_id")
        if not room_id:
            return

        members = self.rooms.get(room_id)
        if members and username in members:
            members.discard(username)
            print(f"[supernode] {username} left room {room_id}")
        else:
            print(f"[supernode] LEAVE_ROOM from {username} for {room_id}, but they were not a member.")


    def _handle_list_rooms(self, username: str | None, addr, payload: Dict[str, Any]) -> None:
        """
        Handle LIST_ROOMS from a client.

        Response payload:
            {
                "joined":   [room_id, ...],   # rooms where 'username' is a member
                "available":[room_id, ...],   # all known rooms (discovery)
            }
        """
        if not username:
            return

        joined = sorted(
            [rid for rid, members in self.rooms.items() if username in members]
        )
        available = sorted(list(self.rooms.keys()))

        print(
            f"[supernode] LIST_ROOMS from {username}: "
            f"joined={joined}, available={available}"
        )

        resp_payload = {
            "user": username,
            "joined": joined,
            "available": available,
        }

        env = make_envelope(
            MessageType.LIST_ROOMS_RESPONSE,
            "supernode",
            self.host,
            self.port,
            self.lamport.tick(),
            resp_payload,
        )
        self._send(env, addr)

    def _handle_chat(self, username, payload):
        """
        Room chat is now P2P.

        The supernode's role here is:
        - Log the message.
        - Optionally keep its view of room membership in sync.
        - NOT to fan out the CHAT to other peers.
        """
        room_id = payload.get("room_id")
        if not room_id:
            return

        text = payload.get("text")
        print(f"[supernode] CHAT in {room_id} from {username}: {text}")

        # Keep membership roughly in sync: if we see a chat from someone we
        # don't think is in the room yet, add them.
        members = self.rooms.setdefault(room_id, set())
        if username not in members:
            members.add(username)
            print(
                f"[supernode] (implicit) added {username} to room {room_id} "
                f"on first CHAT"
            )

        # No broadcast here – peers already send P2P room messages.

    def _handle_new_chunk(self, username, payload):
        room_id = payload.get("room_id")
        chunk_id = payload.get("chunk_id")
        if not room_id or not chunk_id or not username:
            return

        # --- Store metadata locally (optional but useful) ---
        meta = {
            "room_id": room_id,
            "owner": username,
            "num_messages": payload.get("num_messages"),
            "hash": payload.get("hash"),
        }
        self.chunks[chunk_id] = meta
        print(f"[supernode] NEW_CHUNK {chunk_id} for room {room_id} from {username}: {meta}")

        # --- Ensure room exists ---
        members = self.rooms.setdefault(room_id, set())

        # --- Forward to all room members except the sender ---
        for member in members:
            if member == username:
                continue

            dest = self.users.get(member)
            if not dest:
                continue  # user offline or unknown

            ip, port = dest

            # Forward envelope with logical source = owner, network source = supernode
            lamport = self.lamport.tick()
            fwd_env = make_envelope(
                MessageType.NEW_CHUNK,
                username,  # logical source stays the real owner
                self.host,  # network path: supernode → peer
                self.port,
                lamport,
                payload,
            )
            self._send(fwd_env, (ip, port))

    def _handle_friend_request(self, src_user: str, payload: Dict[str, Any]) -> None:
        """
        Sender -> supernode -> recipient, with offline-safe queuing.

        Payload from sender: {"target": "<username>"}.
        Payload to recipient: {"from_user": src_user, "friend_ip", "friend_port"}.

        Behaviour:
          - If target is ONLINE and known -> forward immediately.
          - If target is OFFLINE but known -> queue request; deliver on USER_STATUS ONLINE.
          - If target is completely unknown (never registered) -> synthetic reject
            so the sender doesn't block forever on a typo / non-existent user.
        """
        target = payload.get("target")
        if not target or not src_user:
            return

        dest_ep = self.users.get(target)
        status = self.user_status.get(target, "OFFLINE")

        # If we have never seen this username, treat as unknown and reject.
        if dest_ep is None:
            print(
                f"[supernode] FRIEND_REQUEST from {src_user} to {target}, "
                f"but target is unknown; sending synthetic reject."
            )
            requester_ep = self.users.get(src_user)
            if requester_ep:
                r_ip, r_port = requester_ep
                resp_payload: Dict[str, Any] = {
                    "from_user": target,
                    "accepted": False,
                }
                env = make_envelope(
                    MessageType.FRIEND_RESPONSE,
                    "supernode",
                    self.host,
                    self.port,
                    self.lamport.tick(),
                    resp_payload,
                )
                self._send(env, (r_ip, r_port))
            return

        # Known user but currently offline -> queue for later delivery.
        if status != "ONLINE":
            pending = self.pending_friend_requests.setdefault(target, set())
            if src_user in pending:
                print(
                    f"[supernode] FRIEND_REQUEST from {src_user} to offline {target} "
                    f"already queued; ignoring duplicate."
                )
            else:
                pending.add(src_user)
                print(
                    f"[supernode] Queued FRIEND_REQUEST from {src_user} to offline {target}."
                )
            return

        # Target is ONLINE -> forward immediately.
        sender_ep = self.users.get(src_user)
        if not sender_ep:
            print(f"[supernode] FRIEND_REQUEST: no endpoint for src_user {src_user}")
            return
        sender_ip, sender_port = sender_ep

        ip, port = dest_ep
        print(f"[supernode] FRIEND_REQUEST {src_user} -> {target} (immediate deliver)")

        forward_payload = {
            "from_user": src_user,
            "friend_ip": sender_ip,
            "friend_port": sender_port,
        }

        env = make_envelope(
            MessageType.FRIEND_REQUEST,
            src_user,  # logical source is the requester
            self.host,  # network source is supernode
            self.port,
            self.lamport.tick(),
            forward_payload,
        )
        self._send(env, (ip, port))

    def _handle_friend_response(self, src_user: str, payload: Dict[str, Any]) -> None:
        """
        Recipient -> supernode -> original requester, with offline-safe queuing.

        Payload from recipient: {"target": "<requester>", "accepted": bool,
                                 "friend_ip"?, "friend_port"?}.
        Payload to requester:   {"from_user": src_user, "accepted": bool,
                                 "friend_ip"?, "friend_port"?}.
        """
        target = payload.get("target")
        accepted = payload.get("accepted", False)

        if not target or not src_user:
            return

        dest_ep = self.users.get(target)
        status = self.user_status.get(target, "OFFLINE")

        # If requester is offline or unknown -> queue response for later.
        if dest_ep is None or status != "ONLINE":
            bucket = self.pending_friend_responses.setdefault(target, [])
            bucket.append(
                {
                    "from_user": src_user,
                    "accepted": bool(accepted),
                    "friend_ip": payload.get("friend_ip"),
                    "friend_port": payload.get("friend_port"),
                }
            )
            print(
                f"[supernode] Queued FRIEND_RESPONSE from {src_user} to offline/unknown "
                f"{target}, accepted={accepted}."
            )
            return

        ip, port = dest_ep
        forward_payload: Dict[str, Any] = {
            "from_user": src_user,
            "accepted": bool(accepted),
        }

        # Only share recipient's details if they accepted.
        if accepted:
            friend_ip = payload.get("friend_ip")
            friend_port = payload.get("friend_port")
            if friend_ip and friend_port:
                forward_payload["friend_ip"] = friend_ip
                forward_payload["friend_port"] = friend_port

        print(f"[supernode] FRIEND_RESPONSE {src_user} -> {target}, accepted={accepted} (immediate deliver)")

        env = make_envelope(
            MessageType.FRIEND_RESPONSE,
            src_user,      # logical source
            self.host,     # network source is supernode
            self.port,
            self.lamport.tick(),
            forward_payload,
        )
        self._send(env, (ip, port))

    def _handle_lookup_user(self, src_user: str, addr, payload: Dict[str, Any]) -> None:
        """
        Handle LOOKUP_USER from a client.

        Payload from client: {"target": "<username>"}
        Response payload:    {"user": "<username>", "found": bool,
                              "ip"?, "port"?}

        Presence-aware: if we know the user but they are not ONLINE,
        we report found=False so peers can treat them as offline and
        queue/forward DMs instead of trying heartbeat.
        """
        target = payload.get("target")
        if not target:
            return

        ep = self.users.get(target)
        status = self.user_status.get(target, "OFFLINE")

        if ep is None or status != "ONLINE":
            print(f"[supernode] LOOKUP_USER from {src_user}: {target} not found/online")
            resp_payload = {
                "user": target,
                "found": False,
            }
        else:
            ip, port = ep
            print(f"[supernode] LOOKUP_USER from {src_user}: {target} -> {ip}:{port}")
            resp_payload = {
                "user": target,
                "found": True,
                "ip": ip,
                "port": port,
            }

        env = make_envelope(
            MessageType.LOOKUP_RESPONSE,
            "supernode",
            self.host,
            self.port,
            self.lamport.tick(),
            resp_payload,
        )
        self._send(env, addr)

    # ------------------------------------------------------------------
    # Presence + offline DM + offline friend orchestration
    # ------------------------------------------------------------------

    def _handle_user_status(self, src_user: str, payload: Dict[str, Any]) -> None:
        if not src_user:
            return
        status = payload.get("status")
        if not status:
            return

        self.user_status[src_user] = status
        print(f"[supernode] USER_STATUS {src_user} -> {status}")

        if status == "ONLINE":
            # --- DM holders / supernode-held DMs ---

            # 1) If src_user is a DM recipient, tell all known holders to deliver.
            holders = self.dm_holders.get(src_user, set())
            for holder in list(holders):
                self._send_dm_deliver_request(holder, src_user)

            # 2) If src_user is a holder, and there are recipients they hold
            #    messages for who are already ONLINE, tell them to deliver too.
            for for_user, holder_set in self.dm_holders.items():
                if src_user in holder_set and self.user_status.get(for_user) == "ONLINE":
                    self._send_dm_deliver_request(src_user, for_user)

            # 3) If supernode is holding DMs for src_user as last resort, deliver now.
            stored = self.dm_super_store.get(src_user) or []
            if stored and self.users.get(src_user):
                print(
                    f"[supernode] Delivering {len(stored)} supernode-held "
                    f"pending DM(s) to {src_user}."
                )
                for msg in stored:
                    from_user = msg.get("from_user")
                    text = msg.get("text")
                    ts = msg.get("timestamp")
                    if not from_user or text is None:
                        continue
                    self._send_dm_direct_from_supernode(from_user, src_user, text, ts)
                # After delivery, clear store for this user.
                self.dm_super_store.pop(src_user, None)

            # --- Offline friend events stored on supernode as last resort ---

            friend_events = self.friend_super_store.get(src_user) or []
            if friend_events and self.users.get(src_user):
                dest_ip, dest_port = self.users[src_user]
                print(
                    f"[supernode] Delivering {len(friend_events)} supernode-held "
                    f"pending friend event(s) to {src_user}."
                )
                for ev in friend_events:
                    kind = ev.get("kind")
                    from_user = ev.get("from_user")
                    if not kind or not from_user:
                        continue

                    if kind == "request":
                        # Replay as FRIEND_REQUEST from from_user -> src_user
                        fwd_payload: Dict[str, Any] = {
                            "from_user": from_user,
                        }
                        # If we know from_user's endpoint and they are online,
                        # include friend_ip/port (nice-to-have, not required).
                        from_ep = self.users.get(from_user)
                        if from_ep and self.user_status.get(from_user) == "ONLINE":
                            f_ip, f_port = from_ep
                            fwd_payload["friend_ip"] = f_ip
                            fwd_payload["friend_port"] = f_port

                        env = make_envelope(
                            MessageType.FRIEND_REQUEST,
                            from_user,   # logical source is original requester
                            self.host,   # network source is supernode
                            self.port,
                            self.lamport.tick(),
                            fwd_payload,
                        )
                        self._send(env, (dest_ip, dest_port))

                    elif kind == "response":
                        accepted = bool(ev.get("accepted", False))
                        fwd_payload = {
                            "from_user": from_user,
                            "accepted": accepted,
                        }
                        # If accepted and we know from_user's endpoint, forward address too.
                        from_ep = self.users.get(from_user)
                        if accepted and from_ep and self.user_status.get(from_user) == "ONLINE":
                            f_ip, f_port = from_ep
                            fwd_payload["friend_ip"] = f_ip
                            fwd_payload["friend_port"] = f_port

                        env = make_envelope(
                            MessageType.FRIEND_RESPONSE,
                            from_user,   # logical source
                            self.host,
                            self.port,
                            self.lamport.tick(),
                            fwd_payload,
                        )
                        self._send(env, (dest_ip, dest_port))

                # Clear after replay
                self.friend_super_store.pop(src_user, None)

            # --- Offline friend requests queued for this user as TARGET ---

            pending_reqs = self.pending_friend_requests.pop(src_user, set())
            if pending_reqs:
                dest_ep = self.users.get(src_user)
                if dest_ep:
                    ip, port = dest_ep
                    for requester in sorted(pending_reqs):
                        requester_ep = self.users.get(requester)
                        if requester_ep:
                            r_ip, r_port = requester_ep
                            fwd_payload = {
                                "from_user": requester,
                                "friend_ip": r_ip,
                                "friend_port": r_port,
                            }
                        else:
                            # Requester currently offline/unknown → still deliver logical request
                            fwd_payload = {
                                "from_user": requester,
                            }

                        env = make_envelope(
                            MessageType.FRIEND_REQUEST,
                            requester,      # logical source
                            self.host,      # network source is supernode
                            self.port,
                            self.lamport.tick(),
                            fwd_payload,
                        )
                        self._send(env, (ip, port))
                        print(
                            f"[supernode] Delivered queued FRIEND_REQUEST from {requester} "
                            f"to {src_user} on ONLINE."
                        )

            # --- Offline friend responses queued for this user as REQUESTER ---

            pending_resps = self.pending_friend_responses.pop(src_user, [])
            if pending_resps:
                dest_ep = self.users.get(src_user)
                if dest_ep:
                    ip, port = dest_ep
                    for msg in pending_resps:
                        from_user = msg.get("from_user")
                        accepted = bool(msg.get("accepted", False))
                        friend_ip = msg.get("friend_ip")
                        friend_port = msg.get("friend_port")

                        fwd_payload: Dict[str, Any] = {
                            "from_user": from_user,
                            "accepted": accepted,
                        }
                        if accepted and friend_ip and friend_port:
                            fwd_payload["friend_ip"] = friend_ip
                            fwd_payload["friend_port"] = friend_port

                        env = make_envelope(
                            MessageType.FRIEND_RESPONSE,
                            from_user,      # logical source
                            self.host,      # network source is supernode
                            self.port,
                            self.lamport.tick(),
                            fwd_payload,
                        )
                        self._send(env, (ip, port))
                        print(
                            f"[supernode] Delivered queued FRIEND_RESPONSE from {from_user} "
                            f"to {src_user} on ONLINE, accepted={accepted}."
                        )

        elif status == "OFFLINE":
            # Marked offline; LOOKUP_USER will now say "found=False" for them.
            pass

    def _send_dm_deliver_request(self, holder: str, for_user: str) -> None:
        holder_ep = self.users.get(holder)
        if not holder_ep:
            return
        ip, port = holder_ep
        payload = {"for_user": for_user}
        env = make_envelope(
            MessageType.PENDING_DM_DELIVER_REQUEST,
            "supernode",
            self.host,
            self.port,
            self.lamport.tick(),
            payload,
        )
        self._send(env, (ip, port))
        print(f"[supernode] Asked holder {holder} to deliver pending DM(s) for {for_user}.")

    def _handle_pending_dm_holder_update(self, src_user: str, payload: Dict[str, Any]) -> None:
        """
        Record that 'holder' is now holding pending DMs for 'for_user'.

        Payload: {"for_user": "<username>", "holder": "<username>"}
        """
        for_user = payload.get("for_user")
        holder = payload.get("holder")
        if not for_user or not holder:
            return

        holders = self.dm_holders.setdefault(for_user, set())
        holders.add(holder)
        print(f"[supernode] Holder update: {holder} now holds pending DM(s) for {for_user}.")

    def _handle_pending_dm_store(self, src_user: str, payload: Dict[str, Any]) -> None:
        """
        Last-resort storage of pending DMs on the supernode itself.

        Payload: {"for_user": "<username>", "messages": [ {from_user, to_user, text, timestamp}, ... ]}
        """
        target = payload.get("for_user")
        msgs = payload.get("messages") or []
        if not target or not msgs:
            return

        bucket = self.dm_super_store.setdefault(target, [])
        bucket.extend(msgs)
        print(
            f"[supernode] Stored {len(msgs)} pending DM(s) for {target} as last-resort holder "
            f"(from {src_user})."
        )

    def _handle_pending_dm_delivered(self, src_user: str, payload: Dict[str, Any]) -> None:
        """
        A holder reports that it has delivered pending DMs.

        Payload: {"for_user": "<username>", "count": int}
        """
        for_user = payload.get("for_user")
        count = int(payload.get("count", 0))
        if not for_user:
            return

        print(
            f"[supernode] Holder {src_user} reports delivery of {count} pending DM(s) "
            f"to {for_user}."
        )

        # In this design, there's only ever one holder per user at a time,
        # so if we see a non-zero count we can safely clear holder metadata.
        if count > 0:
            self.dm_holders.pop(for_user, None)
            # Supernode store for that user should now be irrelevant as well.
            self.dm_super_store.pop(for_user, None)

    def _send_dm_direct_from_supernode(
        self,
        from_user: str,
        to_user: str,
        text: str,
        timestamp: int,
    ) -> None:
        """
        Deliver a DM held by the supernode directly to 'to_user' as if it came
        from 'from_user'. Only used for last-resort stored messages.
        """
        dest_ep = self.users.get(to_user)
        if not dest_ep:
            print(
                f"[supernode] Cannot deliver supernode-held DM from {from_user} to {to_user}: "
                f"user has no endpoint."
            )
            return

        ip, port = dest_ep
        payload = {
            "dm": True,
            "target": to_user,
            "text": text,
            "timestamp": timestamp,
        }
        env = make_envelope(
            MessageType.CHAT,
            from_user,       # logical sender is original user
            self.host,       # network source is supernode
            self.port,
            self.lamport.tick(),
            payload,
        )
        self._send(env, (ip, port))
        print(
            f"[supernode] Delivered supernode-held DM from {from_user} to {to_user} "
            f"at {ip}:{port}."
        )

    # ---------- Pending friend last-resort handlers ----------

    def _handle_pending_friend_store(self, src_user: str, payload: Dict[str, Any]) -> None:
        """
        Last-resort storage of pending friend events on the supernode.

        Payload: {"for_user": "<username>",
                  "events": [ {kind, from_user, accepted?, timestamp}, ... ]}
        """
        target = payload.get("for_user")
        events = payload.get("events") or []
        if not target or not events:
            return

        bucket = self.friend_super_store.setdefault(target, [])
        bucket.extend(events)
        print(
            f"[supernode] Stored {len(events)} pending friend event(s) for {target} "
            f"as last-resort holder (from {src_user})."
        )

    def _handle_pending_friend_delivered(self, src_user: str, payload: Dict[str, Any]) -> None:
        """
        A holder reports that it has delivered pending friend events.

        Payload: {"for_user": "<username>", "count": int}
        """
        for_user = payload.get("for_user")
        count = int(payload.get("count", 0))
        if not for_user:
            return

        print(
            f"[supernode] Holder {src_user} reports delivery of {count} pending "
            f"friend event(s) to {for_user}."
        )

        if count > 0:
            # If a holder has delivered events, any last-resort copy we hold
            # for that user is now stale.
            self.friend_super_store.pop(for_user, None)

    def _handle_supernode_hello(self, payload):
        host = payload["host"]
        port = int(payload["port"])
        addr = (host, port)

        if addr not in self.peer_supernodes:
            self.peer_supernodes.add(addr)
            print(f"[supernode] Registered peer supernode {addr}")
            # Push updated supernode list to clients
            self._broadcast_supernode_list()

    def _handle_supernode_replica_request(self, payload: Dict[str, Any], sender: Tuple[str, int]) -> None:
        """
        Another supernode has asked us for a full snapshot of our state so it can
        become an identical replica.

        We MUST only send JSON-serialisable types, so we convert all sets to lists.
        """
        # users: username -> (ip, port) is already JSON-friendly
        users_snapshot = dict(self.users)

        # rooms: room_id -> set(usernames) => list(usernames)
        rooms_snapshot: Dict[str, List[str]] = {
            room_id: list(members) for room_id, members in self.rooms.items()
        }

        # files: file_id -> metadata; convert any sets inside to lists
        files_snapshot: Dict[str, Dict[str, Any]] = {}
        for fid, meta in self.files.items():
            enc = dict(meta)
            if isinstance(enc.get("allowed"), set):
                enc["allowed"] = list(enc["allowed"])
            if isinstance(enc.get("seeders"), set):
                enc["seeders"] = list(enc["seeders"])
            files_snapshot[fid] = enc

        # dm_holders: username -> set(holders) => list(holders)
        dm_holders_snapshot: Dict[str, List[str]] = {
            user: list(holders) for user, holders in self.dm_holders.items()
        }

        # pending_friend_requests: target_user -> set(requester_usernames) => list(...)
        pfr_snapshot: Dict[str, List[str]] = {
            target: list(requesters)
            for target, requesters in self.pending_friend_requests.items()
        }

        # pending_friend_responses, dm_super_store, friend_super_store and user_status
        # are already dicts of lists/dicts/strings, so they should be JSON-safe.
        snapshot = {
            "users": users_snapshot,
            "rooms": rooms_snapshot,
            "files": files_snapshot,
            "dm_holders": dm_holders_snapshot,
            "pending_friend_requests": pfr_snapshot,
            "pending_friend_responses": self.pending_friend_responses,
            "dm_super_store": self.dm_super_store,
            "friend_super_store": self.friend_super_store,
            "user_status": self.user_status,
        }

        env = make_envelope(
            MessageType.SUPERNODE_REPLICA_STATE,
            "supernode",
            self.host,
            self.port,
            self.lamport.tick(),
            snapshot,
        )
        self._send(env, sender)
        print(f"[supernode] Sent replica snapshot to peer {sender}")

    def _handle_supernode_replica_state(self, payload: Dict[str, Any]) -> None:
        """
        Merge a full replica snapshot from another supernode into our own state.
        This is called on the *new* supernode when it first joins the cluster.
        """

        # users: username -> (ip, port)
        incoming_users: Dict[str, Tuple[str, int]] = payload.get("users", {}) or {}
        self.users.update(incoming_users)

        # rooms: room_id -> list(usernames) -> convert back to sets and merge
        incoming_rooms: Dict[str, List[str]] = payload.get("rooms", {}) or {}
        for room_id, members_list in incoming_rooms.items():
            members_set = set(members_list)
            if room_id not in self.rooms:
                self.rooms[room_id] = members_set
            else:
                self.rooms[room_id].update(members_set)

        # files: file_id -> metadata (with "allowed" and "seeders" as lists)
        incoming_files: Dict[str, Dict[str, Any]] = payload.get("files", {}) or {}
        for fid, meta in incoming_files.items():
            if fid not in self.files:
                # convert lists back to sets internally
                m = dict(meta)
                if isinstance(m.get("allowed"), list):
                    m["allowed"] = set(m["allowed"])
                if isinstance(m.get("seeders"), list):
                    m["seeders"] = set(m["seeders"])
                self.files[fid] = m
            else:
                existing = self.files[fid]
                # merge seeders
                existing_seeders = set(existing.get("seeders", []))
                incoming_seeders = set(meta.get("seeders", []))
                existing["seeders"] = existing_seeders | incoming_seeders
                # allowed: union if present
                if "allowed" in meta:
                    existing_allowed = set(existing.get("allowed", []))
                    incoming_allowed = set(meta.get("allowed", []))
                    existing["allowed"] = existing_allowed | incoming_allowed

        # dm_holders: username -> list(holders) -> convert back to sets and merge
        incoming_dm_holders: Dict[str, List[str]] = payload.get("dm_holders", {}) or {}
        for user, holders_list in incoming_dm_holders.items():
            holders_set = set(holders_list)
            if user not in self.dm_holders:
                self.dm_holders[user] = holders_set
            else:
                self.dm_holders[user].update(holders_set)

        # pending_friend_requests: target_user -> list(requester_usernames)
        incoming_pfr: Dict[str, List[str]] = payload.get("pending_friend_requests", {}) or {}
        for target, req_list in incoming_pfr.items():
            req_set = set(req_list)
            if target not in self.pending_friend_requests:
                self.pending_friend_requests[target] = req_set
            else:
                self.pending_friend_requests[target].update(req_set)

        # pending_friend_responses: requester_user -> list[...]
        incoming_pfrsp: Dict[str, List[Dict[str, Any]]] = payload.get(
            "pending_friend_responses", {}
        ) or {}
        for requester, resps in incoming_pfrsp.items():
            lst = self.pending_friend_responses.setdefault(requester, [])
            lst.extend(resps)

        # dm_super_store: username -> list[DM events]
        incoming_dm_super: Dict[str, List[Dict[str, Any]]] = payload.get(
            "dm_super_store", {}
        ) or {}
        for user, events in incoming_dm_super.items():
            lst = self.dm_super_store.setdefault(user, [])
            lst.extend(events)

        # friend_super_store: username -> list[friend events]
        incoming_friend_super: Dict[str, List[Dict[str, Any]]] = payload.get(
            "friend_super_store", {}
        ) or {}
        for user, events in incoming_friend_super.items():
            lst = self.friend_super_store.setdefault(user, [])
            lst.extend(events)

        # user_status: username -> "ONLINE"/"OFFLINE"
        incoming_status: Dict[str, str] = payload.get("user_status", {}) or {}
        self.user_status.update(incoming_status)

        print("[supernode] Replica state merged.")

    def _handle_file_register(self, user, addr, payload):
        file_id = payload["file_id"]
        name = payload["name"]
        size = payload["size"]
        sha256 = payload["sha256"]
        allowed = set(payload.get("allowed_users", []))

        if file_id not in self.files:
            self.files[file_id] = {
                "name": name,
                "size": size,
                "sha256": sha256,
                "owner": user,
                "allowed": allowed,
                "seeders": set([user]),
            }
        else:
            self.files[file_id]["allowed"].update(allowed)
            self.files[file_id]["seeders"].add(user)

        resp = make_envelope(
            MessageType.FILE_REGISTERED,
            "supernode",
            self.host, self.port,
            self.lamport.tick(),
            {"file_id": file_id}
        )
        self._send(resp, addr)

    def _handle_file_list(self, user, addr, payload):
        visible = []
        for fid, meta in self.files.items():
            if user in meta["allowed"]:
                visible.append({
                    "file_id": fid,
                    "name": meta["name"],
                    "size": meta["size"],
                    "owner": meta["owner"],
                    "seeders": list(meta["seeders"])
                })

        resp = make_envelope(
            MessageType.FILE_LIST_RESPONSE,
            "supernode",
            self.host, self.port,
            self.lamport.tick(),
            {"files": visible}
        )
        self._send(resp, addr)

    def _handle_file_info(self, user, addr, payload):
        file_id = payload["file_id"]
        meta = self.files.get(file_id)
        if not meta:
            resp = {"error": "not_found"}
        elif user not in meta["allowed"]:
            resp = {"error": "forbidden"}
        else:
            # Build a JSON-serialisable view
            resp = {
                "file_id": file_id,
                "name": meta["name"],
                "size": meta["size"],
                "sha256": meta["sha256"],
                "owner": meta["owner"],
                "allowed": list(meta["allowed"]),
                "seeders": list(meta["seeders"]),
            }

        env = make_envelope(
            MessageType.FILE_INFO_RESPONSE,
            "supernode",
            self.host, self.port,
            self.lamport.tick(),
            resp,
        )
        self._send(env, addr)

    def _handle_file_seed_update(self, user, addr, payload):
        file_id = payload["file_id"]
        action = payload["action"]

        if file_id not in self.files:
            return

        if action == "add":
            self.files[file_id]["seeders"].add(user)
        else:
            self.files[file_id]["seeders"].discard(user)

    def _broadcast_supernode_list(self) -> None:
        """
        Send SUPERNODE_LIST_UPDATE to all known online users with the full
        list of supernodes (including self).
        """
        # Compose list
        entries = [{"host": self.host, "port": self.port}]
        for host, port in sorted(self.peer_supernodes):
            entries.append({"host": host, "port": port})

        payload = {"supernodes": entries}
        lamport = self.lamport.tick()

        env = make_envelope(
            MessageType.SUPERNODE_LIST_UPDATE,
            "supernode",
            self.host,
            self.port,
            lamport,
            payload,
        )

        for user, (ip, port) in self.users.items():
            self._send(env, (ip, port))
        print(f"[supernode] Broadcast SUPERNODE_LIST_UPDATE to {len(self.users)} users.")

    def register_with_seed_supernode(self, seed_host: str, seed_port: int) -> None:
        """
        Inform an existing "seed" supernode about this instance so it can add us
        to its peer_supernodes set and broadcast SUPERNODE_LIST_UPDATE to clients.
        """
        payload = {"host": self.host, "port": self.port}
        env = make_envelope(
            MessageType.SUPERNODE_HELLO,
            "supernode",
            self.host,
            self.port,
            self.lamport.tick(),
            payload,
        )
        try:
            self._send(env, (seed_host, int(seed_port)))
            print(
                f"[supernode] Sent SUPERNODE_HELLO to seed {seed_host}:{seed_port} "
                f"for self {self.host}:{self.port}"
            )
        except OSError as e:
            print(
                f"[supernode] Failed to send SUPERNODE_HELLO to "
                f"{seed_host}:{seed_port}: {e!r}"
            )

    def request_full_replica_sync(self, peer):
        env = make_envelope(
            MessageType.SUPERNODE_REPLICA_REQUEST,
            "supernode",
            self.host, self.port,
            self.lamport.tick(),
            {"request": "full_state"}
        )
        self._send(env, peer)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _guess_lan_ip() -> str:
        """
        Try to determine the LAN IP address of this machine.
        Falls back to 127.0.0.1 if we can't reach an external host.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # We never actually send anything to 8.8.8.8, just use it to pick the right interface
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except OSError:
            return "127.0.0.1"
        finally:
            s.close()


def main() -> None:
    print(f"[supernode] CWD is: {os.getcwd()}")
    config = Config("config/supernode_config.json")

    host = config["bind_host"]
    base_port = int(config["bind_port"])
    # How many consecutive ports to try starting from base_port
    max_nodes = int(config.get("max_supernodes", 5))

    sn = None
    chosen_port = None
    last_err = None

    for offset in range(max_nodes):
        port = base_port + offset
        try:
            sn = Supernode(host, port)
            chosen_port = port
            print(f"[supernode] Bound supernode on {host}:{port}")
            break
        except OSError as e:
            last_err = e
            print(f"[supernode] Port {port} unavailable ({e}); trying next...")

    if sn is None:
        raise RuntimeError(
            f"Failed to bind any supernode port in range "
            f"{base_port}-{base_port + max_nodes - 1}"
        ) from last_err

    try:
        sn.start()

        # If we are *not* the seed (first) supernode, register with seed
        if chosen_port != base_port:
            # Seed host is where clients already point; for local dev just use 127.0.0.1
            seed_host = "127.0.0.1" if host == "0.0.0.0" else host
            sn.register_with_seed_supernode(seed_host, base_port)
            sn.request_full_replica_sync((seed_host, base_port))

        # Block forever until killed (Ctrl+C)
        threading.Event().wait()
    finally:
        # Make sure we always stop cleanly if this function exits
        sn.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("\n[supernode] Shutting down...")
        traceback.print_exc()
    finally:
        input("\nPress Enter to close...")
