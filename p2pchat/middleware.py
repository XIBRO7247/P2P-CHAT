# p2pchat/middleware.py

import socket
import threading
from queue import Queue
from typing import Any, Dict, Tuple

from .config_loader import Config
from .transport import UDPTransport
from .protocol import MessageType, make_envelope, sha256_hash
from .lamport import LamportClock
from .room_manager import RoomManager
from .storage import Storage
from .chunk_manager import ChunkManager
from .replication_manager import ReplicationManager
from .heartbeat_manager import HeartbeatManager


class Middleware:
    """Core middleware logic for a peer node."""

    def __init__(self, username: str, config: Config):
        self.username = username
        self.config = config

        start_port = int(config.get("listen_port_start", 50010))
        end_port = int(config.get("listen_port_end", start_port))

        self.local_ip = self._local_ip()
        self.lamport = LamportClock()

        # UDP transport with auto port selection
        self.transport = UDPTransport("0.0.0.0", start_port, end_port)
        self.listen_port = self.transport.listen_port

        # Supernode address
        self.supernode_addr = (
            config["supernode_host"],
            int(config["supernode_port"]),
        )

        # Managers
        self.room_manager = RoomManager()
        self.storage = Storage(config.get("chunk_dir", "chunks"))
        chunk_size = int(config.get("chunk_size_messages", 20))
        self.chunk_manager = ChunkManager(chunk_size, self.storage)

        rp = config.get("replication_policy", {}) or {}
        min_reps = int(rp.get("min_replicas", 2))
        self.replication_manager = ReplicationManager(min_reps)

        partner_timeout = float(config.get("partner_timeout_sec", 15.0))
        self.partner_heartbeats = HeartbeatManager(partner_timeout)

        # Queues
        self.command_queue: Queue = Queue()
        self.event_queue: Queue = self.transport.incoming_queue

        self.user_cache: Dict[str, Tuple[str, int]] = {}

        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._running = threading.Event()

        # Friend system state
        self.friends: set[str] = set()
        self.outgoing_friend_requests: set[str] = set()
        self.incoming_friend_requests: set[str] = set()
        self.pending_dm_queues: Dict[str, list[str]] = {}

    # ---------- Lifecycle ----------

    def start(self) -> None:
        self.transport.start()
        self._running.set()
        self._worker_thread.start()
        self._enqueue(("register", {}))

    def stop(self) -> None:
        self._running.clear()
        self.transport.stop()

    # ---------- External API (CLI) ----------

    def join_room(self, room_id: str) -> None:
        self._enqueue(("join_room", {"room_id": room_id}))

    def send_room_message(self, room_id: str, text: str) -> None:
        self._enqueue(("send_room_msg", {"room_id": room_id, "text": text}))

    def lookup_user(self, target: str) -> None:
        self._enqueue(("lookup_user", {"target": target}))

    # FRIEND SYSTEM (public API)
    def send_friend_request(self, target: str) -> None:
        self._enqueue(("friend_request", {"target": target}))

    def accept_friend(self, user: str) -> None:
        self._enqueue(("friend_accept", {"user": user}))

    def reject_friend(self, user: str) -> None:
        self._enqueue(("friend_reject", {"user": user}))

    def send_direct_message(self, target: str, text: str) -> None:
        self._enqueue(("send_dm", {"target": target, "text": text}))

    # ---------- Internal plumbing ----------

    def _enqueue(self, cmd) -> None:
        self.command_queue.put(cmd)

    def _worker_loop(self) -> None:
        while self._running.is_set():
            try:
                cmd = self.command_queue.get(timeout=0.1)
                self._handle_command(cmd)
            except Exception:
                pass

            try:
                env, addr = self.event_queue.get_nowait()
                self._handle_event(env, addr)
            except Exception:
                pass

    def _handle_command(self, cmd) -> None:
        name, args = cmd
        if name == "register":
            self._do_register()
        elif name == "join_room":
            self._do_join_room(args["room_id"])
        elif name == "send_room_msg":
            self._do_send_room_msg(args["room_id"], args["text"])
        elif name == "lookup_user":
            self._do_lookup_user(args["target"])
        elif name == "friend_request":
            self._do_friend_request(args["target"])
        elif name == "friend_accept":
            self._do_friend_accept(args["user"])
        elif name == "friend_reject":
            self._do_friend_reject(args["user"])
        elif name == "send_dm":
            self._do_send_dm(args["target"], args["text"])
        else:
            print(f"[middleware] Unknown command: {name}")

    def _handle_event(self, envelope: Dict[str, Any], addr) -> None:
        msg_type = envelope.get("type")
        payload = envelope.get("payload", {})
        lamport_val = envelope.get("lamport", 0)
        self.lamport.update(lamport_val)

        if msg_type == MessageType.REGISTER_ACK.value:
            print("[middleware] Registration successful.")

        elif msg_type == MessageType.REGISTER_FAIL.value:
            print("[middleware] Registration failed (username taken?)")

        elif msg_type == MessageType.JOIN_ROOM_ACK.value:
            room_id = payload.get("room_id")
            members = set(payload.get("members", []))
            self.room_manager.update_members(room_id, members)
            print(f"[middleware] Joined room {room_id}, members = {members}")


        elif msg_type == MessageType.FRIEND_REQUEST.value:

            from_user = payload.get("from_user")

            if from_user:

                self.incoming_friend_requests.add(from_user)

                # Cache sender's address if supernode provided it

                friend_ip = payload.get("friend_ip")

                friend_port = payload.get("friend_port")

                if friend_ip and friend_port:
                    self.user_cache[from_user] = (friend_ip, int(friend_port))

                print(f"[middleware] New friend request from {from_user}.")

        elif msg_type == MessageType.FRIEND_RESPONSE.value:
            from_user = payload.get("from_user")
            accepted = payload.get("accepted", False)
            if not from_user:
                return

            # Clear outgoing request entry
            self.outgoing_friend_requests.discard(from_user)

            if accepted:
                self.friends.add(from_user)
                friend_ip = payload.get("friend_ip")
                friend_port = payload.get("friend_port")
                if friend_ip and friend_port:
                    self.user_cache[from_user] = (friend_ip, int(friend_port))
                print(f"[middleware] {from_user} accepted your friend request.")

                # Flush queued DMs, if any
                queued = self.pending_dm_queues.pop(from_user, [])
                if queued:
                    print(f"[middleware] Delivering {len(queued)} queued DMs to {from_user}...")
                    for msg in queued:
                        self._send_dm_direct(from_user, msg)
            else:
                print(f"[middleware] {from_user} rejected your friend request.")
                # Clear any queued messages; they will never be delivered
                if from_user in self.pending_dm_queues:
                    dropped = len(self.pending_dm_queues.pop(from_user, []))
                    if dropped:
                        print(f"[middleware] Dropped {dropped} queued DMs to {from_user}.")

        elif msg_type == MessageType.CHAT.value:
            text = payload.get("text")
            src_user = envelope.get("src_user")
            if payload.get("dm"):
                print(f"[DM] <{src_user}> {text}")
            else:
                room_id = payload.get("room_id")
                print(f"[{room_id}] <{src_user}> {text}")

        elif msg_type == MessageType.NEW_CHUNK.value:
            # Optional: peers could learn about chunks here if you propagate it.
            pass

        else:
            # ignore other messages for now
            pass

    # ---------- Command implementations ----------

    def _do_register(self) -> None:
        lamport = self.lamport.tick()
        payload = {
            "username": self.username,
            "listen_port": self.listen_port,
        }
        data = make_envelope(
            MessageType.REGISTER_USER,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(data, self.supernode_addr)

    def _do_join_room(self, room_id: str) -> None:
        lamport = self.lamport.tick()
        payload = {"room_id": room_id}
        data = make_envelope(
            MessageType.JOIN_ROOM,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(data, self.supernode_addr)

    def _do_send_room_msg(self, room_id: str, text: str) -> None:
        lamport = self.lamport.tick()
        payload = {
            "room_id": room_id,
            "text": text,
            "timestamp": lamport,
        }

        # For now: still send CHAT via supernode (simpler); later: broadcast P2P.
        data = make_envelope(
            MessageType.CHAT,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(data, self.supernode_addr)

        # Record into our local active chunk.
        msg_record = {
            "room_id": room_id,
            "sender": self.username,
            "text": text,
            "lamport": lamport,
        }
        chunk_id = self.chunk_manager.add_message(room_id, msg_record)
        if chunk_id:
            self._announce_new_chunk(room_id, chunk_id)

    def _do_lookup_user(self, target: str) -> None:
        lamport = self.lamport.tick()
        payload = {"target": target}
        data = make_envelope(
            MessageType.LOOKUP_USER,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(data, self.supernode_addr)

    def _do_friend_request(self, target: str) -> None:
        if target == self.username:
            print("[middleware] You cannot friend yourself.")
            return
        if target in self.friends:
            print(f"[middleware] {target} is already your friend.")
            return
        if target in self.outgoing_friend_requests:
            print(f"[middleware] Friend request already sent to {target}.")
            return

        self.outgoing_friend_requests.add(target)
        lamport = self.lamport.tick()
        payload = {"target": target}
        data = make_envelope(
            MessageType.FRIEND_REQUEST,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(data, self.supernode_addr)
        print(f"[middleware] Sent friend request to {target}.")

    def _do_friend_accept(self, user: str) -> None:
        if user not in self.incoming_friend_requests:
            print(f"[middleware] No pending friend request from {user}.")
            return

        self.incoming_friend_requests.discard(user)
        self.friends.add(user)

        lamport = self.lamport.tick()
        payload = {
            "target": user,
            "accepted": True,
            "friend_ip": self.local_ip,
            "friend_port": self.listen_port,
        }
        data = make_envelope(
            MessageType.FRIEND_RESPONSE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(data, self.supernode_addr)
        print(f"[middleware] Accepted friend request from {user}.")

    def _do_friend_reject(self, user: str) -> None:
        if user not in self.incoming_friend_requests:
            print(f"[middleware] No pending friend request from {user}.")
            return

        self.incoming_friend_requests.discard(user)

        lamport = self.lamport.tick()
        payload = {
            "target": user,
            "accepted": False,
            # IMPORTANT: no friend_ip/port so we don't reveal details
        }
        data = make_envelope(
            MessageType.FRIEND_RESPONSE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(data, self.supernode_addr)
        print(f"[middleware] Rejected friend request from {user}.")

    def _do_send_dm(self, target: str, text: str) -> None:
        if target in self.friends and target in self.user_cache:
            # We can send immediately
            self._send_dm_direct(target, text)
            return

        if target in self.friends and target not in self.user_cache:
            print(f"[middleware] {target} is your friend but no address cached; try again later.")
            return

        if target in self.outgoing_friend_requests or target in self.incoming_friend_requests:
            # Queue message until they accept
            queue = self.pending_dm_queues.setdefault(target, [])
            queue.append(text)
            print(f"[middleware] Queued DM to {target} (pending friendship).")
            return

        print(f"[middleware] {target} is not your friend. Use /friend {target} first.")

    def _send_dm_direct(self, target: str, text: str) -> None:
        addr = self.user_cache.get(target)
        if not addr:
            print(f"[middleware] No address for friend {target}.")
            return
        ip, port = addr
        lamport = self.lamport.tick()
        payload = {
            "dm": True,
            "target": target,
            "text": text,
            "timestamp": lamport,
        }
        data = make_envelope(
            MessageType.CHAT,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(data, (ip, port))
        print(f"[middleware] Sent DM to {target} at {ip}:{port}")

    # ---------- Chunk announcement ----------

    def _announce_new_chunk(self, room_id: str, chunk_id: str) -> None:
        """
        Inform the supernode about a newly sealed chunk so it
        can track metadata and (later) assign replication targets.
        """
        messages = self.storage.load_chunk(room_id, chunk_id)
        # Serialize the messages to bytes to hash them consistently.
        import json
        data_bytes = json.dumps(messages, sort_keys=True).encode("utf-8")
        digest = sha256_hash(data_bytes)

        lamport = self.lamport.tick()
        payload = {
            "room_id": room_id,
            "chunk_id": chunk_id,
            "num_messages": len(messages),
            "hash": digest,
        }
        env = make_envelope(
            MessageType.NEW_CHUNK,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self.transport.send_raw(env, self.supernode_addr)
        print(f"[middleware] Announced NEW_CHUNK {chunk_id} for room {room_id} to supernode.")

    # ---------- Utility ----------

    @staticmethod
    def _local_ip() -> str:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        finally:
            s.close()
