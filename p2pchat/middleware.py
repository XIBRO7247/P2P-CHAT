import socket
import threading
import time
from queue import Queue, Empty
from typing import Any, Dict, Tuple, Optional, Set

import json

from .config_loader import Config
from .transport import UDPTransport
from .protocol import MessageType, make_envelope, sha256_hash, md5_checksum
from .lamport import LamportClock
from .room_manager import RoomManager
from .storage import Storage
from .chunk_manager import ChunkManager
from .replication_manager import ReplicationManager
from .heartbeat_manager import HeartbeatManager
from .friend_manager import FriendManager
from .dm_history import DMHistoryManager
from .room_history import RoomHistoryManager


class Middleware:
    """Core middleware logic for a peer node."""

    def __init__(self, username: str, config: Config):
        self.username = username
        self.config = config

        # Debug flag – if missing or false, only user-facing messages are printed.
        raw_debug = config.get("debug", False)
        if isinstance(raw_debug, str):
            self.debug = raw_debug.strip().lower() in {"1", "true", "yes", "on"}
        else:
            self.debug = bool(raw_debug)

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

        # Friend / user state manager (owns persistence for friends + addresses)
        self.friend_manager = FriendManager(self.username, self.local_ip, self.listen_port)

        # Persistent history managers (per local user)
        state_dir = config.get("state_dir", "state")
        self.dm_history = DMHistoryManager(self.username, state_dir)
        self.room_history = RoomHistoryManager(self.username, state_dir)

        # Use FriendManager's cache as our user_cache
        self.user_cache: Dict[str, Tuple[str, int]] = self.friend_manager.user_cache

        # Command + event queues
        self.command_queue: Queue = Queue()
        self.event_queue: Queue = self.transport.incoming_queue

        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._running = threading.Event()

        # Pending friend-intro (friend-of-a-friend) lookups:
        # target_username -> {
        #   "waiting_for": set(friend_names),
        #   "got_helper": bool,
        #   "deadline": float
        # }
        self.pending_intros: Dict[str, Dict[str, Any]] = {}
        self.intro_timeout_sec = float(config.get("friend_intro_timeout_sec", 2.0))

        # Live listeners for DMs and rooms: peer/room_id -> list[Queue]
        self.dm_listeners: Dict[str, list[Queue]] = {}
        self.room_listeners: Dict[str, list[Queue]] = {}

        # Pending DMs we are holding for ourselves or others:
        # target_username -> list[{from_user, to_user, text, timestamp}]
        self.pending_dms: Dict[str, list[Dict[str, Any]]] = {}

        # Pending friend events (requests/responses) we are holding for others:
        # for_user -> list[{
        #   "kind": "request"|"response",
        #   "from_user": str,
        #   "accepted": Optional[bool],  # for responses
        #   "timestamp": int
        # }]
        self.pending_friend_events: Dict[str, list[Dict[str, Any]]] = {}

        # Simple local presence cache (best-effort, supernode is the source of truth)
        # username -> "ONLINE" / "OFFLINE"
        self.presence: Dict[str, str] = {}

        # Retransmit buffer: msg_id -> {
        #   "envelope": Dict[str, Any],
        #   "addr": (ip, port),
        #   "timestamp": float,
        #   "attempts": int,
        # }
        self._retransmit_buffer: Dict[str, Dict[str, Any]] = {}
        self._retransmit_lock = threading.Lock()
        self._retransmit_ttl = float(config.get("retransmit_ttl_sec", 10.0))
        self._retransmit_max_attempts = int(config.get("retransmit_max_attempts", 3))

    # ---------- Lifecycle ----------

    def start(self) -> None:
        self.transport.start()
        self._running.set()
        self._worker_thread.start()
        self._enqueue(("register", {}))

    def stop(self) -> None:
        # Before we shut down, try to hand off any pending *friend events* we hold
        self._redistribute_pending_friend_events_before_shutdown()
        # Before we shut down, try to hand off any pending DMs we are holding
        self._redistribute_pending_dms_before_shutdown()
        # Announce going offline
        self._broadcast_presence("OFFLINE")
        self._running.clear()
        self.transport.stop()

    # ---------- Logging helpers ----------

    def _debug(self, msg: str) -> None:
        """Internal debug logging; suppressed when debug flag is false."""
        if self.debug:
            print(msg)

    # ---------- Retransmission helpers ----------

    def _register_retransmit(self, env: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """
        Store a sent envelope so it can be resent if we later get a HASH_ERROR
        referring to its msg_id.
        """
        msg_id = env.get("msg_id")
        if not msg_id:
            return

        entry = {
            "envelope": env,
            "addr": addr,
            "timestamp": time.time(),
            "attempts": 0,
        }

        with self._retransmit_lock:
            # Do not reset attempts if we already have an entry for this msg_id.
            if msg_id not in self._retransmit_buffer:
                self._retransmit_buffer[msg_id] = entry

    def _send_envelope(
        self,
        env: Dict[str, Any],
        addr: Tuple[str, int],
        track: bool = True,
    ) -> None:
        """
        Thin wrapper around transport.send_raw that also records the message
        in the retransmit buffer (unless track=False).
        """
        self.transport.send_raw(env, addr)
        if track:
            self._register_retransmit(env, addr)

    def _cleanup_retransmit_buffer(self) -> None:
        """
        Periodically prune old / exhausted retransmit entries.
        """
        now = time.time()
        with self._retransmit_lock:
            to_delete = []
            for msg_id, entry in self._retransmit_buffer.items():
                age = now - entry.get("timestamp", now)
                attempts = entry.get("attempts", 0)
                if age > self._retransmit_ttl or attempts >= self._retransmit_max_attempts:
                    to_delete.append(msg_id)
            for msg_id in to_delete:
                self._retransmit_buffer.pop(msg_id, None)

    # ---------- Hash error helper ----------

    def _send_hash_error(
        self,
        addr: Tuple[str, int],
        peer_user: Optional[str],
        reason: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send a HASH_ERROR back to whoever sent us a corrupted / unverified envelope.
        """
        payload: Dict[str, Any] = {
            "reason": reason,
        }
        if peer_user:
            payload["peer"] = peer_user
        if details:
            payload["details"] = details

        env = make_envelope(
            MessageType.HASH_ERROR,
            self.username,
            self.local_ip,
            self.listen_port,
            self.lamport.tick(),
            payload,
        )
        try:
            # Never track HASH_ERROR itself to avoid loops.
            self._send_envelope(env, addr, track=False)
            self._debug(
                f"[middleware] Sent HASH_ERROR to {addr[0]}:{addr[1]} "
                f"(peer={peer_user}, reason={reason}, details={details})"
            )
        except Exception as e:
            self._debug(f"[middleware] Failed to send HASH_ERROR to {addr}: {e!r}")

    # ---------- External API (CLI) ----------

    def join_room(self, room_id: str) -> None:
        self._enqueue(("join_room", {"room_id": room_id}))

    def send_room_message(self, room_id: str, text: str) -> None:
        self._enqueue(("send_room_msg", {"room_id": room_id, "text": text}))

    def lookup_user(self, target: str) -> None:
        self._enqueue(("lookup_user", {"target": target}))

    # Friend system
    def send_friend_request(self, target: str) -> None:
        self._enqueue(("friend_request", {"target": target}))

    def send_friend_request_via(self, mutual: str, target: str) -> None:
        """Optional explicit 'via' API if needed later by the CLI."""
        self._enqueue(("friend_request_via", {"mutual": mutual, "target": target}))

    def accept_friend(self, user: str) -> None:
        self._enqueue(("friend_accept", {"user": user}))

    def reject_friend(self, user: str) -> None:
        self._enqueue(("friend_reject", {"user": user}))

    def unfriend(self, user: str) -> None:
        self._enqueue(("unfriend", {"user": user}))

    # Direct messages
    def send_direct_message(self, target: str, text: str) -> None:
        self._enqueue(("send_dm", {"target": target, "text": text}))

    def get_dm_history(self, peer: str):
        """Used by the CLI to render DM conversation view."""
        return self.dm_history.get_history(peer)

    # Room history (for /room msg sessions)
    def get_room_history(self, room_id: str):
        """Used by the CLI to render room conversation view."""
        return self.room_history.get_history(room_id)

    # ---------- Listener registration (for CLI live views) ----------

    def register_dm_listener(self, peer: str) -> Queue:
        """Return a queue that will receive (sender, text) for DMs with 'peer'."""
        q: Queue = Queue()
        self.dm_listeners.setdefault(peer, []).append(q)
        return q

    def unregister_dm_listener(self, peer: str, q: Queue) -> None:
        lst = self.dm_listeners.get(peer)
        if not lst:
            return
        if q in lst:
            lst.remove(q)
        if not lst:
            self.dm_listeners.pop(peer, None)

    def register_room_listener(self, room_id: str) -> Queue:
        """Return a queue that will receive (sender, text) for messages in 'room_id'."""
        q: Queue = Queue()
        self.room_listeners.setdefault(room_id, []).append(q)
        return q

    def unregister_room_listener(self, room_id: str, q: Queue) -> None:
        lst = self.room_listeners.get(room_id)
        if not lst:
            return
        if q in lst:
            lst.remove(q)
        if not lst:
            self.room_listeners.pop(room_id, None)

    # Internal notifiers

    def _notify_dm_listeners(self, peer: str, sender: str, text: str) -> None:
        queues = self.dm_listeners.get(peer)
        if not queues:
            return
        for q in list(queues):
            try:
                q.put_nowait((sender, text))
            except Exception:
                pass

    def _notify_room_listeners(self, room_id: str, sender: str, text: str) -> None:
        queues = self.room_listeners.get(room_id)
        if not queues:
            return
        for q in list(queues):
            try:
                q.put_nowait((sender, text))
            except Exception:
                pass

    # ---------- Internal plumbing ----------

    def _enqueue(self, cmd) -> None:
        self.command_queue.put(cmd)

    def _worker_loop(self) -> None:
        while self._running.is_set():
            # ---- Commands from CLI / external API ----
            try:
                cmd = self.command_queue.get(timeout=0.1)
            except Empty:
                cmd = None
            except Exception as e:
                print(f"[middleware] Error getting command: {e!r}")
                cmd = None

            if cmd is not None:
                try:
                    self._handle_command(cmd)
                except Exception as e:
                    print(f"[middleware] Error handling command {cmd!r}: {e!r}")

            # ---- Network events: drain the queue ----
            while True:
                try:
                    env, addr = self.event_queue.get_nowait()
                except Empty:
                    break
                except Exception as e:
                    print(f"[middleware] Error getting event: {e!r}")
                    break

                try:
                    self._handle_event(env, addr)
                except Exception as e:
                    print(f"[middleware] Error handling event: {e!r}")

            # Timeouts for friend-intro attempts
            self._check_intro_timeouts()
            # Prune old retransmit entries
            self._cleanup_retransmit_buffer()

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
        elif name == "friend_request_via":
            self._do_friend_request_via(args["mutual"], args["target"])
        elif name == "friend_accept":
            self._do_friend_accept(args["user"])
        elif name == "friend_reject":
            self._do_friend_reject(args["user"])
        elif name == "unfriend":
            self._do_unfriend(args["user"])
        elif name == "send_dm":
            self._do_send_dm(args["target"], args["text"])
        else:
            self._debug(f"[middleware] Unknown command: {name}")

    def _handle_event(self, envelope: Dict[str, Any], addr) -> None:
        msg_type = envelope.get("type")
        payload = envelope.get("payload", {}) or {}
        src_user = envelope.get("src_user")
        msg_id = envelope.get("msg_id")

        # -------- Integrity check for all incoming messages ----------
        # Skip HASH_ERROR itself to avoid endless loops; still processed below.
        if msg_type != MessageType.HASH_ERROR.value:
            expected_checksum = envelope.get("checksum")
            if expected_checksum is None:
                self._debug(
                    f"[middleware] Dropping message from {src_user} at {addr}: "
                    f"missing checksum."
                )
                self._send_hash_error(
                    addr,
                    src_user,
                    reason="missing_checksum",
                    details={
                        "msg_type": msg_type,
                        "msg_id": msg_id,
                    },
                )
                return

            try:
                payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
                computed = md5_checksum(payload_bytes)
            except Exception as e:
                self._debug(
                    f"[middleware] Error computing checksum for message from "
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
                self._debug(
                    f"[middleware] Checksum mismatch from {src_user} at {addr}: "
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

        # Lamport only updated for messages we trust
        lamport_val = envelope.get("lamport", 0)
        self.lamport.update(lamport_val)

        if msg_type == MessageType.REGISTER_ACK.value:
            print("[middleware] Registration successful.")

        elif msg_type == MessageType.REGISTER_FAIL.value:
            print("[middleware] Registration failed (username taken?)")

        elif msg_type == MessageType.JOIN_ROOM_ACK.value:
            room_id = payload.get("room_id")
            members = set(payload.get("members", []))
            if hasattr(self.room_manager, "update_members"):
                self.room_manager.update_members(room_id, members)
            else:
                for m in self.room_manager.get_peers_in_room(room_id).copy():
                    self.room_manager.remove_peer_from_room(room_id, m)
                for m in members:
                    self.room_manager.add_peer_to_room(room_id, m)
            print(f"[middleware] Joined room {room_id}, members = {members}")
            self._on_join_room_ack(room_id, members)

        elif msg_type == MessageType.FRIEND_REQUEST.value:
            msg = self.friend_manager.on_friend_request(payload)
            if msg:
                print(f"[middleware] {msg}")

        elif msg_type == MessageType.FRIEND_RESPONSE.value:
            accepted, msg = self.friend_manager.on_friend_response(payload)
            # If supernode sent a synthetic reject because target is offline/unknown,
            # FriendManager will see accepted=False and from_user=<target>.
            if msg:
                print(f"[middleware] {msg}")
            # If we ever add queued DMs, we could flush them here on accepted=True.

        elif msg_type == MessageType.LOOKUP_RESPONSE.value:
            user = payload.get("user")
            found = payload.get("found", False)
            if not user:
                return
            if not found:
                print(f"[middleware] LOOKUP: user {user} is currently offline or unknown.")
                # Best-effort presence hint
                self.presence[user] = "OFFLINE"
                return
            ip = payload.get("ip")
            port = payload.get("port")
            if not ip or not port:
                print(f"[middleware] LOOKUP: malformed response for {user}.")
                return
            self.user_cache[user] = (ip, int(port))
            self.presence[user] = "ONLINE"
            self._debug(f"[middleware] Refreshed address for {user}: {ip}:{port}")

        elif msg_type == MessageType.CHAT.value:
            text = payload.get("text")
            src_user = envelope.get("src_user")
            room_id = payload.get("room_id")
            target = payload.get("target")

            # DM if:
            #   - explicit dm flag, OR
            #   - no room_id AND target == me
            dm_flag = bool(payload.get("dm"))
            is_dm = dm_flag or (room_id is None and target == self.username)

            if is_dm:
                if src_user and text is not None:
                    peer = src_user
                    try:
                        self.dm_history.log_incoming(peer, src_user, text)
                        self._notify_dm_listeners(peer, src_user, text)
                    except Exception as e:
                        print(f"[middleware] Error logging DM from {src_user}: {e!r}")
            else:
                if room_id and src_user and text is not None:
                    # Track active speakers as room members
                    self.room_manager.add_peer_to_room(room_id, src_user)
                    self.room_history.log_incoming(room_id, src_user, text)
                    self._notify_room_listeners(room_id, src_user, text)
                    # Always show actual chat lines
                    print(f"[{room_id}] <{src_user}> {text}")

        elif msg_type == MessageType.NEW_CHUNK.value:
            # NEW_CHUNK now drives replication tracking and optional replication.
            room_id = payload.get("room_id")
            chunk_id = payload.get("chunk_id")
            src_user = envelope.get("src_user")
            self._debug(f"[middleware] NEW_CHUNK announcement: {payload}")
            if room_id and chunk_id and src_user:
                # Record that src_user holds this chunk.
                self.replication_manager.register_replica(chunk_id, src_user)

                # If we are in this room and chunk is under-replicated,
                # consider replicating it locally.
                if self.username in self.room_manager.get_peers_in_room(room_id):
                    if self.username not in self.replication_manager.get_replicas(chunk_id):
                        if self.replication_manager.needs_more_replicas(chunk_id):
                            threading.Thread(
                                target=self._replicate_chunk_if_needed,
                                args=(room_id, chunk_id, src_user),
                                daemon=True,
                            ).start()

        elif msg_type == MessageType.FRIEND_INTRO.value:
            self._handle_friend_intro(envelope)

        elif msg_type == MessageType.FRIEND_INTRO_RESULT.value:
            self._handle_friend_intro_result(envelope)

        # ---- Heartbeats for IP/port identity checks ----
        elif msg_type == MessageType.PARTNER_HEARTBEAT.value:
            src_user = envelope.get("src_user")
            if src_user:
                self.partner_heartbeats.register_heartbeat(src_user)
                ip, port = addr
                self.user_cache[src_user] = (ip, int(port))

            lamport = self.lamport.tick()
            ack_payload: Dict[str, Any] = {}
            ack = make_envelope(
                MessageType.PARTNER_HEARTBEAT_ACK,
                self.username,
                self.local_ip,
                self.listen_port,
                lamport,
                ack_payload,
            )
            # Do not track heartbeats/acks in retransmit buffer.
            self._send_envelope(ack, addr, track=False)

        elif msg_type == MessageType.PARTNER_HEARTBEAT_ACK.value:
            src_user = envelope.get("src_user")
            if src_user:
                self.partner_heartbeats.register_heartbeat(src_user)
                ip, port = addr
                self.user_cache[src_user] = (ip, int(port))

        # ---- Friend address queries / updates ----
        elif msg_type == MessageType.FRIEND_ADDR_QUERY.value:
            self._handle_friend_addr_query(envelope, addr)

        elif msg_type == MessageType.FRIEND_ADDR_ANSWER.value:
            # Answers are consumed by the synchronous resolver.
            pass

        elif msg_type == MessageType.FRIEND_ADDR_UPDATE.value:
            self._handle_friend_addr_update(envelope, addr)

        # ---- Chunk sync (history) ----
        elif msg_type == MessageType.CHUNK_REQUEST.value:
            self._handle_chunk_request(envelope, addr)

        elif msg_type == MessageType.CHUNK_RESPONSE.value:
            # CHUNK_RESPONSE is used by synchronous history sync loops,
            # so we don't do generic handling here.
            pass

        elif msg_type == MessageType.HASH_ERROR.value:
            self._handle_hash_error(envelope, addr)

        # ---- Presence / pending DM orchestration ----
        elif msg_type == MessageType.USER_STATUS.value:
            status = payload.get("status")
            src_user = envelope.get("src_user")
            if src_user and status:
                self.presence[src_user] = status
                self._debug(f"[middleware] USER_STATUS from {src_user}: {status}")

                # If a friend we have locally queued DMs for has just come online,
                # try to flush our *own* pending DMs directly to them.
                if (
                    status == "ONLINE"
                    and src_user != self.username
                    and src_user in self.pending_dms
                ):
                    self._flush_local_outgoing_dms_to(src_user)

        elif msg_type == MessageType.PENDING_DM_TRANSFER.value:
            self._handle_pending_dm_transfer(envelope, addr)

        elif msg_type == MessageType.PENDING_DM_DELIVER_REQUEST.value:
            self._handle_pending_dm_deliver_request(envelope, addr)

        elif msg_type == MessageType.PENDING_DM_HOLDER_UPDATE.value:
            # This is meant for supernode; peers can ignore.
            self._debug(f"[middleware] Ignoring PENDING_DM_HOLDER_UPDATE (supernode-only).")

        elif msg_type == MessageType.PENDING_DM_STORE.value:
            # Also supernode-only; ignore on peers.
            self._debug(f"[middleware] Ignoring PENDING_DM_STORE (supernode-only).")

        elif msg_type == MessageType.PENDING_DM_DELIVERED.value:
            # Supernode consumes this; peers may see it but can ignore.
            self._debug(f"[middleware] Ignoring PENDING_DM_DELIVERED (supernode-only).")

        # ---- Pending friend orchestration (holder side) ----
        elif msg_type == MessageType.PENDING_FRIEND_TRANSFER.value:
            # Supernode (or another holder) is giving us friend events to hold
            self._handle_pending_friend_transfer(envelope, addr)

        elif msg_type == MessageType.PENDING_FRIEND_DELIVER_REQUEST.value:
            # Supernode says: for_user is online now, deliver what you hold
            self._handle_pending_friend_deliver_request(envelope, addr)

        elif msg_type == MessageType.PENDING_FRIEND_HOLDER_UPDATE.value:
            # Metadata-only, meant for supernode’s internal bookkeeping
            self._debug(
                "[middleware] Ignoring PENDING_FRIEND_HOLDER_UPDATE (supernode-only)."
            )

        elif msg_type == MessageType.PENDING_FRIEND_STORE.value:
            # Last-resort storage at supernode; peers can ignore
            self._debug(
                "[middleware] Ignoring PENDING_FRIEND_STORE (supernode-only)."
            )

        elif msg_type == MessageType.PENDING_FRIEND_DELIVERED.value:
            # Acks consumed by supernode
            self._debug(
                "[middleware] Ignoring PENDING_FRIEND_DELIVERED (supernode-only)."
            )

    def _handle_hash_error(self, envelope: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """
        Respond to a HASH_ERROR from a peer: if it references a msg_id we
        still have in our retransmit buffer, try to resend that message.
        """
        src_user = envelope.get("src_user")
        payload = envelope.get("payload", {}) or {}
        reason = payload.get("reason")
        details = payload.get("details", {}) or {}

        original_msg_id = details.get("msg_id")
        if not original_msg_id:
            self._debug(
                f"[middleware] Received HASH_ERROR from {src_user} at {addr} "
                f"without msg_id (reason={reason}, details={details})"
            )
            return

        with self._retransmit_lock:
            entry = self._retransmit_buffer.get(original_msg_id)

        if not entry:
            self._debug(
                f"[middleware] HASH_ERROR from {src_user} refers to unknown msg_id "
                f"{original_msg_id} (maybe TTL expired or we never sent it)."
            )
            return

        attempts = entry.get("attempts", 0) + 1
        if attempts > self._retransmit_max_attempts:
            self._debug(
                f"[middleware] Not retransmitting msg_id {original_msg_id} to {entry.get('addr')} "
                f"– max attempts ({self._retransmit_max_attempts}) exceeded."
            )
            with self._retransmit_lock:
                self._retransmit_buffer.pop(original_msg_id, None)
            return

        entry["attempts"] = attempts
        entry["timestamp"] = time.time()

        env_to_resend = entry.get("envelope")
        dest = entry.get("addr")

        if not env_to_resend or not dest:
            self._debug(
                f"[middleware] Retransmit entry for msg_id {original_msg_id} is incomplete; "
                f"dropping."
            )
            with self._retransmit_lock:
                self._retransmit_buffer.pop(original_msg_id, None)
            return

        try:
            # Do NOT re-register in buffer; we just bump attempts above.
            self.transport.send_raw(env_to_resend, dest)
            self._debug(
                f"[middleware] Retransmitted msg_id={original_msg_id} to {dest} "
                f"(attempt {attempts}; reason={reason})"
            )
        except Exception as e:
            self._debug(
                f"[middleware] Error retransmitting msg_id {original_msg_id} to {dest}: {e!r}"
            )

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
        self._send_envelope(data, self.supernode_addr)
        # Broadcast presence as "ONLINE" to supernode, friends, and room peers.
        self._broadcast_presence("ONLINE")

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
        self._send_envelope(data, self.supernode_addr)

    def _do_send_room_msg(self, room_id: str, text: str) -> None:
        """
        P2P room send:

        - Refresh membership from supernode (JOIN_ROOM is idempotent).
        - Broadcast CHAT P2P to resolved peers (best-effort).
        - Also send CHAT to supernode for logging.
        """
        # Refresh membership snapshot (non-blocking on peers)
        self._do_join_room(room_id)

        # Now send message
        lamport = self.lamport.tick()
        payload = {
            "room_id": room_id,
            "text": text,
            "timestamp": lamport,
        }

        msg_record = {
            "room_id": room_id,
            "sender": self.username,
            "text": text,
            "lamport": lamport,
        }
        chunk_id = self.chunk_manager.add_message(room_id, msg_record)
        if chunk_id:
            # We store this chunk locally: we are a replica.
            self.replication_manager.register_replica(chunk_id, self.username)
            self._announce_new_chunk(room_id, chunk_id)

        self.room_history.log_outgoing(room_id, self.username, text)
        self._notify_room_listeners(room_id, self.username, text)
        # Also echo to our own console as chat
        print(f"[{room_id}] <{self.username}> {text}")

        data = make_envelope(
            MessageType.CHAT,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )

        peers = self.room_manager.get_peers_in_room(room_id)
        sent_peers: Set[str] = set()
        for peer in peers:
            if peer == self.username:
                continue
            # For rooms: prefer friend+room+supernode resolution, same ordering.
            addr = self._ensure_fresh_address_for_user(peer)
            if not addr:
                continue
            ip, port = addr
            try:
                self._send_envelope(data, (ip, port))
                sent_peers.add(peer)
            except Exception as e:
                self._debug(f"[middleware] Error sending room msg to {peer} at {ip}:{port}: {e!r}")

        try:
            self._send_envelope(data, self.supernode_addr)
        except Exception as e:
            self._debug(f"[middleware] Error sending room msg to supernode: {e!r}")

        self._debug(
            f"[middleware] Sent room message to {room_id} "
            f"(P2P to {len(sent_peers)} peers, plus supernode)."
        )

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
        self._send_envelope(data, self.supernode_addr)

    # ---------- Friend commands ----------

    def _do_friend_request(self, target: str) -> None:
        friends = self.friend_manager.friends
        online_friends = [f for f in friends if f in self.user_cache]

        if not online_friends:
            lamport = self.lamport.tick()
            env, msg = self.friend_manager.build_friend_request(target, lamport)
            if env is not None:
                self._send_envelope(env, self.supernode_addr)
            if msg:
                print(f"[middleware] {msg}")
            return

        self.pending_intros[target] = {
            "waiting_for": set(online_friends),
            "got_helper": False,
            "deadline": time.time() + self.intro_timeout_sec,
        }

        lamport = self.lamport.tick()
        payload = {
            "requester": self.username,
            "target": target,
        }
        env = make_envelope(
            MessageType.FRIEND_INTRO,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )

        for friend in online_friends:
            ip, port = self.user_cache[friend]
            self._send_envelope(env, (ip, port))

        self._debug(
            f"[middleware] Asked online friends to introduce me to {target}: "
            f"{', '.join(online_friends)}"
        )

    def _do_friend_request_via(self, mutual: str, target: str) -> None:
        if mutual not in self.friend_manager.friends:
            print(f"[middleware] Cannot use {mutual} as relay: they are not your friend.")
            return

        addr = self.user_cache.get(mutual)
        if not addr:
            print(f"[middleware] No address cached for relay friend {mutual}.")
            return

        self.pending_intros[target] = {
            "waiting_for": {mutual},
            "got_helper": False,
            "deadline": time.time() + self.intro_timeout_sec,
        }

        ip, port = addr
        lamport = self.lamport.tick()
        payload = {
            "requester": self.username,
            "target": target,
        }
        env = make_envelope(
            MessageType.FRIEND_INTRO,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, (ip, port))
        self._debug(f"[middleware] Sent friend introduction request for {target} via {mutual}.")

    def _do_friend_accept(self, user: str) -> None:
        lamport = self.lamport.tick()
        env, msg = self.friend_manager.build_friend_accept(user, lamport)
        if env is not None:
            addr = self.user_cache.get(user)
            if addr:
                try:
                    self._send_envelope(env, addr)
                    self._debug(f"[middleware] Sent FRIEND_RESPONSE directly to {user} at {addr[0]}:{addr[1]}")
                except Exception as e:
                    self._debug(f"[middleware] Error sending FRIEND_RESPONSE to {user} at {addr}: {e!r}")
            else:
                self._debug(f"[middleware] No cached address for {user}; skipping direct FRIEND_RESPONSE.")
            # also tell supernode
            try:
                self._send_envelope(env, self.supernode_addr)
            except Exception as e:
                self._debug(f"[middleware] Error sending FRIEND_RESPONSE to supernode: {e!r}")
        if msg:
            print(f"[middleware] {msg}")

    def _do_friend_reject(self, user: str) -> None:
        lamport = self.lamport.tick()
        env, msg = self.friend_manager.build_friend_reject(user, lamport)
        if env is not None:
            addr = self.user_cache.get(user)
            if addr:
                try:
                    self._send_envelope(env, addr)
                    self._debug(f"[middleware] Sent FRIEND_RESPONSE (reject) directly to {user} at {addr[0]}:{addr[1]}")
                except Exception as e:
                    self._debug(f"[middleware] Error sending FRIEND_RESPONSE reject to {user} at {addr}: {e!r}")
            else:
                self._debug(f"[middleware] No cached address for {user}; skipping direct reject.")
        if msg:
            print(f"[middleware] {msg}")

    def _do_unfriend(self, user: str) -> None:
        msg = self.friend_manager.unfriend(user)
        print(f"[middleware] {msg}")

    # ---------- Friend intro handlers ----------

    def _handle_friend_intro(self, envelope: Dict[str, Any]) -> None:
        payload = envelope.get("payload", {}) or {}
        requester = payload.get("requester")
        target = payload.get("target")
        if not requester or not target:
            return

        requester_ip = envelope.get("src_ip")
        requester_port = envelope.get("src_port")
        if not requester_ip or not requester_port:
            return

        helper_name = self.username

        can_help = (
            target in self.friend_manager.friends and
            target in self.user_cache
        )

        lamport_res = self.lamport.tick()
        result_payload = {
            "requester": requester,
            "target": target,
            "helper": helper_name,
            "can_help": bool(can_help),
        }
        result_env = make_envelope(
            MessageType.FRIEND_INTRO_RESULT,
            helper_name,
            self.local_ip,
            self.listen_port,
            lamport_res,
            result_payload,
        )
        self._send_envelope(result_env, (requester_ip, int(requester_port)))

        if not can_help:
            self._debug(
                f"[middleware] Intro: {requester} asked me to introduce them to {target}, "
                f"but I'm not friends with {target} or lack address."
            )
            return

        target_ip, target_port = self.user_cache[target]
        lamport_req = self.lamport.tick()
        forward_payload = {
            "from_user": requester,
            "friend_ip": requester_ip,
            "friend_port": int(requester_port),
            "via": helper_name,
        }
        env_req = make_envelope(
            MessageType.FRIEND_REQUEST,
            requester,
            requester_ip,
            int(requester_port),
            lamport_req,
            forward_payload,
        )
        self._send_envelope(env_req, (target_ip, target_port))

        self._debug(
            f"[middleware] Intro: relayed friend request from {requester} to {target} directly."
        )

    def _handle_friend_intro_result(self, envelope: Dict[str, Any]) -> None:
        payload = envelope.get("payload", {}) or {}
        requester = payload.get("requester")
        target = payload.get("target")
        helper = payload.get("helper") or envelope.get("src_user")
        can_help = bool(payload.get("can_help", False))

        if requester != self.username or not target or not helper:
            return

        state = self.pending_intros.get(target)
        if not state:
            return

        waiting_for: set = state["waiting_for"]
        waiting_for.discard(helper)

        if can_help:
            state["got_helper"] = True
            self._debug(
                f"[middleware] Intro: {helper} can introduce me to {target}; "
                f"waiting for their direct friend request to complete."
            )

        if not waiting_for and not state["got_helper"]:
            self._debug(
                f"[middleware] Intro: no online friends could introduce me to {target}, "
                f"falling back to supernode."
            )
            lamport = self.lamport.tick()
            env, msg = self.friend_manager.build_friend_request(target, lamport)
            if env is not None:
                self._send_envelope(env, self.supernode_addr)
            if msg:
                print(f"[middleware] {msg}")
            self.pending_intros.pop(target, None)

    def _check_intro_timeouts(self) -> None:
        if not self.pending_intros:
            return

        now = time.time()
        expired_targets = [
            t for t, st in self.pending_intros.items()
            if not st["got_helper"] and st["deadline"] <= now
        ]

        for target in expired_targets:
            st = self.pending_intros.pop(target, None)
            if not st:
                continue
            self._debug(
                f"[middleware] Intro timeout: no response from friends for {target}, "
                f"falling back to supernode."
            )
            lamport = self.lamport.tick()
            env, msg = self.friend_manager.build_friend_request(target, lamport)
            if env is not None:
                self._send_envelope(env, self.supernode_addr)
            if msg:
                print(f"[middleware] {msg}")

    # ---------- Direct messages + address resolution ----------

    def _do_send_dm(self, target: str, text: str) -> None:
        fm = self.friend_manager

        if target not in fm.friends:
            print(f"[middleware] {target} is not your friend. Use /friend add {target} first.")
            return

        cached_addr = self.user_cache.get(target)

        # Step 1: verify / resolve current address before sending
        lamport = self.lamport.tick()
        fresh_addr = self._ensure_fresh_address_for_dm(target, cached_addr)

        if fresh_addr is None:
            # Target offline / unreachable → queue locally.
            self._debug(
                f"[middleware] {target} appears offline/unreachable; "
                f"queuing DM for later delivery."
            )
            self._queue_pending_dm(target, text, lamport)
            # Log + notify so UI shows "you sent" even for deferred messages.
            self.dm_history.log_outgoing(target, self.username, text)
            self._notify_dm_listeners(target, self.username, text)
            return

        self._send_dm_direct(target, text, timestamp=lamport)

    def _send_dm_direct(self, target: str, text: str, timestamp: Optional[int] = None) -> None:
        addr = self.user_cache.get(target)
        if not addr:
            print(f"[middleware] No address for friend {target}.")
            return
        ip, port = addr
        lamport = timestamp if timestamp is not None else self.lamport.tick()
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
        self._send_envelope(data, (ip, port))

        self.dm_history.log_outgoing(target, self.username, text)
        self._notify_dm_listeners(target, self.username, text)

        print(f"[middleware] Sent DM to {target} at {ip}:{port}")

    def _queue_pending_dm(self, target: str, text: str, timestamp: int) -> None:
        """
        Queue a DM locally for later delivery (when target or a holder is online).
        """
        msgs = self.pending_dms.setdefault(target, [])
        msgs.append({
            "from_user": self.username,
            "to_user": target,
            "text": text,
            "timestamp": timestamp,
        })
        self._debug(f"[middleware] Queued pending DM to {target}: {text!r}")

    def _flush_local_outgoing_dms_to(self, target: str) -> None:
        """
        When we see USER_STATUS(target == ONLINE), try to send any DMs we
        queued *locally* for that target (i.e. messages where from_user == me).

        This does NOT touch messages we are holding on behalf of others;
        those are delivered via the holder / supernode orchestration
        (_handle_pending_dm_deliver_request).
        """
        bucket = self.pending_dms.get(target)
        if not bucket:
            return

        # Separate our own messages from any we might hold as a DM holder.
        own_msgs = [m for m in bucket if m.get("from_user") == self.username]
        if not own_msgs:
            return

        # Resolve a fresh address for the target before sending.
        cached_addr = self.user_cache.get(target)
        addr = self._ensure_fresh_address_for_dm(target, cached_addr)
        if not addr:
            self._debug(
                f"[middleware] FlushDM: {target} reported ONLINE but still cannot resolve address."
            )
            return

        ip, port = addr
        sent_any = False

        for msg in list(own_msgs):
            text = msg.get("text")
            ts = msg.get("timestamp") or self.lamport.tick()
            if text is None:
                continue

            payload = {
                "dm": True,
                "target": target,
                "text": text,
                "timestamp": ts,
            }
            env = make_envelope(
                MessageType.CHAT,
                self.username,       # logical + physical sender is us
                self.local_ip,
                self.listen_port,
                ts,
                payload,
            )
            try:
                self._send_envelope(env, (ip, port))
                sent_any = True
                # Remove this message from the bucket after successful send
                bucket.remove(msg)
            except Exception as e:
                self._debug(
                    f"[middleware] FlushDM: error sending queued DM to {target} at {ip}:{port}: {e!r}"
                )

        # If the bucket is now empty (no more local or holder messages), remove it.
        if not bucket:
            self.pending_dms.pop(target, None)

        if sent_any:
            self._debug(
                f"[middleware] FlushDM: flushed locally queued DM(s) to {target} "
                f"on USER_STATUS(ONLINE)."
            )

    def _relay_dm(self, from_user: str, to_user: str, text: str, timestamp: int) -> None:
        """
        Send a DM on behalf of 'from_user' to 'to_user'.

        Used only for delivering queued messages we are holding for others.
        Doesn't log into our DM history (it belongs to from_user).
        """
        addr = self._ensure_fresh_address_for_user(to_user)
        if not addr:
            self._debug(f"[middleware] Relay: could not resolve address for {to_user}")
            return

        ip, port = addr
        lamport = self.lamport.tick()
        payload = {
            "dm": True,
            "target": to_user,
            "text": text,
            "timestamp": timestamp,
        }
        env = make_envelope(
            MessageType.CHAT,
            from_user,  # logical sender is original user
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, (ip, port))
        self._debug(
            f"[middleware] Relayed DM on behalf of {from_user} to {to_user} at {ip}:{port}"
        )

    def _ensure_fresh_address_for_dm(
        self,
        target: str,
        cached_addr: Optional[Tuple[str, int]],
    ) -> Optional[Tuple[str, int]]:
        """
        DM path address resolution:

        1) Heartbeat on cached address (if any).
        2) Ask my friends (FRIEND_ADDR_QUERY).
        3) Ask room peers (friend-of-friend via rooms).
        4) Ask supernode.
        5) Heartbeat on resolved address.
        6) Broadcast FRIEND_ADDR_UPDATE to friends.
        """
        stale_addr = cached_addr
        if cached_addr is not None:
            ok = self._verify_identity_via_heartbeat(target, cached_addr)
            if ok:
                return cached_addr
            self._debug(f"[middleware] Cached address for {target} appears stale or mismatched.")
        else:
            self._debug(f"[middleware] No cached address for {target}; resolving...")

        # Friends first
        new_addr = self._resolve_address_via_friends(target, stale_addr)
        # Then room peers (friend-of-friend via rooms)
        if new_addr is None:
            new_addr = self._resolve_address_via_rooms(target, stale_addr)
        # Finally, supernode
        if new_addr is None:
            new_addr = self._resolve_address_via_supernode(target)

        if new_addr is None:
            return None

        if not self._verify_identity_via_heartbeat(target, new_addr):
            self._debug(f"[middleware] Identity check failed for {target} at resolved address {new_addr}.")
            return None

        self.user_cache[target] = (new_addr[0], int(new_addr[1]))
        self._broadcast_addr_update(target, new_addr[0], int(new_addr[1]), exclude={self.username})

        return new_addr

    # Generic helper for room peers (friends + rooms + supernode).
    def _ensure_fresh_address_for_user(self, user: str) -> Optional[Tuple[str, int]]:
        """
        Generic resolution for arbitrary user:

        1) Heartbeat on cached.
        2) Ask friends (if any).
        3) Ask room peers (friend-of-friend via rooms).
        4) Ask supernode.
        5) Heartbeat on resolved.
        """
        cached = self.user_cache.get(user)
        stale = cached

        if cached is not None:
            ok = self._verify_identity_via_heartbeat(user, cached)
            if ok:
                return cached
            self._debug(f"[middleware] Cached address for {user} appears stale or mismatched.")
        else:
            self._debug(f"[middleware] No cached address for {user}; resolving...")

        addr = self._resolve_address_via_friends(user, stale)
        if addr is None:
            addr = self._resolve_address_via_rooms(user, stale)
        if addr is None:
            addr = self._resolve_address_via_supernode(user)

        if addr is None:
            return None

        if not self._verify_identity_via_heartbeat(user, addr):
            self._debug(f"[middleware] Identity check failed for {user} at resolved address {addr}.")
            return None

        self.user_cache[user] = addr
        # NOTE: we do NOT broadcast addr for non-friends via FRIEND_ADDR_UPDATE,
        # because address propagation is friend-only.
        return addr

    # ---- Step 1: heartbeat-based identity verification ----

    def _verify_identity_via_heartbeat(
        self,
        expected_user: str,
        addr: Tuple[str, int],
    ) -> bool:
        ip, port = addr
        lamport = self.lamport.tick()
        payload: Dict[str, Any] = {"ping": True}
        env = make_envelope(
            MessageType.PARTNER_HEARTBEAT,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        # Do not track heartbeats in retransmit buffer.
        self._send_envelope(env, (ip, port), track=False)

        deadline = time.time() + self.partner_heartbeats.timeout
        while time.time() < deadline:
            try:
                incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
            except Empty:
                continue
            except Exception as e:
                self._debug(f"[middleware] Error during heartbeat wait: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            src_user = incoming_env.get("src_user")

            if (
                msg_type == MessageType.PARTNER_HEARTBEAT_ACK.value
                and incoming_addr == addr
            ):
                self._handle_event(incoming_env, incoming_addr)
                if src_user == expected_user:
                    return True
                self._debug(
                    f"[middleware] Heartbeat mismatch: expected {expected_user}, "
                    f"but {src_user} is now at {addr[0]}:{addr[1]}"
                )
                return False

            self._handle_event(incoming_env, incoming_addr)

        self._debug(f"[middleware] Heartbeat timeout verifying {expected_user} at {ip}:{port}")
        return False

    # ---- Step 2a: friend-based address resolution ----

    def _resolve_address_via_friends(
        self,
        target: str,
        stale_addr: Optional[Tuple[str, int]],
    ) -> Optional[Tuple[str, int]]:
        friends = list(self.friend_manager.friends)
        online_friends = [
            f for f in friends
            if f != target and f in self.user_cache
        ]

        if not online_friends:
            return None

        lamport = self.lamport.tick()
        query_payload = {
            "requester": self.username,
            "target": target,
        }
        env = make_envelope(
            MessageType.FRIEND_ADDR_QUERY,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            query_payload,
        )

        for friend in online_friends:
            ip, port = self.user_cache[friend]
            self._send_envelope(env, (ip, port))

        self._debug(
            f"[middleware] Resolving address for {target} via friends: "
            f"{', '.join(online_friends)}"
        )

        answers: list[Tuple[str, Tuple[str, int]]] = []
        deadline = time.time() + self.intro_timeout_sec

        while time.time() < deadline:
            try:
                incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
            except Empty:
                continue
            except Exception as e:
                self._debug(f"[middleware] Error during addr query wait: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            payload = incoming_env.get("payload", {}) or {}
            if msg_type == MessageType.FRIEND_ADDR_ANSWER.value:
                if (
                    payload.get("requester") == self.username
                    and payload.get("target") == target
                ):
                    ans_ip = payload.get("ip")
                    ans_port = payload.get("port")
                    helper = payload.get("helper") or incoming_env.get("src_user")
                    if ans_ip and ans_port and helper:
                        addr_tuple = (ans_ip, int(ans_port))
                        answers.append((helper, addr_tuple))

            self._handle_event(incoming_env, incoming_addr)

        for helper, addr_tuple in answers:
            if stale_addr is None or addr_tuple != stale_addr:
                self._debug(
                    f"[middleware] Friend {helper} claims {target} is at "
                    f"{addr_tuple[0]}:{addr_tuple[1]}"
                )
                return addr_tuple

        return None

    # ---- Step 2a (room-based): room graph + friends-of-friends ----

    def _get_all_room_peers(self) -> Set[str]:
        """
        Return the union of all usernames we know from any room we are in,
        excluding ourselves.
        """
        peers: Set[str] = set()
        room_members = getattr(self.room_manager, "room_members", {})
        for members in room_members.values():
            peers.update(members)
        peers.discard(self.username)
        return peers

    def _resolve_address_via_rooms(
        self,
        target: str,
        stale_addr: Optional[Tuple[str, int]],
    ) -> Optional[Tuple[str, int]]:
        """
        Ask everyone we share *any* room with:

            'Do you know the address for `target`?'

        This still respects the privacy rule because _handle_friend_addr_query()
        only answers if the helper is *friends* with `target`.
        """
        room_peers = self._get_all_room_peers()
        helpers = [
            u for u in room_peers
            if u != target and u in self.user_cache
        ]

        if not helpers:
            return None

        lamport = self.lamport.tick()
        query_payload = {
            "requester": self.username,
            "target": target,
        }
        env = make_envelope(
            MessageType.FRIEND_ADDR_QUERY,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            query_payload,
        )

        for helper in helpers:
            ip, port = self.user_cache[helper]
            self._send_envelope(env, (ip, port))

        self._debug(
            f"[middleware] Resolving address for {target} via room peers: "
            f"{', '.join(helpers)}"
        )

        answers: list[Tuple[str, Tuple[str, int]]] = []
        deadline = time.time() + self.intro_timeout_sec

        while time.time() < deadline:
            try:
                incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
            except Empty:
                continue
            except Exception as e:
                self._debug(f"[middleware] Error during room-addr query wait: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            payload = incoming_env.get("payload", {}) or {}
            if msg_type == MessageType.FRIEND_ADDR_ANSWER.value:
                if (
                    payload.get("requester") == self.username
                    and payload.get("target") == target
                ):
                    ans_ip = payload.get("ip")
                    ans_port = payload.get("port")
                    helper = payload.get("helper") or incoming_env.get("src_user")
                    if ans_ip and ans_port and helper:
                        addr_tuple = (ans_ip, int(ans_port))
                        answers.append((helper, addr_tuple))

            self._handle_event(incoming_env, incoming_addr)

        for helper, addr_tuple in answers:
            if stale_addr is None or addr_tuple != stale_addr:
                self._debug(
                    f"[middleware] Room-peer {helper} claims {target} is at "
                    f"{addr_tuple[0]}:{addr_tuple[1]}"
                )
                return addr_tuple

        return None

    def _handle_friend_addr_query(self, envelope: Dict[str, Any], addr) -> None:
        payload = envelope.get("payload", {}) or {}
        requester = payload.get("requester")
        target = payload.get("target")
        if not requester or not target:
            return

        # Privacy rule: only answer if we are actually friends with `target`.
        if target not in self.friend_manager.friends:
            return
        addr_for_target = self.user_cache.get(target)
        if not addr_for_target:
            return

        target_ip, target_port = addr_for_target
        lamport = self.lamport.tick()
        answer_payload = {
            "requester": requester,
            "target": target,
            "helper": self.username,
            "ip": target_ip,
            "port": int(target_port),
        }
        answer_env = make_envelope(
            MessageType.FRIEND_ADDR_ANSWER,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            answer_payload,
        )

        requester_ip = envelope.get("src_ip")
        requester_port = envelope.get("src_port")
        if requester_ip and requester_port:
            self._send_envelope(answer_env, (requester_ip, int(requester_port)))
            self._debug(
                f"[middleware] AddrQuery: told {requester} that {target} is at "
                f"{target_ip}:{target_port}"
            )

    # ---- Step 2b: supernode-based resolution ----

    def _resolve_address_via_supernode(self, target: str) -> Optional[Tuple[str, int]]:
        lamport = self.lamport.tick()
        payload = {"target": target}
        env = make_envelope(
            MessageType.LOOKUP_USER,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, self.supernode_addr)

        self._debug(f"[middleware] Resolving address for {target} via supernode...")

        deadline = time.time() + self.intro_timeout_sec
        result_addr: Optional[Tuple[str, int]] = None

        while time.time() < deadline:
            try:
                incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
            except Empty:
                continue
            except Exception as e:
                self._debug(f"[middleware] Error during supernode lookup wait: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            payload = incoming_env.get("payload", {}) or {}

            if msg_type == MessageType.LOOKUP_RESPONSE.value:
                user = payload.get("user")
                if user == target:
                    found = payload.get("found", False)
                    if not found:
                        self._handle_event(incoming_env, incoming_addr)
                        self._debug(f"[middleware] Supernode reports {target} offline/unknown.")
                        return None
                    ip = payload.get("ip")
                    port = payload.get("port")
                    if not ip or not port:
                        self._handle_event(incoming_env, incoming_addr)
                        self._debug(f"[middleware] Supernode returned malformed address for {target}.")
                        return None
                    result_addr = (ip, int(port))
                    self._handle_event(incoming_env, incoming_addr)
                    break

            self._handle_event(incoming_env, incoming_addr)

        if result_addr:
            self._debug(
                f"[middleware] Supernode resolved {target} to "
                f"{result_addr[0]}:{result_addr[1]}"
            )
        return result_addr

    # ---- Address update propagation ----

    def _broadcast_addr_update(
        self,
        user: str,
        ip: str,
        port: int,
        exclude: Optional[set[str]] = None,
    ) -> None:
        exclude = set(exclude or set())
        exclude.add(self.username)
        exclude.add(user)

        lamport = self.lamport.tick()
        payload = {
            "user": user,
            "ip": ip,
            "port": int(port),
            "exclude": list(exclude),
        }
        env = make_envelope(
            MessageType.FRIEND_ADDR_UPDATE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )

        for friend in self.friend_manager.friends:
            if friend in exclude:
                continue
            addr = self.user_cache.get(friend)
            if not addr:
                continue
            f_ip, f_port = addr
            self._send_envelope(env, (f_ip, int(f_port)))

    def _handle_friend_addr_update(self, envelope: Dict[str, Any], addr) -> None:
        payload = envelope.get("payload", {}) or {}
        user = payload.get("user")
        ip = payload.get("ip")
        port = payload.get("port")
        exclude_list = payload.get("exclude", []) or []

        if not user or not ip or not port:
            return

        if user not in self.friend_manager.friends:
            return

        new_addr = (ip, int(port))
        old_addr = self.user_cache.get(user)
        if old_addr != new_addr:
            self.user_cache[user] = new_addr
            self._debug(
                f"[middleware] AddrUpdate: recorded {user} at {ip}:{port} "
                f"(was {old_addr})"
            )

        exclude: set[str] = set(exclude_list)
        exclude.add(self.username)
        self._broadcast_addr_update(user, ip, int(port), exclude=exclude)

    # ---------- Room helpers ----------

    def _on_join_room_ack(self, room_id: str, members: Set[str]) -> None:
        """
        After JOIN_ROOM_ACK, spin up a background job to:
          - warm room peers (best-effort),
          - sync room history.
        This avoids blocking the main worker loop or user commands.
        """
        self._debug(f"[middleware] Room {room_id}: scheduling warm+sync after JOIN_ROOM_ACK...")
        threading.Thread(
            target=self._warm_and_sync_room,
            args=(room_id,),
            daemon=True,
        ).start()

    def _warm_and_sync_room(self, room_id: str) -> None:
        try:
            self._warm_room_peers(room_id)
            self._sync_room_history(room_id)
        except Exception as e:
            self._debug(f"[middleware] Error while warming/syncing room {room_id}: {e!r}")

    def _warm_room_peers(self, room_id: str) -> None:
        peers = self.room_manager.get_peers_in_room(room_id)
        online = []
        for peer in peers:
            if peer == self.username:
                continue
            addr = self._ensure_fresh_address_for_user(peer)
            if addr:
                online.append(peer)

        if online:
            self._debug(f"[middleware] Room {room_id}: verified peers online: {', '.join(online)}")
        else:
            self._debug(f"[middleware] Room {room_id}: no peers verified online yet.")

    def _sync_room_history(self, room_id: str) -> None:
        """
        Builder / rebuilder / updater for room chat history.

        Strategy:
          - Find online peers in the room (using our existing address resolution).
          - Ask them for an 'index' of chunks for this room (CHUNK_REQUEST with request='index').
          - Compare against our local chunks (via ChunkManager) and request any missing ones.
        """
        peers = self.room_manager.get_peers_in_room(room_id)
        helpers: Dict[str, Tuple[str, int]] = {}

        # Resolve addresses for members first (this will use heartbeat + friend/room/supernode
        # ordering just like DMs).
        for peer in peers:
            if peer == self.username:
                continue
            addr = self._ensure_fresh_address_for_user(peer)
            if addr:
                helpers[peer] = addr

        if not helpers:
            self._debug(f"[middleware] Room {room_id}: no online peers for history sync.")
            return

        # ---- Step 1: ask helpers for their index of chunks ----
        lamport = self.lamport.tick()
        index_payload = {
            "room_id": room_id,
            "request": "index",
        }
        index_env = make_envelope(
            MessageType.CHUNK_REQUEST,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            index_payload,
        )

        for peer, addr in helpers.items():
            self._send_envelope(index_env, addr)

        self._debug(
            f"[middleware] Room {room_id}: requested chunk index from "
            f"{', '.join(helpers.keys())}"
        )

        helper_indexes: Dict[str, Set[str]] = {}
        deadline = time.time() + self.intro_timeout_sec

        while time.time() < deadline:
            try:
                incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
            except Empty:
                continue
            except Exception as e:
                self._debug(f"[middleware] Error during room index sync: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            payload = incoming_env.get("payload", {}) or {}

            if (
                msg_type == MessageType.CHUNK_RESPONSE.value
                and payload.get("room_id") == room_id
                and "index" in payload
            ):
                helper = incoming_env.get("src_user")
                idx = payload.get("index") or []
                helper_indexes[helper] = set(idx)
                self._debug(
                    f"[middleware] Room {room_id}: got index from {helper} "
                    f"({len(idx)} chunks)"
                )
                # Record that helper is a replica for these chunks
                if helper and idx:
                    for cid in idx:
                        self.replication_manager.register_replica(cid, helper)
                # Do NOT generic-handle this; it's fully consumed here.
                continue

            # Everything else goes back through normal event handling
            self._handle_event(incoming_env, incoming_addr)

        # No indexes → nothing to sync
        if not helper_indexes:
            self._debug(f"[middleware] Room {room_id}: no chunk indexes received; skipping sync.")
            return

        # Union of all remote chunk ids
        all_remote_chunks: Set[str] = set()
        for idx in helper_indexes.values():
            all_remote_chunks.update(idx)

        # What we're missing (using ChunkManager knowledge)
        missing_chunks: Set[str] = set(
            self.chunk_manager.get_missing_chunks(room_id, all_remote_chunks)
        )
        if not missing_chunks:
            self._debug(f"[middleware] Room {room_id}: history up-to-date (no missing chunks).")
            return

        self._debug(
            f"[middleware] Room {room_id}: missing {len(missing_chunks)} chunks; fetching..."
        )

        # ---- Step 2: request missing chunks from helpers ----
        for helper, idx in helper_indexes.items():
            addr = helpers.get(helper)
            if not addr:
                continue

            # Only request chunks this helper claims to have AND we still miss
            needed_from_helper = sorted(idx & missing_chunks)
            if not needed_from_helper:
                continue

            for chunk_id in needed_from_helper:
                lamport = self.lamport.tick()
                req_payload = {
                    "room_id": room_id,
                    "chunk_id": chunk_id,
                }
                req_env = make_envelope(
                    MessageType.CHUNK_REQUEST,
                    self.username,
                    self.local_ip,
                    self.listen_port,
                    lamport,
                    req_payload,
                )
                self._send_envelope(req_env, addr)
                self._debug(
                    f"[middleware] Room {room_id}: requested chunk {chunk_id} "
                    f"from {helper}"
                )

                # Wait for the corresponding CHUNK_RESPONSE
                chunk_deadline = time.time() + self.intro_timeout_sec
                got_chunk = False

                while time.time() < chunk_deadline and not got_chunk:
                    try:
                        incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
                    except Empty:
                        continue
                    except Exception as e:
                        self._debug(f"[middleware] Error during chunk sync: {e!r}")
                        continue

                    msg_type = incoming_env.get("type")
                    payload2 = incoming_env.get("payload", {}) or {}

                    if (
                        msg_type == MessageType.CHUNK_RESPONSE.value
                        and payload2.get("room_id") == room_id
                        and payload2.get("chunk_id") == chunk_id
                    ):
                        messages = payload2.get("messages") or []
                        # Optional: verify sha256 content hash if present
                        expected_hash = payload2.get("hash")
                        if expected_hash is not None:
                            try:
                                data_bytes = json.dumps(messages, sort_keys=True).encode("utf-8")
                                computed_hash = sha256_hash(data_bytes)
                            except Exception as e:
                                self._debug(
                                    f"[middleware] Room {room_id}: error computing hash for "
                                    f"chunk {chunk_id} from {helper}: {e!r}"
                                )
                                self._send_hash_error(
                                    incoming_addr,
                                    incoming_env.get("src_user"),
                                    reason="chunk_hash_compute_error",
                                    details={
                                        "room_id": room_id,
                                        "chunk_id": chunk_id,
                                        "msg_id": incoming_env.get("msg_id"),
                                    },
                                )
                                continue

                            if computed_hash != expected_hash:
                                self._debug(
                                    f"[middleware] Room {room_id}: hash mismatch for chunk {chunk_id} "
                                    f"from {helper}: expected={expected_hash}, computed={computed_hash}"
                                )
                                self._send_hash_error(
                                    incoming_addr,
                                    incoming_env.get("src_user"),
                                    reason="chunk_hash_mismatch",
                                    details={
                                        "room_id": room_id,
                                        "chunk_id": chunk_id,
                                        "msg_id": incoming_env.get("msg_id"),
                                        "expected": expected_hash,
                                        "computed": computed_hash,
                                    },
                                )
                                # Do not accept this corrupted chunk; keep waiting.
                                continue

                        try:
                            # Prefer Storage.save_chunk if available
                            if hasattr(self.storage, "save_chunk"):
                                self.storage.save_chunk(room_id, chunk_id, messages)
                            self._debug(
                                f"[middleware] Room {room_id}: synced chunk {chunk_id} "
                                f"from {helper} ({len(messages)} messages)"
                            )
                            # We now hold this chunk → we are a replica.
                            self.replication_manager.register_replica(chunk_id, self.username)
                        except Exception as e:
                            self._debug(
                                f"[middleware] Error saving chunk {chunk_id} for room {room_id}: {e!r}"
                            )

                        # Remove from missing to avoid re-fetch from other helpers
                        missing_chunks.discard(chunk_id)
                        got_chunk = True
                        # Don't send this CHUNK_RESPONSE back into generic handler;
                        # we've fully consumed it here.
                        continue

                    # For anything else, dispatch normally
                    self._handle_event(incoming_env, incoming_addr)

                if not got_chunk:
                    self._debug(
                        f"[middleware] Room {room_id}: timeout waiting for chunk {chunk_id} "
                        f"from {helper}"
                    )

        if missing_chunks:
            self._debug(
                f"[middleware] Room {room_id}: still missing {len(missing_chunks)} chunks "
                f"after sync attempt."
            )
        else:
            self._debug(f"[middleware] Room {room_id}: history sync complete.")

        # Optional: rebuild ChunkManager's local index view for this room
        try:
            self.chunk_manager.rebuild_chunk_index(room_id)
        except Exception as e:
            self._debug(f"[middleware] Error rebuilding chunk index for {room_id}: {e!r}")

    # ---------- Chunk sync handlers ----------

    def _handle_chunk_request(self, envelope: Dict[str, Any], addr) -> None:
        """
        Handle CHUNK_REQUEST:

        payload:
          - room_id: str
          - request: "index"   -> reply with list of local chunk_ids
            OR
          - chunk_id: "<id>"   -> reply with that chunk's messages
        """
        payload = envelope.get("payload", {}) or {}
        room_id = payload.get("room_id")
        if not room_id:
            return

        # NEW: track who requested the chunk so we can mark them as a replica.
        requester = envelope.get("src_user")

        request_type = payload.get("request")
        if request_type == "index":
            # Return our view of chunks for this room via ChunkManager
            try:
                chunk_ids = list(self.chunk_manager.list_chunks(room_id))
            except Exception as e:
                self._debug(
                    f"[middleware] Room {room_id}: error listing chunks via ChunkManager: {e!r}"
                )
                chunk_ids = []

            # We hold these chunks locally → we are replicas for all of them.
            for cid in chunk_ids:
                self.replication_manager.register_replica(cid, self.username)

            lamport = self.lamport.tick()
            resp_payload = {
                "room_id": room_id,
                "index": chunk_ids,
            }
            env = make_envelope(
                MessageType.CHUNK_RESPONSE,
                self.username,
                self.local_ip,
                self.listen_port,
                lamport,
                resp_payload,
            )
            self._send_envelope(env, addr)
            self._debug(
                f"[middleware] Room {room_id}: answered index request "
                f"with {len(chunk_ids)} chunks."
            )
            return

        # Otherwise, expect a concrete chunk_id
        chunk_id = payload.get("chunk_id")
        if not chunk_id:
            return

        try:
            messages = self.chunk_manager.load_chunk(room_id, chunk_id)
        except Exception as e:
            self._debug(
                f"[middleware] Error loading chunk {chunk_id} for room {room_id}: {e!r}"
            )
            return

        # Record that we hold this chunk.
        self.replication_manager.register_replica(chunk_id, self.username)

        # Optional hash for integrity
        try:
            data_bytes = json.dumps(messages, sort_keys=True).encode("utf-8")
            digest = sha256_hash(data_bytes)
        except Exception:
            digest = None

        lamport = self.lamport.tick()
        resp_payload = {
            "room_id": room_id,
            "chunk_id": chunk_id,
            "messages": messages,
        }
        if digest is not None:
            resp_payload["hash"] = digest

        env = make_envelope(
            MessageType.CHUNK_RESPONSE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            resp_payload,
        )
        self._send_envelope(env, addr)
        self._debug(
            f"[middleware] Room {room_id}: served chunk {chunk_id} "
            f"({len(messages)} messages)."
        )

        # NEW: mark requester as a replica too, since we just served them the chunk.
        if requester:
            self.replication_manager.register_replica(chunk_id, requester)

    # ---------- Chunk announcement ----------

    def _announce_new_chunk(self, room_id: str, chunk_id: str) -> None:
        messages = self.chunk_manager.load_chunk(room_id, chunk_id)
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
        self._send_envelope(env, self.supernode_addr)
        self._debug(f"[middleware] Announced NEW_CHUNK {chunk_id} for room {room_id} to supernode.")

    # ---------- Replication helper ----------

    def _replicate_chunk_if_needed(self, room_id: str, chunk_id: str, src_user: str) -> None:
        """
        Background helper run when we see NEW_CHUNK and we want to
        opportunistically replicate the chunk to ourselves.

        It:
          - checks the replication threshold again,
          - fetches the chunk from src_user via CHUNK_REQUEST,
          - saves it and marks this node as a replica.
        """
        # Don't replicate if threshold already satisfied or we already hold it.
        if not self.replication_manager.needs_more_replicas(chunk_id):
            return
        if self.username in self.replication_manager.get_replicas(chunk_id):
            return

        addr = self._ensure_fresh_address_for_user(src_user)
        if not addr:
            self._debug(
                f"[middleware] Replication: cannot resolve address for {src_user} "
                f"to fetch chunk {chunk_id}."
            )
            return

        lamport = self.lamport.tick()
        req_payload = {
            "room_id": room_id,
            "chunk_id": chunk_id,
        }
        req_env = make_envelope(
            MessageType.CHUNK_REQUEST,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            req_payload,
        )
        self._send_envelope(req_env, addr)
        self._debug(
            f"[middleware] Replication: requested chunk {chunk_id} for room {room_id} "
            f"from {src_user}."
        )

        deadline = time.time() + self.intro_timeout_sec
        got_chunk = False

        while time.time() < deadline and not got_chunk:
            try:
                incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
            except Empty:
                continue
            except Exception as e:
                self._debug(f"[middleware] Error during replication chunk fetch: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            payload = incoming_env.get("payload", {}) or {}

            if (
                msg_type == MessageType.CHUNK_RESPONSE.value
                and payload.get("room_id") == room_id
                and payload.get("chunk_id") == chunk_id
            ):
                messages = payload.get("messages") or []
                expected_hash = payload.get("hash")
                if expected_hash is not None:
                    try:
                        data_bytes = json.dumps(messages, sort_keys=True).encode("utf-8")
                        computed_hash = sha256_hash(data_bytes)
                    except Exception as e:
                        self._debug(
                            f"[middleware] Replication: error computing hash for chunk {chunk_id} "
                            f"from {incoming_env.get('src_user')}: {e!r}"
                        )
                        self._send_hash_error(
                            incoming_addr,
                            incoming_env.get("src_user"),
                            reason="chunk_hash_compute_error",
                            details={
                                "room_id": room_id,
                                "chunk_id": chunk_id,
                                "msg_id": incoming_env.get("msg_id"),
                            },
                        )
                        continue

                    if computed_hash != expected_hash:
                        self._debug(
                            f"[middleware] Replication: hash mismatch for chunk {chunk_id} "
                            f"from {incoming_env.get('src_user')}: "
                            f"expected={expected_hash}, computed={computed_hash}"
                        )
                        self._send_hash_error(
                            incoming_addr,
                            incoming_env.get("src_user"),
                            reason="chunk_hash_mismatch",
                            details={
                                "room_id": room_id,
                                "chunk_id": chunk_id,
                                "msg_id": incoming_env.get("msg_id"),
                                "expected": expected_hash,
                                "computed": computed_hash,
                            },
                        )
                        # Don't accept; keep waiting.
                        continue

                try:
                    if hasattr(self.storage, "save_chunk"):
                        self.storage.save_chunk(room_id, chunk_id, messages)
                    self._debug(
                        f"[middleware] Replication: stored replicated chunk {chunk_id} "
                        f"({len(messages)} messages) for room {room_id}."
                    )
                    # We now hold this chunk.
                    self.replication_manager.register_replica(chunk_id, self.username)
                except Exception as e:
                    self._debug(
                        f"[middleware] Error saving replicated chunk {chunk_id} for room {room_id}: {e!r}"
                    )
                got_chunk = True
                # Don't generic-handle this; we've fully consumed it.
                continue

            # Everything else goes through normal handling
            self._handle_event(incoming_env, incoming_addr)

        if not got_chunk:
            self._debug(
                f"[middleware] Replication: timeout waiting for chunk {chunk_id} "
                f"from {src_user}."
            )

    # ---------- Pending DM orchestration helpers ----------

    def _choose_dm_holder(self, target: str) -> Optional[str]:
        """
        Pick an online peer to hold DMs for 'target'.

        Strategy: first try any online friends (excluding target),
                  then room peers.
        """
        candidates: list[str] = []

        # Friends first
        for f in self.friend_manager.friends:
            if f == self.username or f == target:
                continue
            if f in self.user_cache:
                candidates.append(f)

        # Then room peers (excluding ourselves and target)
        room_peers = self._get_all_room_peers()
        for p in room_peers:
            if p == self.username or p == target:
                continue
            if p in self.user_cache and p not in candidates:
                candidates.append(p)

        if not candidates:
            return None

        # Simple: just pick the first; we could randomise or load-balance later.
        return candidates[0]

    def _notify_supernode_dm_holder(self, for_user: str, holder: str) -> None:
        lamport = self.lamport.tick()
        payload = {
            "for_user": for_user,
            "holder": holder,
        }
        env = make_envelope(
            MessageType.PENDING_DM_HOLDER_UPDATE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, self.supernode_addr)
        self._debug(
            f"[middleware] Notified supernode that holder for {for_user} is now {holder}."
        )

    def _offload_pending_dm_to_supernode(self, target: str, msgs: list[Dict[str, Any]]) -> None:
        """
        LAST RESORT: send actual pending DM data to supernode for storage.
        """
        if not msgs:
            return

        lamport = self.lamport.tick()
        payload = {
            "for_user": target,
            "messages": msgs,
        }
        env = make_envelope(
            MessageType.PENDING_DM_STORE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, self.supernode_addr)
        self._debug(
            f"[middleware] Offloaded {len(msgs)} pending DM(s) for {target} to supernode (last resort)."
        )

    def _redistribute_pending_dms_before_shutdown(self) -> None:
        """
        For each target we have pending DMs for, try to hand them to an online holder peer.
        If that fails, offload them to the supernode as last resort.
        """
        if not self.pending_dms:
            return

        for target, msgs in list(self.pending_dms.items()):
            if not msgs:
                continue

            holder = self._choose_dm_holder(target)
            if holder is None:
                # No peer holder available: final fallback to supernode (data).
                self._debug(
                    f"[middleware] No holder available for {target}; "
                    f"offloading {len(msgs)} pending DMs to supernode."
                )
                self._offload_pending_dm_to_supernode(target, msgs)
                # After best-effort send, clear locally
                self.pending_dms.pop(target, None)
                continue

            # Transfer data to holder peer
            addr = self._ensure_fresh_address_for_user(holder)
            if not addr:
                self._debug(
                    f"[middleware] Could not resolve holder {holder} for {target}; "
                    f"falling back to supernode."
                )
                self._offload_pending_dm_to_supernode(target, msgs)
                self.pending_dms.pop(target, None)
                continue

            lamport = self.lamport.tick()
            payload = {
                "for_user": target,
                "messages": msgs,
            }
            env = make_envelope(
                MessageType.PENDING_DM_TRANSFER,
                self.username,
                self.local_ip,
                self.listen_port,
                lamport,
                payload,
            )
            self._send_envelope(env, addr)
            self._debug(
                f"[middleware] Transferred {len(msgs)} pending DM(s) "
                f"for {target} to holder {holder}."
            )

            # Update holder mapping at supernode (metadata only)
            self._notify_supernode_dm_holder(target, holder)

            # Clear local
            self.pending_dms.pop(target, None)

    def _handle_pending_dm_transfer(self, envelope: Dict[str, Any], addr) -> None:
        """
        Become a holder for another user's pending DMs.
        """
        payload = envelope.get("payload", {}) or {}
        for_user = payload.get("for_user")
        msgs = payload.get("messages") or []
        if not for_user or not msgs:
            return

        bucket = self.pending_dms.setdefault(for_user, [])
        bucket.extend(msgs)
        self._debug(
            f"[middleware] Became holder for {len(msgs)} pending DM(s) "
            f"for {for_user} (from {envelope.get('src_user')})."
        )

    def _handle_pending_dm_deliver_request(self, envelope: Dict[str, Any], addr) -> None:
        """
        Supernode is telling us that 'for_user' is now online; deliver any messages we hold.
        """
        payload = envelope.get("payload", {}) or {}
        for_user = payload.get("for_user")
        if not for_user:
            return

        msgs = self.pending_dms.get(for_user)
        if not msgs:
            self._debug(
                f"[middleware] DeliverRequest: no pending DMs for {for_user} on this holder."
            )
            # Still tell supernode we've got nothing for them.
            self._notify_supernode_dm_delivered(for_user, 0)
            return

        count = 0
        for msg in list(msgs):
            from_user = msg.get("from_user")
            text = msg.get("text")
            ts = msg.get("timestamp")
            if not from_user or text is None:
                continue
            self._relay_dm(from_user, for_user, text, ts)
            count += 1

        # Clear after delivery
        self.pending_dms.pop(for_user, None)
        self._notify_supernode_dm_delivered(for_user, count)

    def _notify_supernode_dm_delivered(self, for_user: str, count: int) -> None:
        lamport = self.lamport.tick()
        payload = {
            "for_user": for_user,
            "count": int(count),
        }
        env = make_envelope(
            MessageType.PENDING_DM_DELIVERED,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, self.supernode_addr)
        self._debug(
            f"[middleware] Notified supernode that we delivered {count} pending DM(s) to {for_user}."
        )

    # ---------- Pending friend orchestration helpers ----------

    def _handle_pending_friend_transfer(self, envelope: Dict[str, Any], addr) -> None:
        """
        Become a holder for another user's pending friend events.

        payload:
          - for_user: str
          - events: list of {
                "kind": "request" | "response",
                "from_user": str,
                "accepted": Optional[bool],  # only for responses
                "timestamp": int
            }
        """
        payload = envelope.get("payload", {}) or {}
        for_user = payload.get("for_user")
        events = payload.get("events") or []
        if not for_user or not events:
            return

        bucket = self.pending_friend_events.setdefault(for_user, [])
        bucket.extend(events)
        self._debug(
            f"[middleware] Became holder for {len(events)} pending friend event(s) "
            f"for {for_user} (from {envelope.get('src_user')})."
        )

    def _handle_pending_friend_deliver_request(self, envelope: Dict[str, Any], addr) -> None:
        """
        Supernode is telling us that 'for_user' is now online; deliver any
        friend requests/responses we hold for them.
        """
        payload = envelope.get("payload", {}) or {}
        for_user = payload.get("for_user")
        if not for_user:
            return

        events = self.pending_friend_events.get(for_user)
        if not events:
            self._debug(
                f"[middleware] FriendDeliverRequest: no pending friend events for {for_user}."
            )
            self._notify_supernode_friend_delivered(for_user, 0)
            return

        count = 0
        for ev in list(events):
            kind = ev.get("kind")
            from_user = ev.get("from_user")
            ts = ev.get("timestamp") or self.lamport.tick()

            if not from_user:
                continue

            if kind == "request":
                # Replay as a friend request from 'from_user' → 'for_user'
                self._relay_friend_request(from_user, for_user, ts)
                count += 1
            elif kind == "response":
                # Replay as a friend response (accept/reject) from 'from_user'
                accepted = bool(ev.get("accepted", False))
                self._relay_friend_response(from_user, for_user, accepted, ts)
                count += 1

        # Clear after delivery
        self.pending_friend_events.pop(for_user, None)
        self._notify_supernode_friend_delivered(for_user, count)

    def _relay_friend_request(
        self,
        from_user: str,
        to_user: str,
        timestamp: int,
    ) -> None:
        """
        Send a FRIEND_REQUEST on behalf of 'from_user' to 'to_user'.
        """
        addr = self._ensure_fresh_address_for_user(to_user)
        if not addr:
            self._debug(
                f"[middleware] Friend relay: could not resolve address for {to_user} "
                f"(request from {from_user})."
            )
            return

        ip, port = addr
        lamport = self.lamport.tick()
        payload: Dict[str, Any] = {
            "from_user": from_user,
            # FriendManager.on_friend_request should at least handle 'from_user'.
        }
        env = make_envelope(
            MessageType.FRIEND_REQUEST,
            from_user,            # logical sender is original user
            self.local_ip,        # physical sender is this holder
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, (ip, port))
        self._debug(
            f"[middleware] Relayed FRIEND_REQUEST from {from_user} to {to_user} "
            f"at {ip}:{port}"
        )

    def _relay_friend_response(
        self,
        from_user: str,
        to_user: str,
        accepted: bool,
        timestamp: int,
    ) -> None:
        """
        Send a FRIEND_RESPONSE (accept/reject) on behalf of 'from_user' to 'to_user'.
        """
        addr = self._ensure_fresh_address_for_user(to_user)
        if not addr:
            self._debug(
                f"[middleware] Friend relay: could not resolve address for {to_user} "
                f"(response from {from_user})."
            )
            return

        ip, port = addr
        lamport = self.lamport.tick()
        payload: Dict[str, Any] = {
            "from_user": from_user,
            "accepted": bool(accepted),
        }
        env = make_envelope(
            MessageType.FRIEND_RESPONSE,
            from_user,          # logical sender is original user
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, (ip, port))
        self._debug(
            f"[middleware] Relayed FRIEND_RESPONSE from {from_user} to {to_user} "
            f"(accepted={accepted}) at {ip}:{port}"
        )

    def _offload_pending_friend_events_to_supernode(
        self,
        for_user: str,
        events: list[Dict[str, Any]],
    ) -> None:
        """
        LAST RESORT: send pending friend events we are holding back to the supernode.
        """
        if not events:
            return

        lamport = self.lamport.tick()
        payload = {
            "for_user": for_user,
            "events": events,
        }
        env = make_envelope(
            MessageType.PENDING_FRIEND_STORE,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, self.supernode_addr)
        self._debug(
            f"[middleware] Offloaded {len(events)} pending friend event(s) for {for_user} "
            f"to supernode (last resort)."
        )

    def _redistribute_pending_friend_events_before_shutdown(self) -> None:
        """
        If we are currently acting as a holder for any users' friend events,
        offload them back to the supernode before shutdown so they are not lost.

        (We *could* also try to pick another holder peer here, but the simplest and
        safest behaviour is to rely on the supernode as the final failsafe.)
        """
        if not self.pending_friend_events:
            return

        for for_user, events in list(self.pending_friend_events.items()):
            if not events:
                continue
            self._debug(
                f"[middleware] Shutdown: offloading {len(events)} pending friend event(s) "
                f"for {for_user} back to supernode."
            )
            self._offload_pending_friend_events_to_supernode(for_user, events)
            self.pending_friend_events.pop(for_user, None)

    def _notify_supernode_friend_delivered(self, for_user: str, count: int) -> None:
        """
        Notify the supernode that we've delivered 'count' pending friend events
        for 'for_user'. Mirrors the DM delivered ack.
        """
        lamport = self.lamport.tick()
        payload = {
            "for_user": for_user,
            "count": int(count),
        }
        env = make_envelope(
            MessageType.PENDING_FRIEND_DELIVERED,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )
        self._send_envelope(env, self.supernode_addr)
        self._debug(
            f"[middleware] Notified supernode that we delivered {count} pending "
            f"friend event(s) to {for_user}."
        )

    # ---------- Presence broadcast ----------

    def _broadcast_presence(self, status: str) -> None:
        """
        Broadcast USER_STATUS to supernode, friends, and known room peers.

        status: "ONLINE" or "OFFLINE"
        """
        lamport = self.lamport.tick()
        payload = {"status": status}
        env = make_envelope(
            MessageType.USER_STATUS,
            self.username,
            self.local_ip,
            self.listen_port,
            lamport,
            payload,
        )

        # Always notify supernode
        try:
            self._send_envelope(env, self.supernode_addr)
        except Exception as e:
            self._debug(f"[middleware] Error sending USER_STATUS to supernode: {e!r}")

        # Friends we have addresses for
        for friend in self.friend_manager.friends:
            if friend == self.username:
                continue
            addr = self.user_cache.get(friend)
            if not addr:
                continue
            try:
                self._send_envelope(env, addr)
            except Exception as e:
                self._debug(f"[middleware] Error sending USER_STATUS to friend {friend}: {e!r}")

        # Room peers we have addresses for
        for peer in self._get_all_room_peers():
            if peer == self.username:
                continue
            addr = self.user_cache.get(peer)
            if not addr:
                continue
            try:
                self._send_envelope(env, addr)
            except Exception as e:
                self._debug(f"[middleware] Error sending USER_STATUS to room peer {peer}: {e!r}")

        # Track our own presence locally.
        self.presence[self.username] = status

        self._debug(f"[middleware] Broadcast presence {status} to supernode, friends, and room peers.")

    # ---------- Utility ----------

    @staticmethod
    def _local_ip() -> str:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except OSError:
            return "127.0.0.1"
        finally:
            s.close()
