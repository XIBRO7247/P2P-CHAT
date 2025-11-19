# p2pchat/middleware.py

import socket
import threading
import time
from queue import Queue
from typing import Any, Dict, Tuple

import json

from .config_loader import Config
from .transport import UDPTransport
from .protocol import MessageType, make_envelope, sha256_hash
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

    # ---------- Internal plumbing ----------

    def _enqueue(self, cmd) -> None:
        self.command_queue.put(cmd)

    def _worker_loop(self) -> None:
        while self._running.is_set():
            # Commands from CLI / external API
            try:
                cmd = self.command_queue.get(timeout=0.1)
                self._handle_command(cmd)
            except Exception:
                pass

            # Network events
            try:
                env, addr = self.event_queue.get_nowait()
                self._handle_event(env, addr)
            except Exception:
                pass

            # Timeouts for friend-intro attempts
            self._check_intro_timeouts()

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
            msg = self.friend_manager.on_friend_request(payload)
            if msg:
                print(f"[middleware] {msg}")

        elif msg_type == MessageType.FRIEND_RESPONSE.value:
            accepted, msg = self.friend_manager.on_friend_response(payload)
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
                return
            ip = payload.get("ip")
            port = payload.get("port")
            if not ip or not port:
                print(f"[middleware] LOOKUP: malformed response for {user}.")
                return
            self.user_cache[user] = (ip, int(port))
            print(f"[middleware] Refreshed address for {user}: {ip}:{port}")

        elif msg_type == MessageType.CHAT.value:
            text = payload.get("text")
            src_user = envelope.get("src_user")
            if payload.get("dm"):
                # Direct message: log only; CLI DM view will render it.
                if src_user:
                    self.dm_history.log_incoming(src_user, src_user, text)
                # Do NOT print here, to avoid corrupting the DM menu UI.
            else:
                room_id = payload.get("room_id")
                if room_id and src_user:
                    # Log incoming room message for room session view
                    self.room_history.log_incoming(room_id, src_user, text)
                print(f"[{room_id}] <{src_user}> {text}")

        elif msg_type == MessageType.NEW_CHUNK.value:
            # Optional: peers could learn about chunks here if you propagate it.
            pass

        elif msg_type == MessageType.FRIEND_INTRO.value:
            # "Do you know <target>?" from requester
            self._handle_friend_intro(envelope)

        elif msg_type == MessageType.FRIEND_INTRO_RESULT.value:
            # Response from mutual friend: can/can't help
            self._handle_friend_intro_result(envelope)

        # CHUNK_REQUEST / CHUNK_RESPONSE / HEARTBEAT messages would be handled here later as needed.

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

        # Log outgoing room message to room history (for room session UI)
        self.room_history.log_outgoing(room_id, self.username, text)

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

    # ---------- Friend commands ----------

    def _do_friend_request(self, target: str) -> None:
        """
        /friend add <target> behaviour:

        1) Check which of my friends are online (have cached address).
        2) Ask each online friend via FRIEND_INTRO if they know 'target'.
        3) If at least one can help, they relay a direct FRIEND_REQUEST to 'target'.
        4) If *none* can help (all say no or timeout), fall back to supernode.
        """
        friends = self.friend_manager.friends
        online_friends = [f for f in friends if f in self.user_cache]

        if not online_friends:
            # No-one to ask → go straight to supernode (old behaviour).
            lamport = self.lamport.tick()
            env, msg = self.friend_manager.build_friend_request(target, lamport)
            if env is not None:
                self.transport.send_raw(env, self.supernode_addr)
            if msg:
                print(f"[middleware] {msg}")
            return

        # Initialise pending intro state for this target
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
            self.transport.send_raw(env, (ip, port))

        print(
            f"[middleware] Asked online friends to introduce me to {target}: "
            f"{', '.join(online_friends)}"
        )

    def _do_friend_request_via(self, mutual: str, target: str) -> None:
        """
        Optional explicit 'via' command if the CLI ever calls it:
        /friend via <mutual> <target>
        Behaves like a one-friend intro.
        """
        if mutual not in self.friend_manager.friends:
            print(f"[middleware] Cannot use {mutual} as relay: they are not your friend.")
            return

        addr = self.user_cache.get(mutual)
        if not addr:
            print(f"[middleware] No address cached for relay friend {mutual}.")
            return

        # Set up a pending intro with only this mutual
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
        self.transport.send_raw(env, (ip, port))
        print(f"[middleware] Sent friend introduction request for {target} via {mutual}.")

    def _do_friend_accept(self, user: str) -> None:
        lamport = self.lamport.tick()
        env, msg = self.friend_manager.build_friend_accept(user, lamport)
        if env is not None:
            # 1) Try to send P2P directly to the requester (e.g. cameron)
            addr = self.user_cache.get(user)
            if addr:
                self.transport.send_raw(env, addr)
                print(f"[middleware] Sent FRIEND_RESPONSE directly to {user} at {addr[0]}:{addr[1]}")
            else:
                print(f"[middleware] No cached address for {user}; skipping direct FRIEND_RESPONSE.")

            # 2) Also send to supernode as a ledger entry (optional)
            self.transport.send_raw(env, self.supernode_addr)

        if msg:
            print(f"[middleware] {msg}")

    def _do_friend_reject(self, user: str) -> None:
        lamport = self.lamport.tick()
        env, msg = self.friend_manager.build_friend_reject(user, lamport)
        if env is not None:
            # You can choose to send rejections only P2P, or also to supernode.
            addr = self.user_cache.get(user)
            if addr:
                self.transport.send_raw(env, addr)
                print(f"[middleware] Sent FRIEND_RESPONSE (reject) directly to {user} at {addr[0]}:{addr[1]}")
            else:
                print(f"[middleware] No cached address for {user}; skipping direct reject.")
            # Optionally *not* send reject to supernode if you don't want it logged.
            # self.transport.send_raw(env, self.supernode_addr)
        if msg:
            print(f"[middleware] {msg}")

    def _do_unfriend(self, user: str) -> None:
        msg = self.friend_manager.unfriend(user)
        print(f"[middleware] {msg}")

    # ---------- Friend intro handlers ----------

    def _handle_friend_intro(self, envelope: Dict[str, Any]) -> None:
        """
        Handle FRIEND_INTRO (query):

        payload:
          - requester: user who wants to befriend 'target'
          - target:    desired friend

        Behaviour on this node (mutual candidate):
          - If we're NOT friends with 'target' or don't know their address:
            send FRIEND_INTRO_RESULT(can_help=False) back to requester.
          - If we ARE friends with 'target' and have their address:
            1) send FRIEND_INTRO_RESULT(can_help=True) back to requester.
            2) send a direct FRIEND_REQUEST to 'target' on behalf of 'requester',
               including the requester's IP/port for P2P DM.
        """
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

        # Do we know 'target' as a friend and have an address?
        can_help = (
            target in self.friend_manager.friends and
            target in self.user_cache
        )

        # 1) Reply to requester with FRIEND_INTRO_RESULT
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
        self.transport.send_raw(result_env, (requester_ip, int(requester_port)))

        if not can_help:
            print(
                f"[middleware] Intro: {requester} asked me to introduce them to {target}, "
                f"but I'm not friends with {target} or lack address."
            )
            return

        # 2) We can help: send direct FRIEND_REQUEST to 'target' on behalf of 'requester'
        target_ip, target_port = self.user_cache[target]
        lamport_req = self.lamport.tick()
        forward_payload = {
            "from_user": requester,
            "friend_ip": requester_ip,
            "friend_port": int(requester_port),
            # optional extra context:
            "via": helper_name,
        }
        env_req = make_envelope(
            MessageType.FRIEND_REQUEST,
            requester,            # logical src_user is the real requester
            requester_ip,         # advertised src_ip = requester's ip
            int(requester_port),  # advertised src_port = requester's port
            lamport_req,
            forward_payload,
        )
        self.transport.send_raw(env_req, (target_ip, target_port))

        print(
            f"[middleware] Intro: relayed friend request from {requester} to {target} directly."
        )

    def _handle_friend_intro_result(self, envelope: Dict[str, Any]) -> None:
        """
        Handle FRIEND_INTRO_RESULT on the requester side.

        payload:
          - requester: should be this.username
          - target:    the user we're trying to add
          - helper:    which friend answered
          - can_help:  bool
        """
        payload = envelope.get("payload", {}) or {}
        requester = payload.get("requester")
        target = payload.get("target")
        helper = payload.get("helper") or envelope.get("src_user")
        can_help = bool(payload.get("can_help", False))

        if requester != self.username or not target or not helper:
            return

        state = self.pending_intros.get(target)
        if not state:
            # No active intro for this target anymore.
            return

        waiting_for: set = state["waiting_for"]
        waiting_for.discard(helper)

        if can_help:
            state["got_helper"] = True
            print(
                f"[middleware] Intro: {helper} can introduce me to {target}; "
                f"waiting for their direct friend request to complete."
            )

        # If we've heard back from everyone and no-one can help,
        # we fall back to the supernode.
        if not waiting_for and not state["got_helper"]:
            print(
                f"[middleware] Intro: no online friends could introduce me to {target}, "
                f"falling back to supernode."
            )
            lamport = self.lamport.tick()
            env, msg = self.friend_manager.build_friend_request(target, lamport)
            if env is not None:
                self.transport.send_raw(env, self.supernode_addr)
            if msg:
                print(f"[middleware] {msg}")
            # Remove from pending intros
            self.pending_intros.pop(target, None)

    def _check_intro_timeouts(self) -> None:
        """
        Periodically called from _worker_loop to handle intro timeouts.
        If an intro is still pending and no helper has responded by the deadline,
        we fall back to the supernode.
        """
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
            print(
                f"[middleware] Intro timeout: no response from friends for {target}, "
                f"falling back to supernode."
            )
            lamport = self.lamport.tick()
            env, msg = self.friend_manager.build_friend_request(target, lamport)
            if env is not None:
                self.transport.send_raw(env, self.supernode_addr)
            if msg:
                print(f"[middleware] {msg}")

    # ---------- Direct messages ----------

    def _do_send_dm(self, target: str, text: str) -> None:
        fm = self.friend_manager

        if target not in fm.friends:
            print(f"[middleware] {target} is not your friend. Use /friend add {target} first.")
            return

        addr = self.user_cache.get(target)
        if not addr:
            print(f"[middleware] No address cached for {target} yet.")
            return

        self._send_dm_direct(target, text)

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

        # Log outgoing DM in DMHistoryManager (persists to disk for this user)
        self.dm_history.log_outgoing(target, self.username, text)

        print(f"[middleware] Sent DM to {target} at {ip}:{port}")

    # ---------- Chunk announcement ----------

    def _announce_new_chunk(self, room_id: str, chunk_id: str) -> None:
        """
        Inform the supernode about a newly sealed chunk so it
        can track metadata and (later) assign replication targets.
        """
        messages = self.storage.load_chunk(room_id, chunk_id)
        # Serialize the messages to bytes to hash them consistently.
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
        except OSError:
            # fallback
            return "127.0.0.1"
        finally:
            s.close()
