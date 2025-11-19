import socket
import threading
import time
from queue import Queue, Empty
from typing import Any, Dict, Tuple, Optional

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

        # Live listeners for DMs and rooms: peer/room_id -> list[Queue]
        self.dm_listeners: Dict[str, list[Queue]] = {}
        self.room_listeners: Dict[str, list[Queue]] = {}

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
            room_id = payload.get("room_id")
            target = payload.get("target")

            # DM if:
            #   - explicit dm flag, OR
            #   - no room_id AND target == me
            dm_flag = bool(payload.get("dm"))
            is_dm = dm_flag or (room_id is None and target == self.username)

            if is_dm:
                if src_user and text is not None:
                    # For the receiver, peer is the *other* user (src_user)
                    peer = src_user
                    try:
                        self.dm_history.log_incoming(peer, src_user, text)
                        self._notify_dm_listeners(peer, src_user, text)
                        # Do NOT print here in normal flow – the DM view handles it
                    except Exception as e:
                        print(f"[middleware] Error logging DM from {src_user}: {e!r}")
            else:
                if room_id and src_user and text is not None:
                    self.room_history.log_incoming(room_id, src_user, text)
                    self._notify_room_listeners(room_id, src_user, text)
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

        # ---- Heartbeats for IP/port identity checks ----
        elif msg_type == MessageType.PARTNER_HEARTBEAT.value:
            src_user = envelope.get("src_user")
            if src_user:
                # Mark this user alive at this moment
                self.partner_heartbeats.register_heartbeat(src_user)
                # Also refresh the address for this src_user
                ip, port = addr
                self.user_cache[src_user] = (ip, int(port))

            # Reply with ACK so the requester can confirm identity
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
            self.transport.send_raw(ack, addr)

        elif msg_type == MessageType.PARTNER_HEARTBEAT_ACK.value:
            # Just record that this peer is alive and update its address.
            src_user = envelope.get("src_user")
            if src_user:
                self.partner_heartbeats.register_heartbeat(src_user)
                ip, port = addr
                self.user_cache[src_user] = (ip, int(port))
            # Identity confirmation is handled in the synchronous verifier.

        # ---- Friend address queries / updates ----
        elif msg_type == MessageType.FRIEND_ADDR_QUERY.value:
            self._handle_friend_addr_query(envelope, addr)

        elif msg_type == MessageType.FRIEND_ADDR_ANSWER.value:
            # Answers are consumed by the synchronous resolver;
            # we don't do generic handling here.
            pass

        elif msg_type == MessageType.FRIEND_ADDR_UPDATE.value:
            self._handle_friend_addr_update(envelope, addr)

        # CHUNK_REQUEST / CHUNK_RESPONSE / HASH_ERROR could be handled later as needed.

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

        # Log outgoing room message + notify listeners
        self.room_history.log_outgoing(room_id, self.username, text)
        self._notify_room_listeners(room_id, self.username, text)

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
            # 1) Try to send P2P directly to the requester
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
            addr = self.user_cache.get(user)
            if addr:
                self.transport.send_raw(env, addr)
                print(f"[middleware] Sent FRIEND_RESPONSE (reject) directly to {user} at {addr[0]}:{addr[1]}")
            else:
                print(f"[middleware] No cached address for {user}; skipping direct reject.")
            # If you want the supernode to also know about rejections, uncomment:
            # self.transport.send_raw(env, self.supernode_addr)
        if msg:
            print(f"[middleware] {msg}")

    def _do_unfriend(self, user: str) -> None:
        msg = self.friend_manager.unfriend(user)
        print(f"[middleware] {msg}")

    # ---------- Friend intro handlers ----------

    def _handle_friend_intro(self, envelope: Dict[str, Any], addr) -> None:
        """
        Handle FRIEND_INTRO (query):

        payload:
          - requester: user who wants to befriend 'target'
          - target:    desired friend
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

    # ---------- Direct messages + address resolution ----------

    def _do_send_dm(self, target: str, text: str) -> None:
        fm = self.friend_manager

        if target not in fm.friends:
            print(f"[middleware] {target} is not your friend. Use /friend add {target} first.")
            return

        cached_addr = self.user_cache.get(target)

        # Step 1: verify / resolve current address before sending
        fresh_addr = self._ensure_fresh_address_for_dm(target, cached_addr)

        if fresh_addr is None:
            print(f"[middleware] Could not resolve a valid address for {target}; DM not sent.")
            return

        # At this point user_cache[target] should be set to fresh_addr.
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
        # Notify any active DM views for this peer
        self._notify_dm_listeners(target, self.username, text)

        print(f"[middleware] Sent DM to {target} at {ip}:{port}")

    # Core helper: Step 1 (heartbeat identity) + Step 2 (friends/supernode resolution)
    def _ensure_fresh_address_for_dm(
        self,
        target: str,
        cached_addr: Optional[Tuple[str, int]],
    ) -> Optional[Tuple[str, int]]:
        """
        Ensure that our address for `target` is still valid and belongs to `target`,
        before sending a DM.

        Returns a (ip, port) tuple if we have a fresh, verified address, else None.
        """

        # If we have a cached address, first do a heartbeat identity check.
        stale_addr = cached_addr
        if cached_addr is not None:
            ok = self._verify_identity_via_heartbeat(target, cached_addr)
            if ok:
                # Confirmed that the current occupant of (ip,port) is indeed `target`.
                return cached_addr
            # Identity mismatch or timeout: treat as stale.
            print(f"[middleware] Cached address for {target} appears stale or mismatched.")
        else:
            print(f"[middleware] No cached address for {target}; resolving...")

        # Step 2: address resolution via friends, then supernode fallback.
        new_addr = self._resolve_address_via_friends(target, stale_addr)
        if new_addr is None:
            new_addr = self._resolve_address_via_supernode(target)

        if new_addr is None:
            return None

        # Final sanity: verify identity at new address as well.
        if not self._verify_identity_via_heartbeat(target, new_addr):
            print(f"[middleware] Identity check failed for {target} at resolved address {new_addr}.")
            return None

        # Update local cache and broadcast update to friends.
        self.user_cache[target] = (new_addr[0], int(new_addr[1]))
        self._broadcast_addr_update(target, new_addr[0], int(new_addr[1]), exclude={self.username})

        return new_addr

    # ---- Step 1: heartbeat-based identity verification ----

    def _verify_identity_via_heartbeat(
        self,
        expected_user: str,
        addr: Tuple[str, int],
    ) -> bool:
        """
        Send a PARTNER_HEARTBEAT to `addr` and wait for an ACK.
        If the responder's src_user matches `expected_user`, we accept it.
        Otherwise, we treat this address as mismatched.

        This runs a small local event loop and *still* dispatches any other
        events through _handle_event, so we don't starve the rest of the system.
        """
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
        self.transport.send_raw(env, (ip, port))

        deadline = time.time() + self.partner_heartbeats.timeout
        while time.time() < deadline:
            try:
                incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
            except Empty:
                continue
            except Exception as e:
                print(f"[middleware] Error during heartbeat wait: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            src_user = incoming_env.get("src_user")

            # If it's the ACK from this address, we can make a decision.
            if (
                msg_type == MessageType.PARTNER_HEARTBEAT_ACK.value
                and incoming_addr == addr
            ):
                # Dispatch normal handling (register_heartbeat, cache update)
                self._handle_event(incoming_env, incoming_addr)
                if src_user == expected_user:
                    return True
                # ACK from some *other* user => address has been re-used
                print(
                    f"[middleware] Heartbeat mismatch: expected {expected_user}, "
                    f"but {src_user} is now at {addr[0]}:{addr[1]}"
                )
                return False

            # Otherwise, just handle the event normally.
            self._handle_event(incoming_env, incoming_addr)

        # Timeout -> we couldn't confirm.
        print(f"[middleware] Heartbeat timeout verifying {expected_user} at {ip}:{port}")
        return False

    # ---- Step 2a: friend-based address resolution ----

    def _resolve_address_via_friends(
        self,
        target: str,
        stale_addr: Optional[Tuple[str, int]],
    ) -> Optional[Tuple[str, int]]:
        """
        Ask all online friends:
          'Do you know the address for `target`?'

        Returns a candidate (ip, port) if any friend reports an address
        different from `stale_addr`; otherwise None.
        """
        friends = list(self.friend_manager.friends)
        # Exclude the target themself and any friends we don't have an address for
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
            self.transport.send_raw(env, (ip, port))

        print(
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
                print(f"[middleware] Error during addr query wait: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            payload = incoming_env.get("payload", {}) or {}
            if msg_type == MessageType.FRIEND_ADDR_ANSWER.value:
                # Only care about answers for *this* requester+target.
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
                        # We continue collecting until timeout; could early-exit if you like.

            # Dispatch everything (including answers) into the normal handler.
            self._handle_event(incoming_env, incoming_addr)

        # Select any answer that is not the stale address (if stale_addr is known).
        for helper, addr_tuple in answers:
            if stale_addr is None or addr_tuple != stale_addr:
                print(
                    f"[middleware] Friend {helper} claims {target} is at "
                    f"{addr_tuple[0]}:{addr_tuple[1]}"
                )
                return addr_tuple

        return None

    def _handle_friend_addr_query(self, envelope: Dict[str, Any], addr) -> None:
        """
        Handle FRIEND_ADDR_QUERY from one of our friends:
          - If we are NOT friends with 'target' or don't have an address:
            do nothing.
          - If we ARE friends with 'target' and have an address:
            reply with FRIEND_ADDR_ANSWER giving our current view.
        """
        payload = envelope.get("payload", {}) or {}
        requester = payload.get("requester")
        target = payload.get("target")
        if not requester or not target:
            return

        # Only respond if we're friends with target *and* know their address.
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
            self.transport.send_raw(answer_env, (requester_ip, int(requester_port)))
            print(
                f"[middleware] AddrQuery: told {requester} that {target} is at "
                f"{target_ip}:{target_port}"
            )

    # ---- Step 2b: supernode-based resolution ----

    def _resolve_address_via_supernode(self, target: str) -> Optional[Tuple[str, int]]:
        """
        Send a LOOKUP_USER to the supernode and synchronously wait
        for the LOOKUP_RESPONSE for `target`.
        """
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
        self.transport.send_raw(env, self.supernode_addr)

        print(f"[middleware] Resolving address for {target} via supernode...")

        deadline = time.time() + self.intro_timeout_sec
        result_addr: Optional[Tuple[str, int]] = None

        while time.time() < deadline:
            try:
                incoming_env, incoming_addr = self.event_queue.get(timeout=0.2)
            except Empty:
                continue
            except Exception as e:
                print(f"[middleware] Error during supernode lookup wait: {e!r}")
                continue

            msg_type = incoming_env.get("type")
            payload = incoming_env.get("payload", {}) or {}

            if msg_type == MessageType.LOOKUP_RESPONSE.value:
                user = payload.get("user")
                if user == target:
                    found = payload.get("found", False)
                    if not found:
                        self._handle_event(incoming_env, incoming_addr)
                        print(f"[middleware] Supernode reports {target} offline/unknown.")
                        return None
                    ip = payload.get("ip")
                    port = payload.get("port")
                    if not ip or not port:
                        self._handle_event(incoming_env, incoming_addr)
                        print(f"[middleware] Supernode returned malformed address for {target}.")
                        return None
                    result_addr = (ip, int(port))
                    # Let normal handler also update user_cache and print if needed.
                    self._handle_event(incoming_env, incoming_addr)
                    break

            # For all other events, handle normally.
            self._handle_event(incoming_env, incoming_addr)

        if result_addr:
            print(
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
        """
        Broadcast an address update 'user is now at ip:port' to all of *our*
        friends, except those in `exclude` (and except `user`).
        This is the recursive propagation we discussed.
        """
        exclude = set(exclude or set())
        exclude.add(self.username)  # don't send back to ourselves
        exclude.add(user)           # no need to notify the subject

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
            self.transport.send_raw(env, (f_ip, int(f_port)))

    def _handle_friend_addr_update(self, envelope: Dict[str, Any], addr) -> None:
        """
        Handle FRIEND_ADDR_UPDATE:

        payload:
          - user:    username whose address changed
          - ip/port: new address
          - exclude: usernames that must NOT be re-notified (to avoid loops)
        """
        payload = envelope.get("payload", {}) or {}
        user = payload.get("user")
        ip = payload.get("ip")
        port = payload.get("port")
        exclude_list = payload.get("exclude", []) or []

        if not user or not ip or not port:
            return

        # Only care if we're actually friends with this user.
        if user not in self.friend_manager.friends:
            # We still *see* the packet but do not store or propagate it.
            return

        new_addr = (ip, int(port))
        old_addr = self.user_cache.get(user)
        if old_addr != new_addr:
            self.user_cache[user] = new_addr
            print(
                f"[middleware] AddrUpdate: recorded {user} at {ip}:{port} "
                f"(was {old_addr})"
            )

        # Propagate to our own friends except those in the exclude set.
        exclude: set[str] = set(exclude_list)
        # Add ourselves so it doesn't bounce back.
        exclude.add(self.username)
        self._broadcast_addr_update(user, ip, int(port), exclude=exclude)

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

