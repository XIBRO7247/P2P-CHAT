# supernode_main.py

import threading
import socket
from typing import Dict, Any

from p2pchat.config_loader import Config
from p2pchat.protocol import MessageType, make_envelope, parse_envelope
from p2pchat.lamport import LamportClock


class Supernode:
    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, port))

        self.lamport = LamportClock()

        # username -> (ip, port)
        self.users: Dict[str, tuple[str, int]] = {}
        # room_id -> set(username)
        self.rooms: Dict[str, set[str]] = {}
        # chunk_id -> metadata dict
        self.chunks: Dict[str, Dict[str, Any]] = {}

        self._running = threading.Event()
        self._recv_queue: list[tuple[Dict[str, Any], tuple[str, int]]] = []

    def start(self):
        self._running.set()
        threading.Thread(target=self._recv_loop, daemon=True).start()
        threading.Thread(target=self._worker_loop, daemon=True).start()

        lan_ip = self._guess_lan_ip()
        print(f"[supernode] Listening on {self.host}:{self.port}")
        print(f"[supernode] LAN address (for clients): {lan_ip}:{self.port}")

    def stop(self):
        self._running.clear()
        self.sock.close()

    def _recv_loop(self):
        while self._running.is_set():
            try:
                data, addr = self.sock.recvfrom(65535)
            except OSError:
                break
            try:
                env = parse_envelope(data)
                self._recv_queue.append((env, addr))
            except Exception:
                continue

    def _worker_loop(self):
        while self._running.is_set():
            if not self._recv_queue:
                continue
            env, addr = self._recv_queue.pop(0)
            self._handle(env, addr)

    def _handle(self, env: Dict[str, Any], addr):
        msg_type = env.get("type")
        payload = env.get("payload", {})
        src_user = env.get("src_user")
        self.lamport.update(env.get("lamport", 0))

        if msg_type == MessageType.REGISTER_USER.value:
            self._handle_register(src_user, addr, payload)
        elif msg_type == MessageType.JOIN_ROOM.value:
            self._handle_join_room(src_user, addr, payload)
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
        else:
            # Ignore unknown or peer-only types such as FRIEND_INTRO, etc.
            pass

    def _send(self, data: bytes, addr) -> None:
        """Wrapper for sending a UDP datagram with basic error handling."""
        try:
            self.sock.sendto(data, addr)
        except OSError:
            # Could log this if desired
            pass

    # ---------- handlers ----------

    def _handle_register(self, username, addr, payload):
        if not username:
            return

        listen_port = payload.get("listen_port", addr[1])
        new_endpoint = (addr[0], listen_port)

        old_endpoint = self.users.get(username)
        self.users[username] = new_endpoint

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

    def _handle_chat(self, username, payload):
        room_id = payload.get("room_id")
        if not room_id:
            return

        text = payload.get("text")
        print(f"[supernode] CHAT in {room_id} from {username}: {text}")
        members = self.rooms.get(room_id, set())
        for member in members:
            if member == username:
                continue
            dest = self.users.get(member)
            if dest:
                ip, port = dest
                env = make_envelope(
                    MessageType.CHAT,
                    username,
                    ip,
                    port,
                    self.lamport.tick(),
                    payload,
                )
                self._send(env, (ip, port))

    def _handle_new_chunk(self, username, payload):
        room_id = payload.get("room_id")
        chunk_id = payload.get("chunk_id")
        if not room_id or not chunk_id:
            return

        meta = {
            "room_id": room_id,
            "owner": username,
            "num_messages": payload.get("num_messages"),
            "hash": payload.get("hash"),
        }
        self.chunks[chunk_id] = meta
        print(f"[supernode] NEW_CHUNK {chunk_id} for room {room_id} from {username}: {meta}")
        # In a later version: choose replication targets and send REPLICATE_CHUNK_TO here.

    def _handle_friend_request(self, src_user: str, payload: Dict[str, Any]) -> None:
        """
        Sender -> supernode -> recipient.

        Payload from sender: {"target": "<username>"}.
        Payload to recipient: {"from_user": src_user, "friend_ip", "friend_port"}.

        Note: src_user may be the *real requester* even if the datagram
        came from a relay peer (friend-of-a-friend flow).
        """
        target = payload.get("target")
        if not target:
            return

        dest = self.users.get(target)
        if not dest:
            print(f"[supernode] FRIEND_REQUEST from {src_user} to unknown user {target}")
            return

        # Look up the sender's endpoint so we can pass it to the recipient
        sender_ep = self.users.get(src_user)
        if not sender_ep:
            print(f"[supernode] FRIEND_REQUEST: no endpoint for src_user {src_user}")
            return
        sender_ip, sender_port = sender_ep

        ip, port = dest
        print(f"[supernode] FRIEND_REQUEST {src_user} -> {target}")

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
        Recipient -> supernode -> original requester.

        Payload from recipient: {"target": "<requester>", "accepted": bool,
                                 "friend_ip"?, "friend_port"?}.
        Payload to requester:   {"from_user": src_user, "accepted": bool,
                                 "friend_ip"?, "friend_port"?}.
        """
        target = payload.get("target")
        accepted = payload.get("accepted", False)

        if not target:
            return

        dest = self.users.get(target)
        if not dest:
            print(f"[supernode] FRIEND_RESPONSE from {src_user} to unknown user {target}")
            return

        ip, port = dest
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

        print(f"[supernode] FRIEND_RESPONSE {src_user} -> {target}, accepted={accepted}")

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
        """
        target = payload.get("target")
        if not target:
            return

        ep = self.users.get(target)
        if ep is None:
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


if __name__ == "__main__":
    config = Config("config/supernode_config.json")
    sn = Supernode(config["bind_host"], int(config["bind_port"]))
    try:
        sn.start()
        threading.Event().wait()
    except KeyboardInterrupt:
        print("\n[supernode] Shutting down...")
        sn.stop()
