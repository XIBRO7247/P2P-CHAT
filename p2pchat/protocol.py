# p2pchat/protocol.py

import json
import hashlib
from enum import Enum
from typing import Dict, Any, Optional


class MessageType(Enum):
    REGISTER_USER = "register_user"
    REGISTER_ACK = "register_ack"
    JOIN_ROOM = "join_room"
    JOIN_ROOM_ACK = "join_room_ack"
    CHAT = "chat"
    NEW_CHUNK = "new_chunk"
    LOOKUP_USER = "lookup_user"
    LOOKUP_RESPONSE = "lookup_response"

    # Rooms listing
    LIST_ROOMS = "list_rooms"
    LIST_ROOMS_RESPONSE = "list_rooms_response"

    # Room leaving
    LEAVE_ROOM = "leave_room"

    # Friend system ...
    FRIEND_REQUEST = "friend_request"
    FRIEND_RESPONSE = "FRIEND_RESPONSE"
    FRIEND_INTRO = "FRIEND_INTRO"
    FRIEND_INTRO_RESULT = "FRIEND_INTRO_RESULT"
    FRIEND_ADDR_UPDATE = "FRIEND_ADDR_UPDATE"
    FRIEND_ADDR_QUERY = "FRIEND_ADDR_QUERY"
    FRIEND_ADDR_ANSWER = "FRIEND_ADDR_ANSWER"

    # Heartbeats / identity checks
    PARTNER_HEARTBEAT = "PARTNER_HEARTBEAT"
    PARTNER_HEARTBEAT_ACK = "PARTNER_HEARTBEAT_ACK"

    # Presence / offline DM orchestration
    USER_STATUS = "USER_STATUS"
    PENDING_DM_TRANSFER = "PENDING_DM_TRANSFER"
    PENDING_DM_DELIVER_REQUEST = "PENDING_DM_DELIVER_REQUEST"
    PENDING_DM_HOLDER_UPDATE = "PENDING_DM_HOLDER_UPDATE"
    PENDING_DM_STORE = "PENDING_DM_STORE"
    PENDING_DM_DELIVERED = "PENDING_DM_DELIVERED"

    # Offline friend orchestration (via supernode + holders)
    PENDING_FRIEND_TRANSFER = "PENDING_FRIEND_TRANSFER"
    PENDING_FRIEND_DELIVER_REQUEST = "PENDING_FRIEND_DELIVER_REQUEST"
    PENDING_FRIEND_HOLDER_UPDATE = "PENDING_FRIEND_HOLDER_UPDATE"
    PENDING_FRIEND_STORE = "PENDING_FRIEND_STORE"
    PENDING_FRIEND_DELIVERED = "PENDING_FRIEND_DELIVERED"

    # Hash / integrity signalling
    HASH_ERROR = "HASH_ERROR"


def md5_checksum(data: bytes) -> str:
    """
    Compute an MD5 checksum for the given bytes.

    This is primarily to catch corruption on our UDP payloads – it's not
    intended for cryptographic security.
    """
    return hashlib.md5(data).hexdigest()


def sha256_hash(data: bytes) -> str:
    """
    Convenience SHA-256 hash, used e.g. for chunk/message history integrity.
    """
    return hashlib.sha256(data).hexdigest()


def _compute_msg_id(
    msg_type: MessageType,
    src_user: str,
    lamport: int,
    payload: Dict[str, Any],
) -> str:
    """
    Compute a deterministic message ID.

    For now we use a SHA-256 over a small, stable subset of the envelope
    fields so it's unique per (src_user, lamport, type, payload).
    """
    base = {
        "type": msg_type.value,
        "src_user": src_user,
        "lamport": int(lamport),
        "payload": payload,
    }
    data = json.dumps(base, sort_keys=True).encode("utf-8")
    return sha256_hash(data)


def make_envelope(
    msg_type: MessageType,
    src_user: str,
    src_ip: str,
    src_port: int,
    lamport: int,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Wrap a logical message into an envelope dict.

    The envelope includes:
      - "type": MessageType.value
      - "src_user": logical username
      - "src_ip", "src_port": network source (as seen by sender)
      - "lamport": Lamport clock (for ordering)
      - "payload": arbitrary dict (or {} if None)
      - "checksum": MD5 of the payload JSON bytes
      - "msg_id": deterministic message identifier (used for retransmit / HASH_ERROR)

    The checksum is over the *payload* only, not the whole envelope, so peers
    can validate integrity of the actual content.

    NOTE: This returns a Python dict. The transport layer is responsible for
    serialising to bytes on send, and for calling parse_envelope() on receive.
    """
    if payload is None:
        payload = {}

    # Serialize payload in a stable way for hashing
    payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    checksum = md5_checksum(payload_bytes)

    msg_id = _compute_msg_id(msg_type, src_user, lamport, payload)

    env: Dict[str, Any] = {
        "type": msg_type.value,
        "src_user": src_user,
        "src_ip": src_ip,
        "src_port": int(src_port),
        "lamport": int(lamport),
        "payload": payload,
        "checksum": checksum,
        "msg_id": msg_id,
    }
    return env


def parse_envelope(data: bytes) -> Dict[str, Any]:
    """
    Parse bytes into an envelope dict.

    This **does not** enforce checksum validity – it just decodes and ensures
    there is always a 'payload' dict. Integrity checks are done at higher
    levels (middleware / supernode) where we can decide how to react
    (e.g. send HASH_ERROR, drop, retry, etc.).

    'msg_id' and 'checksum' may be missing if the sender is running an older
    implementation; higher layers should tolerate that and simply skip
    retransmit / integrity features when absent.
    """
    env = json.loads(data.decode("utf-8"))
    payload = env.get("payload")
    if payload is None:
        env["payload"] = {}
    return env
