# p2pchat/protocol.py

import json
import hashlib
from enum import Enum
from typing import Dict, Any, Optional


class MessageType(Enum):
    # Core registration / presence
    REGISTER_USER = "REGISTER_USER"
    REGISTER_ACK = "REGISTER_ACK"
    JOIN_ROOM = "JOIN_ROOM"
    JOIN_ROOM_ACK = "JOIN_ROOM_ACK"

    # Room chat / chunks
    CHAT = "CHAT"
    NEW_CHUNK = "NEW_CHUNK"
    CHUNK_REQUEST = "CHUNK_REQUEST"
    CHUNK_RESPONSE = "CHUNK_RESPONSE"

    # Supernode lookup
    LOOKUP_USER = "LOOKUP_USER"
    LOOKUP_RESPONSE = "LOOKUP_RESPONSE"

    # Friend system
    FRIEND_REQUEST = "FRIEND_REQUEST"
    FRIEND_RESPONSE = "FRIEND_RESPONSE"
    FRIEND_INTRO = "FRIEND_INTRO"
    FRIEND_ADDR_UPDATE = "FRIEND_ADDR_UPDATE"

    # Presence / offline DM orchestration
    USER_STATUS = "USER_STATUS"
    PENDING_DM_HOLDER_UPDATE = "PENDING_DM_HOLDER_UPDATE"
    PENDING_DM_STORE = "PENDING_DM_STORE"
    PENDING_DM_DELIVERED = "PENDING_DM_DELIVERED"

    # Offline friend orchestration (via supernode)
    PENDING_FRIEND_STORE = "PENDING_FRIEND_STORE"
    PENDING_FRIEND_DELIVERED = "PENDING_FRIEND_DELIVERED"

    # Hash / integrity signalling (optional)
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


def make_envelope(
    msg_type: MessageType,
    src_user: str,
    src_ip: str,
    src_port: int,
    lamport: int,
    payload: Optional[Dict[str, Any]] = None,
) -> bytes:
    """
    Wrap a logical message into a JSON envelope and return it as bytes.

    The envelope includes:
      - "type": MessageType.value
      - "src_user": logical username
      - "src_ip", "src_port": network source
      - "lamport": Lamport clock (for ordering)
      - "payload": arbitrary dict (or {} if None)
      - "checksum": MD5 of the payload JSON bytes

    The checksum is over the *payload* only, not the whole envelope, so peers
    can validate integrity of the actual content.
    """
    if payload is None:
        payload = {}

    # Serialize payload in a stable way for hashing
    payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
    checksum = md5_checksum(payload_bytes)

    env: Dict[str, Any] = {
        "type": msg_type.value,
        "src_user": src_user,
        "src_ip": src_ip,
        "src_port": int(src_port),
        "lamport": int(lamport),
        "payload": payload,
        "checksum": checksum,
    }
    return json.dumps(env).encode("utf-8")


def parse_envelope(data: bytes) -> Dict[str, Any]:
    """
    Parse bytes into an envelope dict.

    This **does not** enforce checksum validity – it just decodes and ensures
    there is always a 'payload' dict. Integrity checks are done at higher
    levels (middleware / supernode) where we can decide how to react
    (e.g. send HASH_ERROR, drop, retry, etc.).
    """
    env = json.loads(data.decode("utf-8"))
    payload = env.get("payload")
    if payload is None:
        env["payload"] = {}
    return env
