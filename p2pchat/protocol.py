# p2pchat/protocol.py

import hashlib
import json
from enum import Enum
from typing import Any, Dict


class MessageType(str, Enum):
    # Control (peer <-> supernode)
    REGISTER_USER = "REGISTER_USER"
    REGISTER_ACK = "REGISTER_ACK"
    REGISTER_FAIL = "REGISTER_FAIL"

    LOOKUP_USER = "LOOKUP_USER"
    LOOKUP_RESPONSE = "LOOKUP_RESPONSE"

    JOIN_ROOM = "JOIN_ROOM"
    JOIN_ROOM_ACK = "JOIN_ROOM_ACK"

    # Friend system
    FRIEND_REQUEST = "FRIEND_REQUEST"
    FRIEND_RESPONSE = "FRIEND_RESPONSE"
    FRIEND_INTRO = "FRIEND_INTRO"   # friend-of-a-friend introduction (peer <-> peer)
    FRIEND_INTRO_RESULT = "FRIEND_INTRO_RESULT"


    # Data
    CHAT = "CHAT"

    NEW_CHUNK = "NEW_CHUNK"
    REPLICATE_CHUNK_TO = "REPLICATE_CHUNK_TO"

    CHUNK_REQUEST = "CHUNK_REQUEST"
    CHUNK_RESPONSE = "CHUNK_RESPONSE"
    HASH_ERROR = "HASH_ERROR"

    PARTNER_HEARTBEAT = "PARTNER_HEARTBEAT"
    PARTNER_HEARTBEAT_ACK = "PARTNER_HEARTBEAT_ACK"


def md5_checksum(payload: Dict[str, Any]) -> str:
    data = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.md5(data).hexdigest()


def sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_envelope(
    msg_type: MessageType,
    src_user: str,
    src_ip: str,
    src_port: int,
    lamport: int,
    payload: Dict[str, Any],
) -> bytes:
    envelope = {
        "type": msg_type.value,
        "src_user": src_user,
        "src_ip": src_ip,
        "src_port": src_port,
        "lamport": lamport,
        "payload": payload,
        "checksum": md5_checksum(payload),
    }
    return json.dumps(envelope).encode("utf-8")


def parse_envelope(data: bytes) -> Dict[str, Any]:
    return json.loads(data.decode("utf-8"))
