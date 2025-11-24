# p2pchat/chunk_manager.py

import json
import time
import hashlib
from typing import Dict, List, Any, Optional, Set


class ChunkManager:
    """
    Handles chunk building and sealing.

    Each chunk = list of messages.
    When a chunk reaches max_size, it is sealed and saved via Storage.
    """

    def __init__(self, max_messages: int, storage):
        self.max_messages = max_messages
        self.storage = storage

        # room_id -> current unsealed list of messages
        self.buffers: Dict[str, List[Dict[str, Any]]] = {}

        # room_id -> list of sealed chunk_ids (local)
        self.local_index: Dict[str, List[str]] = {}

    # ----------------------------------------------
    # Core: add message
    # ----------------------------------------------

    def add_message(self, room_id: str, msg: Dict[str, Any]) -> Optional[str]:
        """
        Add a message to the buffer.
        Return chunk_id if sealed, else None.
        """
        buf = self.buffers.setdefault(room_id, [])
        buf.append(msg)

        if len(buf) >= self.max_messages:
            return self._seal_chunk(room_id)

        return None

    # ----------------------------------------------
    # Sealing chunks
    # ----------------------------------------------

    def _seal_chunk(self, room_id: str) -> str:
        """
        Seal current buffer → chunk_id, save via storage, clear buffer.
        """
        buf = self.buffers.get(room_id)
        if not buf:
            return ""

        # Hash = stable, unique chunk id
        data_bytes = json.dumps(buf, sort_keys=True).encode("utf-8")
        chunk_id = hashlib.sha256(data_bytes).hexdigest()[:16]

        self.storage.save_chunk(room_id, chunk_id, buf)

        # Track locally
        self.local_index.setdefault(room_id, []).append(chunk_id)

        # Clear buffer
        self.buffers[room_id] = []

        return chunk_id

    # ----------------------------------------------
    # Read local state
    # ----------------------------------------------

    def list_chunks(self, room_id: str) -> List[str]:
        """
        Return list of local chunk_ids.
        Prefer Storage.list_chunks() if available.
        """
        try:
            return self.storage.list_chunks(room_id)
        except Exception:
            return self.local_index.get(room_id, [])

    def chunk_exists(self, room_id: str, chunk_id: str) -> bool:
        """
        Check if a chunk_id exists locally.
        """
        try:
            return chunk_id in self.storage.list_chunks(room_id)
        except Exception:
            return chunk_id in self.local_index.get(room_id, [])

    def get_all_chunks(self) -> Set[str]:
        """
        Union of all chunks across all rooms.
        """
        all_ids: Set[str] = set()
        for room_id in self.local_index.keys():
            for cid in self.list_chunks(room_id):
                all_ids.add(cid)
        return all_ids

    def get_room_last_chunk(self, room_id: str) -> Optional[str]:
        """
        Return last sealed chunk for a room.
        """
        chunks = self.list_chunks(room_id)
        return chunks[-1] if chunks else None

    # ----------------------------------------------
    # Load full history (for rebuild)
    # ----------------------------------------------

    def load_chunk(self, room_id: str, chunk_id: str) -> List[Dict[str, Any]]:
        return self.storage.load_chunk(room_id, chunk_id)

    def load_all_chunks(self, room_id: str) -> List[Dict[str, Any]]:
        """
        Return a *combined* ordered list of all messages for a room.
        Used for full rebuilds.
        """
        msgs: List[Dict[str, Any]] = []
        for cid in self.list_chunks(room_id):
            try:
                msgs.extend(self.storage.load_chunk(room_id, cid))
            except FileNotFoundError:
                pass
        return msgs

    # ----------------------------------------------
    # Cleanup utilities
    # ----------------------------------------------

    def remove_chunk(self, room_id: str, chunk_id: str) -> None:
        """
        Remove a chunk from local tracking (but keep file unless manually deleted).
        """
        if room_id in self.local_index:
            if chunk_id in self.local_index[room_id]:
                self.local_index[room_id].remove(chunk_id)

    def rebuild_chunk_index(self, room_id: str) -> None:
        """
        Rebuild local chunk_index from filesystem.
        Useful after crashes or unexpected state.
        """
        try:
            self.local_index[room_id] = self.storage.list_chunks(room_id)
        except Exception:
            self.local_index[room_id] = []

    # ----------------------------------------------
    # Missing chunk calculation (for sync)
    # ----------------------------------------------

    def get_missing_chunks(self, room_id: str, remote_index: Set[str]) -> Set[str]:
        """
        Return remote_index minus local_index.
        """
        local = set(self.list_chunks(room_id))
        return remote_index - local
