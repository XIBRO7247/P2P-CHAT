# p2pchat/chunk_manager.py

from typing import Dict, List, Any, Optional
from .utils import generate_chunk_id
from .storage import Storage


class ChunkManager:
    """
    Tracks active (unsealed) chunks per room and seals them
    after a size threshold, persisting to Storage.
    """

    def __init__(self, chunk_size_messages: int, storage: Storage):
        self.chunk_size = chunk_size_messages
        self.active_chunks: Dict[str, List[Dict[str, Any]]] = {}
        self.storage = storage

    def add_message(self, room_id: str, message: Dict[str, Any]) -> Optional[str]:
        """
        Add a message to the current active chunk for a room.
        Returns a chunk_id if this call caused the chunk to be sealed; otherwise None.
        """
        chunk = self.active_chunks.setdefault(room_id, [])
        chunk.append(message)

        if len(chunk) >= self.chunk_size:
            return self.seal_chunk(room_id)
        return None

    def seal_chunk(self, room_id: str) -> Optional[str]:
        """
        Seal the active chunk for room_id:
        - generates a chunk_id,
        - persists messages via Storage,
        - clears the active chunk.
        Returns chunk_id, or None if there is nothing to seal.
        """
        messages = self.active_chunks.get(room_id)
        if not messages:
            return None

        chunk_id = generate_chunk_id()
        self.storage.save_chunk(room_id, chunk_id, messages)
        self.active_chunks[room_id] = []
        print(f"[chunk_manager] Sealed chunk {chunk_id} for room {room_id} ({len(messages)} messages)")
        return chunk_id

    def get_active_messages(self, room_id: str) -> List[Dict[str, Any]]:
        return self.active_chunks.get(room_id, [])
