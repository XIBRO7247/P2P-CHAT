# p2pchat/storage.py

import json
from pathlib import Path
from typing import List, Dict, Any


class Storage:
    """
    Persists sealed chunks as JSON files:
    chunks/<room_id>/<chunk_id>.json
    """

    def __init__(self, base_dir: str = "chunks"):
        self.base_path = Path(base_dir)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _chunk_file(self, room_id: str, chunk_id: str) -> Path:
        room_dir = self.base_path / room_id
        room_dir.mkdir(parents=True, exist_ok=True)
        return room_dir / f"{chunk_id}.json"

    def save_chunk(self, room_id: str, chunk_id: str, messages: List[Dict[str, Any]]) -> None:
        path = self._chunk_file(room_id, chunk_id)
        with path.open("w", encoding="utf-8") as f:
            json.dump(messages, f, indent=2)
        print(f"[storage] Saved chunk {chunk_id} for room {room_id} at {path}")

    def load_chunk(self, room_id: str, chunk_id: str) -> List[Dict[str, Any]]:
        path = self._chunk_file(room_id, chunk_id)
        if not path.exists():
            raise FileNotFoundError(f"No such chunk {chunk_id} in room {room_id}")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def list_chunks(self, room_id: str) -> List[str]:
        """
        Return a list of chunk_ids we currently have for this room.

        It looks for files:
            chunks/<room_id>/<chunk_id>.json
        and strips the `.json` suffix.
        """
        room_dir = self.base_path / room_id
        if not room_dir.exists() or not room_dir.is_dir():
            return []

        chunk_ids: List[str] = []
        for path in room_dir.glob("*.json"):
            # e.g. "abcd1234.json" -> "abcd1234"
            chunk_ids.append(path.stem)

        # Sort for stable ordering (e.g. if chunk_ids are monotonic)
        chunk_ids.sort()
        return chunk_ids
