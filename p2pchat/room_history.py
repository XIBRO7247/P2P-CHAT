# p2pchat/room_history.py

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Dict, List, Tuple


class RoomHistoryManager:
    """
    Stores per-room message history for a given local user.

    Data model:
        {
          "room_id": [
              ["sender1", "text1"],
              ["sender2", "text2"],
              ...
          ],
          ...
        }

    This is *local* history used by the CLI for rendering /room chat views.
    It does not replace your distributed chunk/replication design.
    """

    def __init__(self, username: str, state_dir: str):
        self.username = username
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self.path = self.state_dir / f"{self.username}_rooms.json"
        self._lock = threading.Lock()
        self._history: Dict[str, List[Tuple[str, str]]] = {}
        self._load()

    # ---------- Persistence ----------

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            with self.path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            return

        # raw is: dict[str, list[list[str, str]]]
        for room, entries in raw.items():
            self._history[room] = [(s, t) for s, t in entries]

    def _save(self) -> None:
        try:
            serialisable = {
                room: [[s, t] for (s, t) in history]
                for room, history in self._history.items()
            }
            with self.path.open("w", encoding="utf-8") as f:
                json.dump(serialisable, f, ensure_ascii=False, indent=2)
        except Exception:
            # For robustness in coursework setting, ignore save errors.
            pass

    # ---------- API ----------

    def log_incoming(self, room_id: str, sender: str, text: str) -> None:
        with self._lock:
            self._history.setdefault(room_id, []).append((sender, text))
            self._save()

    def log_outgoing(self, room_id: str, sender: str, text: str) -> None:
        with self._lock:
            self._history.setdefault(room_id, []).append((sender, text))
            self._save()

    def get_history(self, room_id: str) -> list[tuple[str, str]]:
        with self._lock:
            return list(self._history.get(room_id, []))


    def get_all(self) -> dict[str, list[tuple[str, str]]]:
        """Return a shallow copy of all room histories."""
        with self._lock:
            return {
                room: list(history)
                for room, history in self._history.items()
            }

    def clear_room(self, room_id: str) -> None:
        """Drop local history for a single room and persist."""
        with self._lock:
            if room_id in self._history:
                del self._history[room_id]
                self._save()
