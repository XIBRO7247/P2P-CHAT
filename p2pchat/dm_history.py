# p2pchat/dm_history.py

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


class DMHistoryManager:
    """
    Stores DM history per peer.
    - Persists to a JSON file per local username.
    - Middleware just logs messages here; CLI reads via get_history().
    """

    def __init__(self, username: str, state_dir: str | None = None) -> None:
        self.username = username
        base = Path(state_dir or "state")
        base.mkdir(parents=True, exist_ok=True)
        self._path = base / f"{username}_dm.json"

        # peer -> list of (sender, text)
        self._history: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        self._load()

    def _load(self) -> None:
        """Load history from disk if it exists."""
        if not self._path.exists():
            return
        try:
            with self._path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            for peer, msgs in raw.items():
                # each msg is [sender, text]
                self._history[peer] = [(m[0], m[1]) for m in msgs]
        except Exception:
            # If file is corrupt, start with a clean history
            self._history = defaultdict(list)

    def _save(self) -> None:
        """Persist history to disk."""
        data = {
            peer: [[sender, text] for (sender, text) in msgs]
            for peer, msgs in self._history.items()
        }
        with self._path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def log_outgoing(self, peer: str, sender: str, text: str) -> None:
        """Record an outgoing DM to peer."""
        self._history[peer].append((sender, text))
        self._save()

    def log_incoming(self, peer: str, sender: str, text: str) -> None:
        """Record an incoming DM from sender (which is usually the peer)."""
        self._history[peer].append((sender, text))
        self._save()

    def get_history(self, peer: str) -> list[tuple[str, str]]:
        """Return the full DM history with a given peer."""
        return list(self._history.get(peer, []))
