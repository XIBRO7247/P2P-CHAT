from pathlib import Path
import json
from typing import Dict, Set, Tuple


class UserStateStore:
    """Responsible for persisting user-level state (friends, addresses)."""

    def __init__(self, username: str):
        self.username = username
        self.path = Path("state") / f"{username}_state.json"

    def load(self) -> tuple[Set[str], Dict[str, Tuple[str, int]]]:
        friends: Set[str] = set()
        addrs: Dict[str, Tuple[str, int]] = {}

        if not self.path.exists():
            return friends, addrs

        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return friends, addrs

        friends = set(data.get("friends", []))
        for uname, info in data.get("friend_addrs", {}).items():
            ip = info.get("ip")
            port = info.get("port")
            if ip and port:
                addrs[uname] = (ip, int(port))

        return friends, addrs

    def save(self, friends: Set[str], user_cache: Dict[str, Tuple[str, int]]) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            friend_addrs = {}
            for uname in friends:
                if uname in user_cache:
                    ip, port = user_cache[uname]
                    friend_addrs[uname] = {"ip": ip, "port": port}
            data = {
                "friends": sorted(friends),
                "friend_addrs": friend_addrs,
            }
            with self.path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            # Don’t crash the client over a failed save
            pass
