import json
from pathlib import Path
from typing import Any, Dict


class Config:
    """Loads configuration from a JSON file and exposes values."""
    def __init__(self, path: str):
        self._data = self._load(path)

    @staticmethod
    def _load(path: str) -> Dict[str, Any]:
        cfg_path = Path(path)
        if not cfg_path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with cfg_path.open() as f:
            return json.load(f)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def get(self, key: str, default=None) -> Any:
        return self._data.get(key, default)
