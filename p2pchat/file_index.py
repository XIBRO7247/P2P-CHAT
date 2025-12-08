from pathlib import Path
import json
from typing import Dict, Any, Optional

class FileIndex:
    def __init__(self, username: str, base_dir: str = "files"):
        self.base_dir = Path(base_dir) / username
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.index_path = self.base_dir / "files_index.json"
        self.index: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self):
        if self.index_path.exists():
            try:
                self.index = json.loads(self.index_path.read_text())
            except:
                self.index = {}

    def _save(self):
        try:
            self.index_path.write_text(json.dumps(self.index, indent=2))
        except:
            pass

    def add(self, file_id: str, name: str, size: int, sha256: str, path: Path, seeder=True):
        self.index[file_id] = {
            "name": name,
            "size": size,
            "sha256": sha256,
            "path": str(path),
            "seeder": seeder
        }
        self._save()

    def mark_seeder(self, file_id: str, seeder: bool):
        if file_id in self.index:
            self.index[file_id]["seeder"] = seeder
            self._save()

    def get(self, file_id: str):
        return self.index.get(file_id)

    def remove(self, file_id: str):
        meta = self.index.pop(file_id, None)
        self._save()
        return meta
