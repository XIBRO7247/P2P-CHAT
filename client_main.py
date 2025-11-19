import sys
import json
import traceback
import os
from pathlib import Path

from p2pchat.cli import CLI
from p2pchat.config_loader import Config

# --------- force working directory to this file's folder ---------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)
# -----------------------------------------------------------------

BASE_CONFIG_PATH = Path("config/client_config.json")
USER_CONFIG_DIR = Path("config")

def ensure_user_config(username: str) -> Config:
    """
    Ensure a per-user config file exists:
    - If config/<username>_config.json exists: load it.
    - Otherwise: create it by copying config/client_config.json,
      optionally customising some fields, then load it.
    """
    USER_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    user_cfg_path = USER_CONFIG_DIR / f"{username}_config.json"

    # If user config already exists, just load it.
    if user_cfg_path.exists():
        return Config(str(user_cfg_path))

    # Otherwise, base config must exist.
    if not BASE_CONFIG_PATH.exists():
        print(f"[error] Base config not found at {BASE_CONFIG_PATH}")
        sys.exit(1)

    with BASE_CONFIG_PATH.open("r", encoding="utf-8") as f:
        base_cfg = json.load(f)

    # Optional: customise per-user settings here.
    # For example, per-user chunk directory:
    base_cfg.setdefault("chunk_dir", f"chunks_{username}")

    # You could also offset listen_port_start if you want,
    # but auto-port selection will handle collisions anyway.

    with user_cfg_path.open("w", encoding="utf-8") as f:
        json.dump(base_cfg, f, indent=2)

    print(f"[client_main] Created new config for {username} at {user_cfg_path}")
    return Config(str(user_cfg_path))


def main():
    try:
        username = input("Enter username: ").strip()
    except KeyboardInterrupt:
        print("\n[client_main] Aborted.")
        sys.exit(0)

    if not username:
        print("[error] Username cannot be empty.")
        sys.exit(1)

    config = ensure_user_config(username)

    cli = CLI(username, config)
    cli.run()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("\n[client_main] Unhandled exception:\n")
        traceback.print_exc()
    finally:
        input("\nPress Enter to close...")
