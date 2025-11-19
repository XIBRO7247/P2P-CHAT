# p2pchat/cli.py

import os
import json
import threading
import time
from pathlib import Path

from .middleware import Middleware


class CLI:
    """Simple command-line interface for interacting with middleware."""
    def __init__(self, username: str, config):
        self.middleware = Middleware(username, config)

        # Load command tree from JSON config
        cmd_cfg_path = config.get("command_config", "config/commands.json")
        self.command_tree = self._load_command_config(cmd_cfg_path)

    # ---------- Config loading ----------

    @staticmethod
    def _load_command_config(path: str) -> dict:
        p = Path(path)
        if not p.exists():
            print(f"[cli] WARNING: command config {path} not found. Using empty tree.")
            return {"commands": {}}
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[cli] ERROR: failed to load command config: {e}")
            return {"commands": {}}
        return data

    # ---------- Simple UI helpers ----------

    def _clear_screen(self) -> None:
        if os.name == "nt":
            os.system("cls")
        else:
            os.system("clear")
        print("\n" * 2)

    def _print_dm_history(self, user: str, history) -> None:
        print(f"=== Direct messages with {user} ===")
        if not history:
            print("(no messages yet)")
        else:
            for sender, text in history:
                label = "me" if sender == self.middleware.username else sender
                print(f"[{label}] {text}")
        print("\nType a message and press Enter.")
        print("Commands: /home or /exit to return to main menu.")
        print("(New messages will appear below as they arrive.)")

    # ---------- DM mode (multi-threaded view) ----------

    def _dm_session(self, user: str, first_msg: str | None = None) -> None:
        """
        Interactive DM mode with a single user.

        - Background thread: tails DM history and prints new messages as they arrive.
        - Main thread: reads user input and sends DMs.
        - /home or /exit: return to main menu.
        """
        if first_msg:
            self.middleware.send_direct_message(user, first_msg)

        # Initial render
        self._clear_screen()
        history = self.middleware.get_dm_history(user)
        self._print_dm_history(user, history)

        # Track last printed history length
        last_len = len(history)

        stop_event = threading.Event()

        def ui_loop():
            nonlocal last_len
            while not stop_event.is_set():
                try:
                    current = self.middleware.get_dm_history(user)
                except Exception:
                    time.sleep(0.2)
                    continue

                if len(current) > last_len:
                    new_msgs = current[last_len:]
                    for sender, text in new_msgs:
                        label = "me" if sender == self.middleware.username else sender
                        print(f"\n[{label}] {text}", flush=True)
                    last_len = len(current)
                    print("dm> ", end="", flush=True)

                time.sleep(0.2)

        ui_thread = threading.Thread(target=ui_loop, daemon=True)
        ui_thread.start()

        try:
            while True:
                try:
                    line = input("dm> ")
                except KeyboardInterrupt:
                    print()
                    break

                text = line.strip()

                if text == "":
                    continue

                if text in ("/home", "/exit"):
                    break

                self.middleware.send_direct_message(user, text)

        finally:
            stop_event.set()
            ui_thread.join(timeout=1.0)

    # ---------- Command dispatcher (JSON-driven) ----------

    def _dispatch_command(self, line: str) -> bool:
        """
        Parse a line starting with '/' and dispatch it via the command tree.

        Returns True if the client should exit, False otherwise.
        """
        if not line.startswith("/"):
            print("[cli] Commands must start with '/'.")
            return False

        tokens = line[1:].split()
        if not tokens:
            return False

        root_name = tokens[0]
        cmds = self.command_tree.get("commands", {})
        node = cmds.get(root_name)

        if not node:
            print(f"[cli] Unknown command: /{root_name}")
            return False

        idx = 1
        # Walk subcommands if present
        while "subcommands" in node and idx < len(tokens):
            sub_name = tokens[idx]
            sub_node = node["subcommands"].get(sub_name)
            if not sub_node:
                break
            node = sub_node
            idx += 1

        handler_ref = node.get("handler")
        min_args = int(node.get("min_args", 0))
        remaining_args = tokens[idx:]

        # No handler => probably a parent node like /friend with no subcommand
        if not handler_ref:
            help_text = node.get("help", f"/{root_name} ...")
            print(f"[cli] Incomplete command. Usage: {help_text}")
            return False

        # Interpret "self._cmd_exit" style handler references
        if isinstance(handler_ref, str) and handler_ref.startswith("self."):
            method_name = handler_ref.split(".", 1)[1]
            handler = getattr(self, method_name, None)
        else:
            # Fallback: allow bare method names like "_cmd_exit"
            handler = getattr(self, str(handler_ref), None)

        if handler is None:
            print(f"[cli] Internal error: no handler for {handler_ref}")
            return False

        if len(remaining_args) < min_args:
            help_text = node.get("help", f"/{root_name} ...")
            print(f"[cli] Not enough arguments. Usage: {help_text}")
            return False

        # Handler returns True if it wants to exit the client
        return bool(handler(remaining_args))

    # ---------- Individual command handlers ----------

    # /join <room>
    def _cmd_join(self, args: list[str]) -> bool:
        room = args[0]
        self.middleware.join_room(room)
        return False

    # /msg <room> <text...>
    def _cmd_msg(self, args: list[str]) -> bool:
        if len(args) < 2:
            print("[cli] Usage: /msg <room> <text>")
            return False
        room = args[0]
        text = " ".join(args[1:])
        self.middleware.send_room_message(room, text)
        return False

    # /friend add/request <user>
    def _cmd_friend_add(self, args: list[str]) -> bool:
        user = args[0]
        self.middleware.send_friend_request(user)
        return False

    # /friend accept <user>
    def _cmd_friend_accept(self, args: list[str]) -> bool:
        user = args[0]
        self.middleware.accept_friend(user)
        return False

    # /friend reject <user>
    def _cmd_friend_reject(self, args: list[str]) -> bool:
        user = args[0]
        self.middleware.reject_friend(user)
        return False

    # /friend remove <user>
    def _cmd_friend_remove(self, args: list[str]) -> bool:
        user = args[0]
        self.middleware.unfriend(user)
        return False

    # /friend list
    def _cmd_friend_list(self, args: list[str]) -> bool:
        fm = self.middleware.friend_manager
        friends = sorted(fm.friends)
        if not friends:
            print("[cli] You have no friends yet.")
        else:
            print("[cli] Friends:", ", ".join(friends))
        return False

    # /friend requests
    def _cmd_friend_requests(self, args: list[str]) -> bool:
        fm = self.middleware.friend_manager
        reqs = sorted(fm.incoming_friend_requests)
        if not reqs:
            print("[cli] No incoming friend requests.")
        else:
            print("[cli] Incoming friend requests:", ", ".join(reqs))
        return False

    # /dm <user> [text...]
    def _cmd_dm(self, args: list[str]) -> bool:
        user = args[0]
        first_msg = " ".join(args[1:]) if len(args) > 1 else None
        self._dm_session(user, first_msg=first_msg)
        return False

    # /help
    def _cmd_help(self, args: list[str]) -> bool:
        cmds = self.command_tree.get("commands", {})
        print("Available commands:")
        for name, node in cmds.items():
            # Top-level help
            if "subcommands" not in node:
                print(" ", node.get("help", f"/{name} ..."))
            else:
                print(" ", node.get("help", f"/{name} ..."))
                for sub_name, sub_node in node["subcommands"].items():
                    print("   ", sub_node.get("help", f"/{name} {sub_name} ..."))
        return False

    # /exit or /quit
    def _cmd_exit(self, args: list[str]) -> bool:
        # Returning True tells run() to break the main loop.
        return True

    # ---------- Main loop ----------

    def run(self) -> None:
        self.middleware.start()
        print(f"[cli] Running as <{self.middleware.username}> on port {self.middleware.listen_port}")
        print("Type /help to see available commands (loaded from config/commands.json).")

        try:
            while True:
                line = input("> ").strip()
                if not line:
                    continue

                if line.startswith("/"):
                    should_exit = self._dispatch_command(line)
                    if should_exit:
                        break
                else:
                    print("[cli] Unknown input. Commands must start with '/'.")
        except KeyboardInterrupt:
            pass
        finally:
            self.middleware.stop()
            print("[cli] Exiting.")
