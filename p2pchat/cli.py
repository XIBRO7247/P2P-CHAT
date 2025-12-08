# p2pchat/cli.py

import os
import json
import threading
from queue import Empty
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
            # history expected as list[(sender, text)]
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

        # Initial render from persisted history
        self._clear_screen()
        history = self.middleware.get_dm_history(user)
        self._print_dm_history(user, history)

        # Track last printed history length
        last_len = len(history)

        stop_event = threading.Event()
        # Live listener (optional hint; history remains source of truth)
        q = self.middleware.register_dm_listener(user)

        def ui_loop():
            nonlocal last_len
            while not stop_event.is_set():
                # Drain listener queue (we don't print from it directly,
                # we just use it as a hint that something changed)
                try:
                    while True:
                        _ = q.get_nowait()
                        # ignore contents; history will include it
                except Empty:
                    pass

                # Poll history and print any new messages
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
            self.middleware.unregister_dm_listener(user, q)

    # ---------- ROOM mode (multi-threaded view) ----------

    def _room_session(self, room_id: str, first_msg: str | None = None) -> None:
        """
        Interactive room chat mode.

        - Ensures we are joined to the room.
        - Optionally sends an initial message.
        - Background thread tails room history and prints new messages.
        - Main thread reads user input and sends room messages.
        - /home or /exit returns to main menu.
        """
        # Make sure we are in the room
        self.middleware.join_room(room_id)

        # Optional first message, e.g. /room msg <room> hello world
        if first_msg:
            self.middleware.send_room_message(room_id, first_msg)

        # Initial render
        self._clear_screen()
        history = self.middleware.get_room_history(room_id)
        print(f"=== Room: {room_id} ===")
        if not history:
            print("(no messages yet)")
        else:
            # history expected as list[(sender, text)]
            for sender, text in history:
                label = "me" if sender == self.middleware.username else sender
                print(f"[{label}] {text}")

        print("\nType a message and press Enter.")
        print("Commands: /home or /exit to return to main menu.")
        print("(New messages will appear below as they arrive.)")

        last_len = len(history)
        stop_event = threading.Event()
        q = self.middleware.register_room_listener(room_id)

        def ui_loop():
            nonlocal last_len
            while not stop_event.is_set():
                # Drain listener queue (hint only)
                try:
                    while True:
                        _ = q.get_nowait()
                except Empty:
                    pass

                # Poll history
                try:
                    current = self.middleware.get_room_history(room_id)
                except Exception:
                    time.sleep(0.2)
                    continue

                if len(current) > last_len:
                    new_msgs = current[last_len:]
                    for sender, text in new_msgs:
                        label = "me" if sender == self.middleware.username else sender
                        print(f"\n[{label}] {text}", flush=True)
                    last_len = len(current)
                    print("room> ", end="", flush=True)

                time.sleep(0.2)

        ui_thread = threading.Thread(target=ui_loop, daemon=True)
        ui_thread.start()

        try:
            while True:
                try:
                    line = input("room> ")
                except KeyboardInterrupt:
                    print()
                    break

                text = line.strip()

                if text == "":
                    continue

                if text in ("/home", "/exit"):
                    break

                # Normal room message
                self.middleware.send_room_message(room_id, text)

        finally:
            stop_event.set()
            ui_thread.join(timeout=1.0)
            self.middleware.unregister_room_listener(room_id, q)

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

        if not node or (node.get("debug_only") and not self.middleware.debug):
            print(f"[cli] Unknown command: /{root_name}")
            return False

        idx = 1
        # Walk subcommands if present
        while "subcommands" in node and idx < len(tokens):
            sub_name = tokens[idx]
            sub_node = node["subcommands"].get(sub_name)
            if not sub_node:
                break

            # Respect debug_only on subcommands as well
            if sub_node.get("debug_only") and not self.middleware.debug:
                print(f"[cli] Unknown or disabled subcommand: /{root_name} {sub_name}")
                return False

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

    def _cmd_join(self, args: list[str]) -> bool:
        room = args[0]
        self.middleware.join_room(room)
        return False

    def _cmd_msg(self, args: list[str]) -> bool:
        if len(args) < 2:
            print("[cli] Usage: /msg <room> <text>")
            return False
        room = args[0]
        text = " ".join(args[1:])
        self.middleware.send_room_message(room, text)
        return False

    def _cmd_room_msg(self, args: list[str]) -> bool:
        room = args[0]
        first_msg = " ".join(args[1:]) if len(args) > 1 else None
        self._room_session(room, first_msg=first_msg)
        return False

    def _cmd_room_join(self, args: list[str]) -> bool:
        room = args[0]
        self.middleware.join_room(room)
        print(f"[cli] Joined room {room}. Use /room msg {room} to open a chat view.")
        return False

    def _cmd_room_list(self, args: list[str]) -> bool:
        """
        List rooms via middleware.list_rooms().

        Expected middleware return:
          - either a dict {"joined": [...], "available": [...]}
          - or a tuple (joined, available)
        """
        try:
            info = self.middleware.list_rooms()
        except AttributeError:
            print("[cli] Room listing is not supported by this middleware version.")
            return False

        if isinstance(info, dict):
            joined = sorted(info.get("joined", []))
            available = sorted(info.get("available", []))
        else:
            # Assume tuple-like
            joined, available = info
            joined = sorted(joined or [])
            available = sorted(available or [])

        print("[cli] Rooms you have joined:")
        if not joined:
            print("  (none)")
        else:
            for r in joined:
                print(f"  - {r}")

        print("[cli] All known rooms:")
        if not available:
            print("  (none)")
        else:
            for r in available:
                print(f"  - {r}")

        return False

    # alias so commands.json can point to either handler name
    def _cmd_rooms(self, args: list[str]) -> bool:
        return self._cmd_room_list(args)

    def _cmd_room_resync(self, args: list[str]) -> bool:
        """
        Clear and re-pull a room's message history from peers.
        Usage: /room resync <room_id>
        """
        if not args:
            print("[cli] Usage: /room resync <room>")
            return False
        room = args[0]
        self.middleware.resync_room_history(room)
        print(f"[cli] Requested resync for room {room}.")
        return False


    def _cmd_friend_add(self, args: list[str]) -> bool:
        user = args[0]
        self.middleware.send_friend_request(user)
        return False

    def _cmd_friend_accept(self, args: list[str]) -> bool:
        user = args[0]
        self.middleware.accept_friend(user)
        return False

    def _cmd_friend_reject(self, args: list[str]) -> bool:
        user = args[0]
        self.middleware.reject_friend(user)
        return False

    def _cmd_friend_remove(self, args: list[str]) -> bool:
        user = args[0]
        self.middleware.unfriend(user)
        return False

    def _cmd_friend_list(self, args: list[str]) -> bool:
        fm = self.middleware.friend_manager
        friends = sorted(fm.friends)
        if not friends:
            print("[cli] You have no friends yet.")
        else:
            print("[cli] Friends:", ", ".join(friends))
        return False

    def _cmd_friend_requests(self, args: list[str]) -> bool:
        fm = self.middleware.friend_manager
        reqs = sorted(fm.incoming_friend_requests)
        if not reqs:
            print("[cli] No incoming friend requests.")
        else:
            print("[cli] Incoming friend requests:", ", ".join(reqs))
        return False

    def _cmd_dm(self, args: list[str]) -> bool:
        user = args[0]
        first_msg = " ".join(args[1:]) if len(args) > 1 else None
        self._dm_session(user, first_msg=first_msg)
        return False

    def _cmd_help(self, args: list[str]) -> bool:
        cmds = self.command_tree.get("commands", {})
        print("Available commands:")
        for name, node in cmds.items():
            # Skip debug-only commands if debug mode is off
            if node.get("debug_only") and not self.middleware.debug:
                continue

            if "subcommands" not in node:
                print(" ", node.get("help", f"/{name} ..."))
            else:
                print(" ", node.get("help", f"/{name} ..."))
                for sub_name, sub_node in node["subcommands"].items():
                    if sub_node.get("debug_only") and not self.middleware.debug:
                        continue
                    print("   ", sub_node.get("help", f"/{name} {sub_name} ..."))
        return False

    def _cmd_robot(self, args: list[str]) -> bool:
        """
        Open a DM session with the local robot user.
        """
        robot_name = getattr(self.middleware, "robot_username", "robot")
        self._dm_session(robot_name)
        return False


    def _cmd_test_start(self, args: list[str]) -> bool:
        """
        Enable fault-injection test mode (debug only).
        """
        # Even if the command gets through, middleware will sanity-check debug.
        self.middleware.enable_test_mode()
        return False

    def _cmd_test_stop(self, args: list[str]) -> bool:
        """
        Disable fault-injection test mode.
        """
        self.middleware.disable_test_mode()
        return False

    def _cmd_export_chats(self, args: list[str]) -> bool:
        """
        Export DM + room histories to plaintext log files.
        Usage: /export chats [out_dir]
        """
        out_dir = args[0] if args else "exports"
        self.middleware.export_chats_plaintext(out_dir)
        return False

    def _cmd_file_send(self, args):
        user = args[0]
        path = " ".join(args[1:])
        self.middleware.file_send(user, path)

    def _cmd_file_list(self, args):
        self.middleware.file_list()

    def _cmd_file_get(self, args):
        self.middleware.file_get(args[0])

    def _cmd_file_forward(self, args):
        self.middleware.file_send(args[0], args[1])  # forward = send with allowed_users

    def _cmd_file_purge(self, args):
        self.middleware.file_purge(args[0])

    def _cmd_exit(self, args: list[str]) -> bool:
        return True

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
