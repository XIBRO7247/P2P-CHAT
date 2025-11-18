# p2pchat/cli.py

import os
import threading
import time

from .middleware import Middleware


class CLI:
    """Simple command-line interface for interacting with middleware."""
    def __init__(self, username: str, config):
        self.middleware = Middleware(username, config)

    def _clear_screen(self) -> None:
        # Try to clear via shell, then push older output off-screen as a fallback
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
            # Runs in background: prints any new messages appended to history
            while not stop_event.is_set():
                try:
                    current = self.middleware.get_dm_history(user)
                except Exception:
                    time.sleep(0.2)
                    continue

                if len(current) > last_len:
                    # Print only the new messages
                    new_msgs = current[last_len:]
                    for sender, text in new_msgs:
                        label = "me" if sender == self.middleware.username else sender
                        # Start on a new line to avoid clobbering any partially typed input
                        print(f"\n[{label}] {text}", flush=True)
                    last_len = len(current)

                    # Re-show a prompt hint (input() still owns the real prompt)
                    print("dm> ", end="", flush=True)

                time.sleep(0.2)

        # Start background UI refresher
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
                    # Just ignore empty lines; background thread will keep printing new messages
                    continue

                if text in ("/home", "/exit"):
                    break

                # Normal DM
                self.middleware.send_direct_message(user, text)

        finally:
            stop_event.set()
            ui_thread.join(timeout=1.0)

    def run(self) -> None:
        self.middleware.start()
        print(f"[cli] Running as <{self.middleware.username}> on port {self.middleware.listen_port}")
        print("Commands:")
        print("  /join <room>              - join a room")
        print("  /msg <room> <text>        - send message to room")
        print("  /friend <user>            - send friend request")
        print("  /friend_accept <user>     - accept friend request")
        print("  /friend_reject <user>     - reject friend request")
        print("  /friends                  - list friends")
        print("  /friendreqs               - list incoming friend requests")
        print("  /dm <user> [text]         - open DM view (optionally send first message)")
        print("  /unfriend <user>          - remove a friend")
        print("  /exit or /quit            - exit client from home screen")

        try:
            while True:
                line = input("> ").strip()
                if not line:
                    continue

                # Home-level exit: both /quit and /exit
                if line.startswith("/quit") or line.startswith("/exit"):
                    break

                elif line.startswith("/join "):
                    _, room = line.split(maxsplit=1)
                    self.middleware.join_room(room)

                elif line.startswith("/msg "):
                    parts = line.split(" ", 2)
                    if len(parts) < 3:
                        print("[cli] Usage: /msg <room> <text>")
                        continue
                    _, room, text = parts
                    self.middleware.send_room_message(room, text)

                elif line.startswith("/friend "):
                    _, user = line.split(maxsplit=1)
                    self.middleware.send_friend_request(user)

                elif line.startswith("/friend_accept "):
                    _, user = line.split(maxsplit=1)
                    self.middleware.accept_friend(user)

                elif line.startswith("/friend_reject "):
                    _, user = line.split(maxsplit=1)
                    self.middleware.reject_friend(user)

                elif line.startswith("/unfriend "):
                    _, user = line.split(maxsplit=1)
                    self.middleware.unfriend(user)

                elif line.startswith("/dm "):
                    parts = line.split(" ", 2)
                    if len(parts) == 2:
                        # /dm <user> -> open DM view, no initial message
                        _, user = parts
                        self._dm_session(user)
                    elif len(parts) == 3:
                        # /dm <user> <text> -> send first message, then open DM view
                        _, user, text = parts
                        self._dm_session(user, first_msg=text)
                    else:
                        print("[cli] Usage: /dm <user> [text]")

                elif line == "/friends":
                    fm = self.middleware.friend_manager
                    friends = sorted(fm.friends)
                    if not friends:
                        print("[cli] You have no friends yet.")
                    else:
                        print("[cli] Friends:", ", ".join(friends))

                elif line == "/friendreqs":
                    fm = self.middleware.friend_manager
                    reqs = sorted(fm.incoming_friend_requests)
                    if not reqs:
                        print("[cli] No incoming friend requests.")
                    else:
                        print("[cli] Incoming friend requests:", ", ".join(reqs))

                else:
                    print("[cli] Unknown command.")

        except KeyboardInterrupt:
            pass
        finally:
            self.middleware.stop()
            print("[cli] Exiting.")
