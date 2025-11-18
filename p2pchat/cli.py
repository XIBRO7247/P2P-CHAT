from .middleware import Middleware


class CLI:
    """Simple command-line interface for interacting with middleware."""
    def __init__(self, username: str, config):
        self.middleware = Middleware(username, config)

    def run(self) -> None:
        self.middleware.start()
        print(f"[cli] Running as <{self.middleware.username}> on port {self.middleware.listen_port}")
        print("[cli] Type /help for a list of commands.")
        try:
            while True:
                line = input("> ").strip()
                if not line:
                    continue

                if line == "/quit":
                    break

                elif line == "/help":
                    print("Commands:")
                    print("  /join <room>                Join a room")
                    print("  /msg <room> <text>          Send message to room")
                    print("  /friend <user>              Send friend request")
                    print("  /friends                    List friends")
                    print("  /friend_requests            List incoming friend requests")
                    print("  /friend_accept <user>       Accept a friend request")
                    print("  /friend_reject <user>       Reject a friend request")
                    print("  /dm <user> <text>           Direct message a friend (or queue if pending)")
                    print("  /quit")

                elif line.startswith("/join "):
                    _, room = line.split(maxsplit=1)
                    self.middleware.join_room(room)

                elif line.startswith("/msg "):
                    try:
                        _, room, text = line.split(" ", 2)
                    except ValueError:
                        print("Usage: /msg <room> <text>")
                        continue
                    self.middleware.send_room_message(room, text)

                elif line.startswith("/friend "):
                    _, user = line.split(maxsplit=1)
                    self.middleware.send_friend_request(user)

                elif line == "/friends":
                    friends = sorted(self.middleware.friends)
                    print("Friends:", ", ".join(friends) if friends else "(none)")

                elif line == "/friend_requests":
                    reqs = sorted(self.middleware.incoming_friend_requests)
                    print("Incoming friend requests:", ", ".join(reqs) if reqs else "(none)")

                elif line.startswith("/friend_accept "):
                    _, user = line.split(maxsplit=1)
                    self.middleware.accept_friend(user)

                elif line.startswith("/friend_reject "):
                    _, user = line.split(maxsplit=1)
                    self.middleware.reject_friend(user)

                elif line.startswith("/dm "):
                    try:
                        _, user, text = line.split(" ", 2)
                    except ValueError:
                        print("Usage: /dm <user> <text>")
                        continue
                    self.middleware.send_direct_message(user, text)

                else:
                    print("[cli] Unknown command. Use /help to see available commands.")
        except KeyboardInterrupt:
            pass
        finally:
            self.middleware.stop()
            print("[cli] Exiting.")
