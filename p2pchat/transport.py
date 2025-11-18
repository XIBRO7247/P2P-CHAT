import socket
import threading
import errno
from queue import Queue
from typing import Tuple, Optional

from .protocol import parse_envelope


class UDPTransport:
    """UDP transport with auto-port selection."""
    def __init__(self, listen_host: str, start_port: int, end_port: int):
        self.listen_host = listen_host
        self._requested_start_port = start_port
        self._requested_end_port = end_port

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listen_port = self._bind_to_first_free_port()

        self._recv_thread: Optional[threading.Thread] = None
        self._running = threading.Event()
        self.incoming_queue: Queue = Queue()

    def _bind_to_first_free_port(self) -> int:
        port = self._requested_start_port
        while True:
            try:
                self.sock.bind((self.listen_host, port))
                print(f"[transport] Bound to {self.listen_host}:{port}")
                return port
            except OSError as e:
                if e.errno == errno.EADDRINUSE and port < self._requested_end_port:
                    port += 1
                    continue
                raise RuntimeError(f"No free ports in {self._requested_start_port}-{self._requested_end_port}")

    def start(self) -> None:
        self._running.set()
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._recv_thread.start()

    def stop(self) -> None:
        self._running.clear()
        self.sock.close()

    def _recv_loop(self) -> None:
        while self._running.is_set():
            try:
                data, addr = self.sock.recvfrom(65535)
                envelope = parse_envelope(data)
                self.incoming_queue.put((envelope, addr))
            except Exception:
                continue

    def send_raw(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            self.sock.sendto(data, addr)
        except Exception:
            pass
