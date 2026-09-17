#!/usr/bin/env python3
"""
VMC UDP Relay - Intercepts VMC data from XR Animator (SystemAnimatorOnline)
and re-sends it on a different port.

If transmission stops, keeps re-sending the last known data (keepalive).

Usage:
    python vmc_relay.py --listen-port 39539 --target-port 39550

This tool:
1. Binds to the listen port (default 39539, XR Animator's send port)
2. Receives UDP packets from XR Animator
3. Forwards them to the target port
4. If no new data arrives for 'timeout' seconds, re-sends the last known data
"""

import socket
import sys
import time
import argparse
import threading
import struct
import os

# Default ports from SystemAnimatorOnline's MMD_SA.js
DEFAULT_LISTEN_PORT = 39539  # VMC send port (XR Animator -> target)
DEFAULT_TARGET_PORT = 39540  # VMC open/receive port


class VMCRelay:
    """
    VMC UDP Relay - intercepts and re-sends VMC data with keepalive.
    """

    def __init__(
        self,
        listen_port: int = DEFAULT_LISTEN_PORT,
        target_host: str = "localhost",
        target_port: int = DEFAULT_TARGET_PORT,
        keepalive_timeout: float = 1.0,
        keepalive_interval: float = 0.05,
    ):
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self.keepalive_timeout = keepalive_timeout
        self.keepalive_interval = keepalive_interval

        self.last_data: bytes = b""
        self.last_recv_time: float = 0.0
        self._running: bool = False

        self._recv_thread: threading.Thread = None
        self._keepalive_thread: threading.Thread = None
        self._recv_sock: socket.socket = None
        self._send_sock: socket.socket = None

    def _recv_handler(self) -> None:
        """
        Receive loop: captures UDP packets from XR Animator.
        """
        self._recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._recv_sock.bind(("0.0.0.0", self.listen_port))
        self._recv_sock.settimeout(0.05)

        print(f"[+] Listening on UDP {self.listen_port}")
        print(f"[+] Forwarding to {self.target_host}:{self.target_port}")
        print(f"[+] Keepalive: timeout={self.keepalive_timeout}s, interval={self.keepalive_interval}s")
        print(f"[+] Press Ctrl+C to stop")
        print()

        while self._running:
            try:
                data, addr = self._recv_sock.recvfrom(65535)
                self.last_data = data
                self.last_recv_time = time.time()
                self._send_to_target(data)
                # Decode OSC address for debug (first few bytes)
                try:
                    # Try to extract OSC address from raw bytes
                    if len(data) >= 4:
                        typetag = data[3]
                        if typetag == 0x00:  # string type tag
                            # Find null terminator
                            null_pos = data.find(b"\x00", 4)
                            if null_pos != -1:
                                addr_bytes = data[4:null_pos]
                                try:
                                    addr_str = addr_bytes.decode("utf-8", errors="ignore")
                                    if addr_str and not addr_str.startswith("\x00"):
                                        print(f"[R] {addr_str[:40]}")
                                except Exception:
                                    pass
                except Exception:
                    pass
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[-] Receive error: {e}", file=sys.stderr)
                break

        if self._recv_sock:
            self._recv_sock.close()
            self._recv_sock = None

    def _keepalive_handler(self) -> None:
        """
        Keepalive loop: re-sends last known data if transmission stops.
        """
        while self._running:
            try:
                time.sleep(self.keepalive_interval)
                elapsed = time.time() - self.last_recv_time
                if elapsed > self.keepalive_timeout:
                    if self.last_data:
                        self._send_to_target(self.last_data)
                        missed = int(elapsed - self.keepalive_timeout)
                        if missed > 0:
                            print(f"[!] Keepalive: re-sending last data ({missed}x, {elapsed:.1f}s since last recv)")
            except Exception as e:
                print(f"[-] Keepalive error: {e}", file=sys.stderr)

    def _send_to_target(self, data: bytes) -> None:
        """
        Send data to the target port.
        Uses a dedicated send socket for efficiency.
        """
        if self._send_sock is None:
            self._send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self._send_sock.sendto(data, (self.target_host, self.target_port))
        except Exception as e:
            print(f"[-] Send error: {e}", file=sys.stderr)

    def start(self) -> None:
        """Start the relay."""
        self._running = True

        self._recv_thread = threading.Thread(target=self._recv_handler, daemon=True)
        self._keepalive_thread = threading.Thread(target=self._keepalive_handler, daemon=True)

        self._recv_thread.start()
        self._keepalive_thread.start()

        # Wait for threads to finish
        self._recv_thread.join()
        self._keepalive_thread.join()

        self._running = False
        print()
        print("[*] Relay stopped")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "VMC UDP Relay - Intercepts VMC data from XR Animator (SystemAnimatorOnline)"
            " and re-sends it on a different port. Keeps sending last known data if transmission stops."
        )
    )
    parser.add_argument(
        "--listen-port",
        type=int,
        default=DEFAULT_LISTEN_PORT,
        help=f"Port to listen on (XR Animator's send port, default: {DEFAULT_LISTEN_PORT})",
    )
    parser.add_argument(
        "--target-host",
        default="localhost",
        help=f"Target host (default: localhost)",
    )
    parser.add_argument(
        "--target-port",
        type=int,
        default=DEFAULT_TARGET_PORT,
        help=f"Target port (default: {DEFAULT_TARGET_PORT})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=1.0,
        help="Keepalive timeout in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.05,
        help="Keepalive check interval in seconds (default: 0.05)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    relay = VMCRelay(
        listen_port=args.listen_port,
        target_host=args.target_host,
        target_port=args.target_port,
        keepalive_timeout=args.timeout,
        keepalive_interval=args.interval,
    )
    relay.start()


if __name__ == "__main__":
    main()
