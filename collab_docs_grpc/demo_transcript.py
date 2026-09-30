#!/usr/bin/env python3
"""
Two-client demonstration of the collaborative document editor (Section 3 Q1).

Starts a real DocumentServer and two real `client.py` CLI processes, then
replays a scripted two-client demo:

    1. Client 1 creates the document
    2. both clients open it
    3. Client 2 subscribes to updates
    4. Client 1 edits            -> Client 2 receives the update unprompted
    5. both clients edit concurrently (same position) -> server serializes,
       both subscribers receive every version
    6. both clients read the final state

Everything both clients print is captured with timestamps and written to
`demo_transcript.txt` (and stdout), so the file is a genuine recorded
session, not a hand-written transcript.

Usage:  python3 demo_transcript.py
"""

import os
import signal
import socket
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "src")
SERVER = os.path.join(SRC, "server.py")
CLIENT = os.path.join(SRC, "client.py")
PORT = 50761
ADDRESS = "127.0.0.1:%d" % PORT

events = []          # (monotonic_time, tag, line)
start = None


def record(tag, line):
    events.append((time.monotonic(), tag, line))


def reader(tag, stream):
    """Background reader: capture everything one client prints."""
    for raw in stream:
        line = raw.decode(errors="replace").rstrip("\n") if isinstance(raw, bytes) \
            else raw.rstrip("\n")
        record(tag, line)


def wait_for_server(timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.3)
            if sock.connect_ex(("127.0.0.1", PORT)) == 0:
                return
        time.sleep(0.15)
    raise RuntimeError("server did not start on %s" % ADDRESS)


def start_client(name):
    process = subprocess.Popen(
        [sys.executable, CLIENT, ADDRESS],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, env={**os.environ, "PYTHONUNBUFFERED": "1"},
        cwd=SRC)
    threading.Thread(target=reader, name=name, daemon=True,
                     args=(name, process.stdout)).start()
    return process


def send(clients, name, command, pause=0.5):
    """Send one CLI command and echo it into the transcript."""
    stdin = clients[name].stdin
    stdin.write((command + "\n").encode())
    stdin.flush()
    record(name + " »", command)
    time.sleep(pause)


def banner(text):
    record("──", " " + text)


def main():
    global start
    start = time.monotonic()

    server = subprocess.Popen(
        [sys.executable, SERVER, "0.0.0.0:%d" % PORT],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    try:
        wait_for_server()
        clients = {"C1": start_client("C1"), "C2": start_client("C2")}
        time.sleep(0.8)

        # ---- Step 1: Client 1 creates the document ----------------------
        banner("Step 1: Client 1 creates the document")
        send(clients, "C1", 'create report.txt "Hello World"')

        # ---- Step 2: both clients open it --------------------------------
        banner("Step 2: both clients open the document")
        send(clients, "C1", "open report.txt")
        send(clients, "C2", "open report.txt")

        # ---- Step 3: Client 2 subscribes ----------------------------------
        banner("Step 3: Client 2 subscribes to updates")
        send(clients, "C2", "subscribe report.txt", pause=0.8)

        # ---- Step 4: Client 1 edits; C2 receives the update ---------------
        banner("Step 4: Client 1 edits — Client 2 receives the update "
               "without asking")
        send(clients, "C1", 'edit report.txt 6 "Distributed "', pause=1.0)

        # ---- Step 5: concurrent edits at the same position ----------------
        banner("Step 5: concurrent edits — both clients insert at "
               "position 6 of a fresh document")
        send(clients, "C1", 'create design.txt "Hello World"', pause=0.5)
        send(clients, "C2", "subscribe design.txt", pause=0.8)
        send(clients, "C1", 'edit design.txt 6 "Distributed "', pause=0.02)
        send(clients, "C2", 'edit design.txt 6 "Systems "')
        time.sleep(1.2)          # let the update queues drain

        # ---- Step 6: final state seen by both ------------------------------
        banner("Step 6: final state as read by both clients")
        send(clients, "C1", "open design.txt")
        send(clients, "C2", "open design.txt")
        time.sleep(0.5)

        # ---- shut down cleanly ---------------------------------------------
        for name in ("C1", "C2"):
            clients[name].stdin.write(b"exit\n")
            clients[name].stdin.flush()
            clients[name].stdin.close()
        for name in ("C1", "C2"):
            clients[name].wait(timeout=10)
    finally:
        server.send_signal(signal.SIGINT)
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()

    # ---- render the transcript ---------------------------------------------
    lines = []
    lines.append("=" * 72)
    lines.append(" Collaborative Document Editor — recorded two-client demo")
    lines.append(" server: %s      clients: C1, C2 (real client.py processes)"
                 % ADDRESS)
    lines.append("=" * 72)
    previous = start
    for stamp, tag, text in events:
        lines.append("[%7.3fs] %-5s %s" % (stamp - start, tag, text))
    transcript = "\n".join(lines) + "\n"
    print(transcript)
    out_path = os.path.join(HERE, "demo_transcript.txt")
    with open(out_path, "w") as stream:
        stream.write(transcript)
    print("transcript written to %s" % out_path, file=sys.stderr)


if __name__ == "__main__":
    main()
