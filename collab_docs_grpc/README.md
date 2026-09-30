# Section 3 Q1 — Collaborative Document Editing with gRPC

A central **DocumentServer** keeps documents in memory; multiple **clients**
create, read and edit them concurrently, and subscribed clients receive
every change automatically via server streaming. (No
OT/CRDT: concurrent edits are applied in the order the server processes
them.)

## Design

* **Document state** (`src/server.py`) — `name → {content, version, Lock,
  subscriber queues}`. Every successful edit bumps `version`.
* **Synchronization** — a per-document lock (`threading.RLock`) makes each
  edit's read-modify-write atomic, so simultaneous edits cannot corrupt the
  text (they are applied one at a time, in the order they get the lock);
  different documents proceed fully in parallel. The registry (create) has
  its own lock.
* **Update propagation** — each subscriber gets its own bounded queue (256
  updates). While still holding the document lock, the edit pushes a
  `DocumentUpdate` (full new contents + version + editor id) to all queues,
  so every subscriber receives updates in increasing version order. A slow
  subscriber never blocks editors; if its queue is full it skips updates,
  and because each update carries the full text it is back in sync with the
  next one. Subscribers also receive an initial snapshot on attach, and the
  client ignores any update older than the version it already has.
* **Streaming** — `SubscribeToUpdates` is a server-streaming RPC; the
  handler blocks on the subscriber's queue until the client disconnects
  and is then removed cleanly.

## API (proto/docs.proto)

| RPC | Type | Notes |
|---|---|---|
| `CreateDocument` | unary | duplicate name → `ALREADY_EXISTS`, empty name → `INVALID_ARGUMENT` |
| `GetDocument` | unary | unknown name → `NOT_FOUND` |
| `EditDocument` | unary | insert `text` at `position` (`position == len` appends); bad position → `OUT_OF_RANGE` |
| `SubscribeToUpdates` | server-streaming | pushed after every modification |

## Usage

```bash
pip install grpcio grpcio-tools rich           # rich: the client's terminal UI
# generated stubs are included; regenerate only after editing the proto:
python3 -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/docs.proto

python3 src/server.py localhost:50051          # terminal 1 (server)
python3 src/client.py localhost:50051          # terminals 2..n (interactive CLI)
```

The client automatically launches in **rich interactive mode** when attached to a terminal:
* **Interactive TUI**: Colorful banners, document preview panels with edit diffs, and real-time telemetry tables (`status`, `list`).
* **Tab Auto-completion**: Native `readline` integration completing command names and tracked document names.
* **Non-Disruptive Push Notifications**: Streaming updates from peer clients are styled as highlighted cards and redisplay the active input prompt cleanly without line corruption.
* **Command History**: Preserved across sessions via `~/.doc_cli_history`.

Options:
* `-i`, `--interactive`: Force rich interactive mode.
* `-p`, `--plain`: Force plain text mode (for pipes/batch scripts).
* `-h`, `--help`: Show usage guide.

Commands: `create <name> ["<content>"]`, `open <name>`, `edit <name> <position> "<text>"`, `subscribe <name>`, `unsubscribe <name>`, `status`, `list`, `clear`, `help`, `exit`.

## Demonstration (as in classic MapReduce)

Terminal 1 (Client 1):

```
> create report.txt "Hello World"
[Client] Document report.txt created (v1).
> open report.txt
[Client] Hello World
> edit report.txt 6 "Distributed "
[Client] Edited report.txt -> v2: Hello Distributed World
```

Terminal 2 (Client 2):

```
> open report.txt
[Client] Hello World
> subscribe report.txt
[Client] Subscribed to updates for report.txt.
        ← (no polling; the next line arrives by itself)
[Update] Document report.txt modified (v2).
Hello Distributed World
```

Concurrent edits (two clients edit position 6 at the same time): the server
applies them in arrival order, bumps the version for each, and both
subscribers receive both updates — the document never corrupts.

`python3 demo_transcript.py` runs this whole scenario with two real client
processes and records it in `demo_transcript.txt` (create → both open →
subscribe → edit → automatic update → two edits at position 6 → both clients
read the same final text).

## Correctness

```bash
python3 tests/run_tests.py       # 13 checks
```

* create/open/duplicate/missing (`ALREADY_EXISTS`, `NOT_FOUND`);
* the exact PDF insert example (`"Hello World"` + `"Distributed "` @6);
* subscription auto-updates without polling (server-streaming, snapshot on
  attach, monotone versions);
* **concurrent edits**: 8 clients editing 8 disjoint documents in parallel
  (per-document locking keeps each document exact) and 8 clients editing
  the same position of one document simultaneously (every edit survives
  intact, version == 1 + number of edits);
* **subscribers under load**: 4 subscribers while 4 clients hammer one
  document — every subscriber ends on exactly the `GetDocument` state with
  version-ordered updates;
* error codes for out-of-range edits, missing documents, empty names;
* a scripted `client.py` run through the demo transcript.

## Files

| File | Purpose |
|---|---|
| `proto/docs.proto` | `DocumentService` + messages |
| `src/server.py` | In-memory documents, per-doc locks, subscriber fan-out |
| `src/client.py` | CLI client with background subscription threads |
| `tests/run_tests.py` | 13 end-to-end checks incl. concurrency and errors |
| `demo_transcript.py` | Runs a server and two real clients through the six demo steps |
| `demo_transcript.txt` | Recorded output of that session |
