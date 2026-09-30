#!/usr/bin/env python3
"""
End-to-end correctness tests for the collaborative document service
(Section 3 Q1).

Covers the full feature checklist:
  * Document management  : create + retrieve (duplicate -> ALREADY_EXISTS)
  * Document editing     : insert at position, incl. the PDF example
                           ("Hello World", insert "Distributed " at 6)
  * Concurrent access    : many clients editing one document simultaneously;
                           final text must be a permutation-preserving
                           merge (no corruption, no lost characters)
  * Update propagation   : subscribers receive edits without polling
  * Streaming            : SubscribeToUpdates behaves as server-streaming
  * Synchronization      : version numbers increase monotonically; the
                           server serializes edits per document
  * Errors               : NOT_FOUND / ALREADY_EXISTS / OUT_OF_RANGE /
                           INVALID_ARGUMENT status codes
Plus a CLI smoke test driving client.py through the demo transcript.
"""

import os
import signal
import subprocess
import sys
import threading
import time
from concurrent import futures

import grpc

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)

import docs_pb2                    # noqa: E402
import docs_pb2_grpc               # noqa: E402

failures = []


def check(condition, name, detail=""):
    if condition:
        print("%s: PASS" % name)
    else:
        failures.append(name)
        print("%s: FAIL %s" % (name, detail))


class Server:
    def __init__(self, port):
        self.proc = subprocess.Popen(
            [sys.executable, os.path.join(SRC, "server.py"),
             "127.0.0.1:%d" % port],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        self.address = "127.0.0.1:%d" % port
        # Wait for the TCP listener (a refused connection must NOT count
        # as ready; gRPC surfaces it as RpcError, so poll the socket).
        import socket
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("server exited early on port %d" % port)
            with socket.socket() as sock:
                sock.settimeout(0.3)
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    return
            time.sleep(0.15)
        raise RuntimeError("server did not start on %d" % port)

    def stop(self):
        try:
            self.proc.terminate()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def make_stub(address):
    channel = grpc.insecure_channel(address)
    return docs_pb2_grpc.DocumentServiceStub(channel), channel


def collect_updates(call, sink, stop, count):
    """Read `count` updates into sink, then stop the call."""
    try:
        for index, update in enumerate(call):
            sink.append(update)
            if index + 1 >= count:
                break
    except grpc.RpcError:
        pass
    finally:
        stop.set()


def main():
    server = Server(50601)
    try:
        stub, channel = make_stub(server.address)

        # ---------------- management ----------------
        reply = stub.CreateDocument(docs_pb2.CreateDocumentRequest(
            name="report.txt", initial_content="Hello World"))
        check(reply.version == 1, "create_document")
        try:
            stub.CreateDocument(docs_pb2.CreateDocumentRequest(
                name="report.txt", initial_content="x"))
            check(False, "duplicate_create", "no error raised")
        except grpc.RpcError as error:
            check(error.code() == grpc.StatusCode.ALREADY_EXISTS,
                  "duplicate_create", str(error.code()))

        got = stub.GetDocument(docs_pb2.GetDocumentRequest(name="report.txt"))
        check(got.content == "Hello World" and got.version == 1,
              "open_document", got.content)

        try:
            stub.GetDocument(docs_pb2.GetDocumentRequest(name="missing"))
            check(False, "missing_document", "no error raised")
        except grpc.RpcError as error:
            check(error.code() == grpc.StatusCode.NOT_FOUND,
                  "missing_document", str(error.code()))

        # ---------------- PDF insert example ----------------
        reply = stub.EditDocument(docs_pb2.EditDocumentRequest(
            name="report.txt", position=6, text="Distributed "))
        check(reply.content == "Hello Distributed World" and reply.version == 2,
              "edit_pdf_example", reply.content)

        # ---------------- subscription / propagation ----------------
        call = stub.SubscribeToUpdates(docs_pb2.UpdateRequest(name="report.txt"))
        updates = []
        done = threading.Event()
        reader = threading.Thread(target=collect_updates,
                                  args=(call, updates, done, 2), daemon=True)
        reader.start()
        time.sleep(0.4)   # let the initial snapshot arrive
        stub.EditDocument(docs_pb2.EditDocumentRequest(
            name="report.txt", position=0, text="CS"))
        reader.join(timeout=5)
        call.cancel()
        kinds = [update.content for update in updates]
        check(any(content == "Hello Distributed World" for content in kinds) and
              any(content == "CSHello Distributed World" for content in kinds),
              "subscribe_updates", str(kinds))

        # ---------------- concurrent edits ----------------
        # EditDocument is insert-at-position and the server applies
        # concurrent edits in arrival order (no
        # OT/CRDT is required). A client computing a position from stale
        # state can land inside other clients' text — inherent to the
        # semantics, not corruption. The tests exercise the two
        # well-defined concurrent patterns:
        #   A) many clients editing DISJOINT documents simultaneously;
        #   B) many clients editing the SAME position of ONE document —
        #      the server must serialize the racing edits so every edit
        #      survives intact (no nesting, no lost edits).
        n_clients, edits_per_client = 8, 25
        token_width = 7                       # "<%02d-%02d>" -> "<07-24>"
        hard_errors = []
        lock = threading.Lock()

        # Scenario A: disjoint documents, self-consistent appends.
        for worker in range(n_clients):
            stub.CreateDocument(docs_pb2.CreateDocumentRequest(
                name="doc%02d.txt" % worker, initial_content=""))

        def hammer_own_document(worker_id):
            try:
                with grpc.insecure_channel(server.address) as channel2:
                    stub2 = docs_pb2_grpc.DocumentServiceStub(channel2)
                    name = "doc%02d.txt" % worker_id
                    position = 0
                    for step in range(edits_per_client):
                        reply = stub2.EditDocument(
                            docs_pb2.EditDocumentRequest(
                                name=name, position=position,
                                text="<%02d-%02d>" % (worker_id, step)))
                        position = len(reply.content)
            except grpc.RpcError as error:
                with lock:
                    hard_errors.append(str(error.code()))

        threads = [threading.Thread(target=hammer_own_document, args=(w,))
                   for w in range(n_clients)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        disjoint_ok = True
        for worker in range(n_clients):
            got = stub.GetDocument(docs_pb2.GetDocumentRequest(
                name="doc%02d.txt" % worker))
            expected_region = "".join(
                "<%02d-%02d>" % (worker, s) for s in range(edits_per_client))
            if got.content != expected_region:
                disjoint_ok = False
        check(not hard_errors and disjoint_ok, "concurrent_edits_disjoint_docs",
              "errors=%s disjoint_ok=%s" % (hard_errors[:2], disjoint_ok))

        # Scenario B: 8 clients prepend at the SAME position 0 of one
        # document simultaneously. The server applies them in arrival
        # order; every token must survive intact (no nesting/corruption).
        stub.CreateDocument(docs_pb2.CreateDocumentRequest(
            name="prepend.txt", initial_content=""))

        def hammer_prepend(worker_id):
            with grpc.insecure_channel(server.address) as channel2:
                stub2 = docs_pb2_grpc.DocumentServiceStub(channel2)
                for step in range(edits_per_client):
                    stub2.EditDocument(docs_pb2.EditDocumentRequest(
                        name="prepend.txt", position=0,
                        text="<%02d-%02d>" % (worker_id, step)))

        threads = [threading.Thread(target=hammer_prepend, args=(w,))
                   for w in range(n_clients)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        final = stub.GetDocument(
            docs_pb2.GetDocumentRequest(name="prepend.txt"))
        tokens = sorted(final.content[i:i + token_width]
                        for i in range(0, len(final.content), token_width))
        expected = sorted("<%02d-%02d>" % (w, s)
                          for w in range(n_clients)
                          for s in range(edits_per_client))
        check(tokens == expected and
              final.version == 1 + n_clients * edits_per_client,
              "concurrent_edits_same_position",
              "version=%d tokens=%d/%d content_ok=%s"
              % (final.version, len(tokens), len(expected),
                 tokens == expected))

        # ---------------- subscribers under concurrent load ----------------
        # 4 subscribers on one document while 4 clients hammer it; every
        # subscriber must reach the final state without polling, updates
        # must be version-ordered, and the last update must match
        # GetDocument exactly.
        stub.CreateDocument(docs_pb2.CreateDocumentRequest(
            name="load.txt", initial_content=""))
        calls, sinks = [], []
        for _ in range(4):
            call = stub.SubscribeToUpdates(
                docs_pb2.UpdateRequest(name="load.txt"))
            sink = []
            done = threading.Event()
            threading.Thread(target=collect_updates,
                             args=(call, sink, done, 101), daemon=True).start()
            calls.append(call)
            sinks.append(sink)
        time.sleep(0.5)   # let the subscriptions attach

        def hammer_load(worker_id):
            with grpc.insecure_channel(server.address) as channel2:
                stub2 = docs_pb2_grpc.DocumentServiceStub(channel2)
                for step in range(edits_per_client):
                    stub2.EditDocument(docs_pb2.EditDocumentRequest(
                        name="load.txt", position=0,
                        text="<%02d-%02d>" % (worker_id, step)))

        threads = [threading.Thread(target=hammer_load, args=(w,))
                   for w in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        time.sleep(1.0)   # let the update queues drain
        final = stub.GetDocument(docs_pb2.GetDocumentRequest(name="load.txt"))
        subs_ok = True
        for sink in sinks:
            versions = [update.version for update in sink]
            if (len(sink) < 101 or
                    versions != sorted(versions) or
                    sink[-1].content != final.content or
                    sink[-1].version != final.version):
                subs_ok = False
        for call in calls:
            call.cancel()
        check(subs_ok, "subscribers_under_load",
              "sink sizes=%s" % [len(sink) for sink in sinks])

        # ---------------- invalid edit position ----------------
        try:
            stub.EditDocument(docs_pb2.EditDocumentRequest(
                name="report.txt", position=10**6, text="x"))
            check(False, "edit_out_of_range", "no error raised")
        except grpc.RpcError as error:
            check(error.code() == grpc.StatusCode.OUT_OF_RANGE,
                  "edit_out_of_range", str(error.code()))
        try:
            stub.EditDocument(docs_pb2.EditDocumentRequest(
                name="missing", position=0, text="x"))
            check(False, "edit_missing_doc", "no error raised")
        except grpc.RpcError as error:
            check(error.code() == grpc.StatusCode.NOT_FOUND,
                  "edit_missing_doc", str(error.code()))
        try:
            stub.CreateDocument(docs_pb2.CreateDocumentRequest(name="  "))
            check(False, "create_empty_name", "no error raised")
        except grpc.RpcError as error:
            check(error.code() == grpc.StatusCode.INVALID_ARGUMENT,
                  "create_empty_name", str(error.code()))

        channel.close()

        # ---------------- CLI demo transcript ----------------
        cli = subprocess.run(
            [sys.executable, os.path.join(SRC, "client.py"), server.address],
            input='create demo.txt "Hello World"\n'
                  'open demo.txt\n'
                  'edit demo.txt 6 "Distributed "\n'
                  'subscribe demo.txt\n'
                  'exit\n',
            capture_output=True, text=True, timeout=30)
        check(cli.returncode == 0 and "Hello Distributed World" in cli.stdout,
              "cli_demo", cli.stdout[-300:] + cli.stderr[-300:])
    finally:
        server.stop()

    print("----------------------------------------")
    if failures:
        print("%d FAILED: %s" % (len(failures), ", ".join(failures)))
        return 1
    print("all document service tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
