#!/usr/bin/env python3
"""
DocumentServer — collaborative document editing over gRPC (Section 3 Q1).

Design
------
* Documents live in memory: name -> {content, version, lock, subscribers}.
* Concurrency: every document has its own Lock. Edits are serialized per
  document (the server may apply concurrent edits in arrival order, per
  a deliberate design choice), so an edit's read-modify-write never interleaves
  with another edit on the same document and the text cannot be corrupted.
  Different documents edit fully in parallel.
* Update propagation: each subscriber gets its own queue.Queue; after
  every successful edit (and creation) the server pushes a DocumentUpdate
  with the new full contents to all of that document's subscribers. A slow
  subscriber only delays its own queue (drops on overflow keep the server
  live), never the editor.
* SubscribeToUpdates is a server-streaming RPC: the handler blocks on the
  subscriber's queue and yields updates until the client disconnects.

Usage:
    python3 server.py localhost:50051
"""

import logging
import queue
import sys
import threading
from concurrent import futures

import grpc

import docs_pb2
import docs_pb2_grpc

SUBSCRIBER_QUEUE_DEPTH = 256


class Document:
    """One shared document plus its subscriber queues."""

    def __init__(self, name, content):
        self.name = name
        self.content = content
        self.version = 1
        self.lock = threading.RLock()
        self.subscribers = []       # list of queue.Queue

    def broadcast(self, update):
        """Push one update to every subscriber (drops on slow queues)."""
        with self.lock:
            for sub in self.subscribers:
                try:
                    sub.put_nowait(update)
                except queue.Full:
                    # Slow subscriber: drop this update rather than block
                    # every editor; the client can re-fetch with GetDocument.
                    pass


class DocumentServiceServicer(docs_pb2_grpc.DocumentServiceServicer):
    def __init__(self):
        self.registry_lock = threading.Lock()
        self.documents = {}         # name -> Document
        self.next_client_id = 1     # identifies editors in update streams

    # ------------------------------------------------------ create ------
    def CreateDocument(self, request, context):
        name = request.name.strip()
        if not name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "document name must not be empty")
        with self.registry_lock:
            if name in self.documents:
                context.abort(grpc.StatusCode.ALREADY_EXISTS,
                              "document '%s' already exists" % name)
            document = Document(name, request.initial_content)
            self.documents[name] = document
        logging.info("created '%s' (%d chars)", name,
                     len(request.initial_content))
        document.broadcast(docs_pb2.DocumentUpdate(
            name=name, content=request.initial_content, version=1,
            edited_by=0))
        return docs_pb2.CreateDocumentResponse(name=name, version=1)

    # --------------------------------------------------------- get ------
    def GetDocument(self, request, context):
        document = self.documents.get(request.name)
        if document is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "document '%s' not found" % request.name)
        with document.lock:
            return docs_pb2.GetDocumentResponse(name=document.name,
                                                content=document.content,
                                                version=document.version)

    # -------------------------------------------------------- edit ------
    def EditDocument(self, request, context):
        document = self.documents.get(request.name)
        if document is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "document '%s' not found" % request.name)
        editor_id = request.edited_by if request.edited_by else 0
        with document.lock:
            position = request.position
            if position < 0 or position > len(document.content):
                context.abort(grpc.StatusCode.OUT_OF_RANGE,
                              "position %d outside [0, %d]"
                              % (position, len(document.content)))
            document.content = (document.content[:position] + request.text +
                                document.content[position:])
            document.version += 1
            version = document.version
            content = document.content
            # Broadcast inside the lock so updates are enqueued in strict
            # monotonically increasing version order without races.
            document.broadcast(docs_pb2.DocumentUpdate(
                name=document.name, content=content, version=version,
                edited_by=editor_id))
        logging.info("edit '%s' at %d (+%d chars) -> v%d",
                     request.name, position, len(request.text), version)
        return docs_pb2.EditDocumentResponse(name=document.name,
                                             version=version, content=content)

    # --------------------------------------------------- subscribe ------
    def SubscribeToUpdates(self, request, context):
        document = self.documents.get(request.name)
        if document is None:
            context.abort(grpc.StatusCode.NOT_FOUND,
                          "document '%s' not found" % request.name)
        subscriptions = queue.Queue(maxsize=SUBSCRIBER_QUEUE_DEPTH)
        with document.lock:
            document.subscribers.append(subscriptions)
            # Send the current state immediately so the subscriber starts
            # from a consistent snapshot.
            subscriptions.put_nowait(docs_pb2.DocumentUpdate(
                name=document.name, content=document.content,
                version=document.version, edited_by=0))
        logging.info("subscriber joined '%s'", request.name)
        try:
            while context.is_active():
                try:
                    update = subscriptions.get(timeout=0.25)
                except queue.Empty:
                    continue
                yield update
        finally:
            with document.lock:
                try:
                    document.subscribers.remove(subscriptions)
                except ValueError:
                    pass
            logging.info("subscriber left '%s'", request.name)


def serve(address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=64))
    docs_pb2_grpc.add_DocumentServiceServicer_to_server(
        DocumentServiceServicer(), server)
    server.add_insecure_port(address)
    server.start()
    logging.info("document server listening on %s", address)
    return server


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [server] %(message)s")
    if len(sys.argv) != 2:
        print("usage: python3 server.py <address>   "
              "e.g. python3 server.py localhost:50051", file=sys.stderr)
        sys.exit(2)
    server = serve(sys.argv[1])
    server.wait_for_termination()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
