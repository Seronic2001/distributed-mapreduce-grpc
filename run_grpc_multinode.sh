#!/bin/bash
# ==============================================================================
# Multi-Node gRPC Execution Helper for RCE Cluster
# Implements the deployment topology from rce_grpc_execution_guide.pdf:
#   Node 1: Server / Coordinator
#   Node 2: Client 1 / Worker 1
#   Node 3: Client 2 / Worker 2
# ==============================================================================

set -euo pipefail
cd "$(dirname "$0")"

# Load gRPC module if available on the cluster cluster
module load gRPC/1.74.1 2>/dev/null || module load gRPC 2>/dev/null || true
module load python/3.8 2>/dev/null || module load python/3.9 2>/dev/null || module load python3 2>/dev/null || true

# Discover allocated nodes
if [ -n "${SLURM_JOB_NODELIST:-}" ]; then
    NODES=($(scontrol show hostnames "$SLURM_JOB_NODELIST"))
else
    NODES=("localhost" "localhost" "localhost")
fi

NUM_NODES=${#NODES[@]}
SERVER_NODE="${NODES[0]}"
CLIENT1_NODE="${NODES[1]:-$SERVER_NODE}"
CLIENT2_NODE="${NODES[2]:-$CLIENT1_NODE}"

echo "============================================================"
echo "Multi-Node gRPC Execution"
echo "Date: $(date)"
echo "Detected Nodes: ${NODES[*]}"
echo "  Server / Coordinator Node: $SERVER_NODE"
echo "  Client 1 / Worker 1 Node:  $CLIENT1_NODE"
echo "  Client 2 / Worker 2 Node:  $CLIENT2_NODE"
echo "============================================================"
echo ""

# Helper to execute a command on a target node
run_on_node() {
    local node="$1"
    shift
    if [ "$node" = "localhost" ] || [ "$node" = "$(hostname)" ] || [ "$node" = "$(hostname -s)" ]; then
        "$@"
    else
        ssh -o StrictHostKeyChecking=no "$node" "cd '$PWD' && $*"
    fi
}

# -------------------------------------------------------------
# Demonstration: Section 2 Q2 (Weather Analytics Python gRPC Streaming)
# -------------------------------------------------------------
echo "============================================================"
echo ">>> Demonstrating Section 2 Q2: Weather Analytics (Python gRPC)"
echo "============================================================"

WEATHER_PORT=50060
WEATHER_ADDR="${SERVER_NODE}:${WEATHER_PORT}"

echo "1. Starting Weather Coordinator on ${SERVER_NODE}:${WEATHER_PORT}..."
run_on_node "$SERVER_NODE" "nohup python3 weather_grpc_streaming/src/server.py 0.0.0.0:${WEATHER_PORT} > /tmp/weather_coord.log 2>&1 &"
sleep 1

echo "2. Starting Weather Worker 1 on ${CLIENT1_NODE}:50061 and Worker 2 on ${CLIENT2_NODE}:50062..."
run_on_node "$CLIENT1_NODE" "nohup python3 weather_grpc_streaming/src/worker.py 0.0.0.0:50061 ${WEATHER_ADDR} --advertise ${CLIENT1_NODE}:50061 > /tmp/w1.log 2>&1 &"
run_on_node "$CLIENT2_NODE" "nohup python3 weather_grpc_streaming/src/worker.py 0.0.0.0:50062 ${WEATHER_ADDR} --advertise ${CLIENT2_NODE}:50062 > /tmp/w2.log 2>&1 &"
sleep 2

echo "3. Streaming sample dataset from ${CLIENT1_NODE} to Coordinator on ${SERVER_NODE}..."
run_on_node "$CLIENT1_NODE" "python3 weather_grpc_streaming/src/client.py --dataset weather_baselines/tests/sample.txt --coordinator ${WEATHER_ADDR} --batch-size 3"

echo "4. Querying CLI Dashboard from ${CLIENT2_NODE}..."
run_on_node "$CLIENT2_NODE" "python3 weather_grpc_streaming/src/dashboard.py --coordinator ${WEATHER_ADDR} --once"

# Cleanup weather services
run_on_node "$SERVER_NODE" "pkill -f 'weather_grpc_streaming/src/server.py' 2>/dev/null || true"
run_on_node "$CLIENT1_NODE" "pkill -f 'weather_grpc_streaming/src/worker.py' 2>/dev/null || true"
run_on_node "$CLIENT2_NODE" "pkill -f 'weather_grpc_streaming/src/worker.py' 2>/dev/null || true"
echo "✓ Section 2 Q2 Python multi-node demonstration complete."
echo ""

# -------------------------------------------------------------
# Demonstration: Section 3 Q1 (Collaborative Document Editing)
# -------------------------------------------------------------
echo "============================================================"
echo ">>> Demonstrating Section 3 Q1: Collaborative Document Editing"
echo "============================================================"

DOCS_PORT=50095
DOCS_ADDR="${SERVER_NODE}:${DOCS_PORT}"

echo "1. Starting Document Server on ${SERVER_NODE}:${DOCS_PORT}..."
if [ "$SERVER_NODE" = "localhost" ] || [ "$SERVER_NODE" = "$(hostname)" ] || [ "$SERVER_NODE" = "$(hostname -s)" ]; then
    python3 collab_docs_grpc/src/server.py "0.0.0.0:${DOCS_PORT}" &
    SERVER_PID=$!
else
    ssh -f -o StrictHostKeyChecking=no "$SERVER_NODE" "cd '$PWD' && nohup python3 collab_docs_grpc/src/server.py 0.0.0.0:${DOCS_PORT} > /tmp/docs_server.log 2>&1 &"
    SERVER_PID=""
fi

cleanup_docs() {
    echo "Stopping Document Server..."
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
    run_on_node "$SERVER_NODE" pkill -f "collab_docs_grpc/src/server.py" 2>/dev/null || true
}
trap cleanup_docs EXIT

sleep 2

echo "2. Running scripted two-client interactive session (Client 1 on $CLIENT1_NODE, Client 2 on $CLIENT2_NODE)..."
# We run the demo script pointing to the cluster server address
python3 -c "
import sys, time
sys.path.insert(0, 'collab_docs_grpc/src')
import grpc, docs_pb2, docs_pb2_grpc

channel = grpc.insecure_channel('$DOCS_ADDR')
stub = docs_pb2_grpc.DocumentServiceStub(channel)

print('[Client 1 on $CLIENT1_NODE] Creating document report.txt...')
resp = stub.CreateDocument(docs_pb2.CreateDocumentRequest(name='report.txt', initial_content='Hello World'))
print(f'[Server Response] Document {resp.name} created (v{resp.version})')

print('[Client 2 on $CLIENT2_NODE] Opening document report.txt...')
resp = stub.GetDocument(docs_pb2.GetDocumentRequest(name='report.txt'))
print(f'[Client 2 Content] {resp.content}')

print('[Client 1 on $CLIENT1_NODE] Editing report.txt (inserting \"Distributed \" at pos 6)...')
resp = stub.EditDocument(docs_pb2.EditDocumentRequest(name='report.txt', position=6, text='Distributed '))
print(f'[Server Response] Edited -> v{resp.version}: {resp.content}')

print('[Client 2 on $CLIENT2_NODE] Reading updated document...')
resp = stub.GetDocument(docs_pb2.GetDocumentRequest(name='report.txt'))
print(f'[Client 2 Final Content] {resp.content}')
"

cleanup_docs
trap - EXIT

echo ""
echo "============================================================"
echo "Multi-Node gRPC Demonstration Completed!"
echo "============================================================"
