#!/bin/bash
# ==============================================================================
# End-to-End Automation Script for MapReduce & gRPC Suite on a Slurm Cluster
# Configure via CLUSTER_USER / CLUSTER_HOST env vars
# ==============================================================================

set -e

CLUSTER_USER="${CLUSTER_USER:-your_username}"
CLUSTER_HOST="${CLUSTER_HOST:-cluster.example.edu}"
REMOTE_TARGET="${CLUSTER_USER}@${CLUSTER_HOST}"
REMOTE_DIR="~/mapreduce_grpc_suite"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
RED="\033[0;31m"
CYAN="\033[0;36m"
NC="\033[0m"

# SSH ControlMaster socket for connection sharing (avoids multiple password prompts)
SSH_SOCKET="/tmp/ssh_mux_${CLUSTER_USER}_${CLUSTER_HOST}_$$"
SSH_OPTS="-o ControlMaster=auto -o ControlPath=${SSH_SOCKET} -o ControlPersist=600"

cleanup() {
    ssh -O exit -o ControlPath="${SSH_SOCKET}" "${REMOTE_TARGET}" 2>/dev/null || true
}
trap cleanup EXIT

# ------------------------------------------------------------------------------
# Functions
# ------------------------------------------------------------------------------

sync_push() {
    echo -e "${BLUE}[1/5] Syncing code, tests, and scripts to cluster...${NC}"
    rsync -avz -e "ssh ${SSH_OPTS}" \
        --exclude='build/' \
        --exclude='.git/' \
        --exclude='.gemini/' \
        --exclude='__pycache__/' \
        --exclude='*.pyc' \
        --exclude='*.png' \
        --exclude='*.pdf' \
        --exclude='results/*.csv' \
        --exclude='bench_*.log' \
        --exclude='bench_*.err' \
        --exclude='benchmark_dist_results_*.out' \
        --exclude='benchmark_dist_results_*.err' \
        "${LOCAL_DIR}/" "${REMOTE_TARGET}:${REMOTE_DIR}/"
    echo -e "${GREEN}✓ Code & test fixtures synced to ${REMOTE_TARGET}:${REMOTE_DIR}/${NC}\n"
}

submit_job() {
    local script="${1:-slurm_bench_multinode.sh}"
    shift || true
    local extra_args="$*"
    echo -e "${BLUE}[2/5] Submitting SLURM benchmark job (${CYAN}${script}${extra_args:+ ${extra_args}}${BLUE}) on cluster...${NC}"
    JOB_OUTPUT=$(ssh ${SSH_OPTS} "${REMOTE_TARGET}" "cd ${REMOTE_DIR} && sbatch ${extra_args} ${script}")
    echo "$JOB_OUTPUT"
    JOB_ID=$(echo "$JOB_OUTPUT" | grep -o '[0-9]\+')

    if [ -z "$JOB_ID" ]; then
        echo -e "${RED}Error: Failed to extract SLURM Job ID.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Job submitted successfully: ID ${CYAN}${JOB_ID}${NC}\n"
}

wait_job() {
    local jid="$1"
    echo -e "${BLUE}[3/5] Monitoring Job ID ${CYAN}${jid}${BLUE} in queue...${NC}"
    
    while true; do
        STATUS=$(ssh ${SSH_OPTS} "${REMOTE_TARGET}" "squeue -j ${jid} -h -o '%T' 2>/dev/null || true")
        
        if [ -z "$STATUS" ]; then
            echo -e "\n${GREEN}✓ Job ${jid} finished execution.${NC}\n"
            break
        elif [ "$STATUS" = "PENDING" ]; then
            echo -ne "${YELLOW}\r[STATUS] Job ${jid} is PENDING in queue (waiting for resource allocation)...   ${NC}"
        elif [ "$STATUS" = "RUNNING" ]; then
            echo -ne "${CYAN}\r[STATUS] Job ${jid} is RUNNING on cluster...                             ${NC}"
        else
            echo -ne "\r[STATUS] Job ${jid} status: ${STATUS}...                                  "
        fi
        sleep 5
    done
}

fetch_results() {
    echo -e "${BLUE}[4/5] Downloading benchmark logs, CSVs, and plots from cluster...${NC}"
    rsync -avz -e "ssh ${SSH_OPTS}" \
        --include='*/' \
        --include='results/**' \
        --include='*.png' \
        --include='bench_*.log' \
        --include='bench_*.err' \
        --include='benchmark_dist_results_*.out' \
        --include='benchmark_dist_results_*.err' \
        --include='demo_transcript.txt' \
        --exclude='*' \
        "${REMOTE_TARGET}:${REMOTE_DIR}/" "${LOCAL_DIR}/"
    echo -e "${GREEN}✓ Results downloaded to local workspace.${NC}\n"
}

regenerate_report() {
    echo -e "${BLUE}[5/5] Generating plots and refreshing report figures locally...${NC}"
    mkdir -p "${LOCAL_DIR}/analysis/figures"

    # 1. Report figures: every benchmark figure, drawn from the result CSVs
    echo -e "${CYAN}Rendering report figures (analysis/make_figures.py)...${NC}"
    python3 "${LOCAL_DIR}/analysis/make_figures.py" || true

    # 2. Per-module plots (kept in each module's results/, not used by the summary figures)
    if [ -f "${LOCAL_DIR}/weather_mapreduce/results/comparison.csv" ]; then
        echo -e "${CYAN}Rendering Weather MapReduce comparison plots...${NC}"
        python3 "${LOCAL_DIR}/weather_mapreduce/plots.py" "${LOCAL_DIR}/weather_mapreduce/results/comparison.csv" || true
    fi
    if [ -f "${LOCAL_DIR}/weather_grpc_streaming/results/grpc_benchmark.csv" ]; then
        echo -e "${CYAN}Rendering Weather gRPC throughput & latency plots...${NC}"
        python3 "${LOCAL_DIR}/weather_grpc_streaming/plots.py" "${LOCAL_DIR}/weather_grpc_streaming/results/grpc_benchmark.csv" || true
    fi

    echo -e "\n${GREEN}======================================================${NC}"
    echo -e "${GREEN}✓ Local Plots Generated in results/ and analysis/figures/!${NC}"
    echo -e "${GREEN}======================================================${NC}"
}

status_job() {
    echo -e "${CYAN}=== Active SLURM Jobs for ${CLUSTER_USER} ===${NC}"
    ssh ${SSH_OPTS} "${REMOTE_TARGET}" "squeue -u ${CLUSTER_USER}"
}

tail_logs() {
    local jid="$1"
    if [ -z "$jid" ]; then
        echo -e "${YELLOW}Fetching latest benchmark log file...${NC}"
        ssh ${SSH_OPTS} "${REMOTE_TARGET}" "cd ${REMOTE_DIR} && ls -t bench_*.log 2>/dev/null | head -n 1 | xargs -r cat"
    else
        ssh ${SSH_OPTS} "${REMOTE_TARGET}" "cd ${REMOTE_DIR} && cat bench_${jid}.log 2>/dev/null || cat bench_${jid}.err 2>/dev/null || cat benchmark_dist_results_${jid}.out 2>/dev/null"
    fi
}

interactive_test() {
    echo -e "${YELLOW}Starting interactive 3-node allocation (per rce_grpc_execution_guide.pdf)...${NC}"
    echo -e "${CYAN}Once granted, you will be inside the compute allocation.${NC}"
    ssh -t ${SSH_OPTS} "${REMOTE_TARGET}" "cd ${REMOTE_DIR} && salloc --nodes=3 --ntasks-per-node=1 --partition=debug --time=01:00:00"
}

demo_grpc_cluster() {
    echo -e "${CYAN}Running multi-node gRPC demonstration across allocated compute nodes...${NC}"
    ssh -t ${SSH_OPTS} "${REMOTE_TARGET}" "cd ${REMOTE_DIR} && bash run_grpc_multinode.sh"
}

setup_ssh_key() {
    echo -e "${CYAN}Setting up passwordless SSH key for ${REMOTE_TARGET}...${NC}"
    if [ ! -f "$HOME/.ssh/id_rsa.pub" ] && [ ! -f "$HOME/.ssh/id_ed25519.pub" ]; then
        echo "Generating SSH key pair..."
        ssh-keygen -t ed25519 -N "" -f "$HOME/.ssh/id_ed25519"
    fi
    ssh-copy-id "${REMOTE_TARGET}"
    echo -e "${GREEN}✓ SSH key copied. You can now run commands without typing your password!${NC}"
}

setup_cluster_env() {
    echo -e "${CYAN}Setting up environment and dependencies (module load gRPC/1.74.1, pip) on cluster...${NC}"
    ssh ${SSH_OPTS} "${REMOTE_TARGET}" "
        module load gRPC/1.74.1 2>/dev/null || module load gRPC 2>/dev/null || true
        module load python/3.8 2>/dev/null || module load python/3.9 2>/dev/null || module load python3 2>/dev/null || true
        python3 -m pip install --user --upgrade grpcio grpcio-tools matplotlib 2>&1 || pip install --user grpcio grpcio-tools matplotlib || true
    "
    echo -e "${GREEN}✓ Cluster environment setup complete!${NC}\n"
}

print_help() {
    echo -e "${CYAN}MapReduce & gRPC Suite Cluster Automation Tool${NC}"
    echo "Usage: ./run_cluster.sh [command] [options]"
    echo ""
    echo "Commands:"
    echo "  all (default)  : Push code -> Run multi-node benchmark -> Wait -> Fetch CSVs -> Plot"
    echo "                   (e.g., ./run_cluster.sh or ./run_cluster.sh all 4 for 4 nodes)"
    echo "  push           : Sync local code & test fixtures to cluster"
    echo "  submit [script]: Push latest code and submit specified SLURM script"
    echo "  status         : Show current SLURM queue status for your user"
    echo "  wait [JOB_ID]  : Wait for a specific job to finish, then fetch results and plot"
    echo "  fetch          : Download results, CSVs, logs, and plots from cluster to local"
    echo "  plot           : Regenerate all plots locally from fetched CSV files"
    echo "  logs [JOB_ID]  : View benchmark output log on cluster"
    echo "  demo-grpc      : Run automated multi-node gRPC demo across cluster compute nodes"
    echo "  setup-ssh      : Copy your local SSH key to the cluster for passwordless automation"
    echo "  setup-env      : Install grpcio, grpcio-tools, and matplotlib on cluster user account"
    echo "  help           : Show this help message"
}

# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------

if [ -n "${1:-}" ] && [ "$1" -eq "$1" ] 2>/dev/null; then
    CMD="all"
    NODES="$1"
else
    CMD="${1:-all}"
    NODES="${2:-}"
fi

case "$CMD" in
    all|run|bench|multinode|multi)
        sync_push
        EXTRA=""
        if [ -n "$NODES" ] && [ "$NODES" -eq "$NODES" ] 2>/dev/null; then
            EXTRA="--nodes=${NODES}"
        fi
        submit_job "slurm_bench_multinode.sh" $EXTRA
        wait_job "$JOB_ID"
        fetch_results
        regenerate_report
        ;;
    push|sync)
        sync_push
        ;;
    submit)
        sync_push
        submit_job "${2:-slurm_bench.sh}"
        ;;
    status|queue)
        status_job
        ;;
    wait)
        if [ -z "$2" ]; then
            echo -e "${RED}Usage: ./run_cluster.sh wait <JOB_ID>${NC}"
            exit 1
        fi
        wait_job "$2"
        fetch_results
        regenerate_report
        ;;
    pull|fetch)
        fetch_results
        regenerate_report
        ;;
    plot|plots)
        regenerate_report
        ;;
    logs|log)
        tail_logs "$2"
        ;;
    interactive|debug)
        interactive_test
        ;;
    demo-grpc)
        demo_grpc_cluster
        ;;
    setup-ssh|key)
        setup_ssh_key
        ;;
    setup-env|pip|env)
        setup_cluster_env
        ;;
    help|--help|-h)
        print_help
        ;;
    *)
        echo -e "${RED}Unknown command: $CMD${NC}"
        print_help
        exit 1
        ;;
esac
