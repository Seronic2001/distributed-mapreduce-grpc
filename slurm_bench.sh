#!/bin/bash
#SBATCH --job-name=mr-grpc-bench
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=02:00:00
#SBATCH --output=bench_%j.log
#SBATCH --error=bench_%j.err
#SBATCH --partition=debug

# ==============================================================================
# Single-Node Benchmark: Only CSVs and logs are generated on the cluster.
# All PNG plots and charts will be generated LOCALLY after fetch.
# ==============================================================================

module load hpcx-2.7.0/hpcx-ompi 2>/dev/null || true
module load gRPC/1.74.1 2>/dev/null || module load gRPC 2>/dev/null || true
module load python/3.8 2>/dev/null || module load python/3.9 2>/dev/null || module load python/3.10 2>/dev/null || module load python3 2>/dev/null || true

export OMPI_MCA_rmaps_base_oversubscribe=1
export OMPI_MCA_btl_vader_single_copy_mechanism=none
export PRTE_MCA_rmaps_default_mapping_policy=:oversubscribe

export PATH="$HOME/.local/bin:$PATH"
export PYTHONUSERBASE="${HOME}/.local"

cd "${SLURM_SUBMIT_DIR:-}" 2>/dev/null || cd "$(dirname "$0")"

echo "========================================="
echo "SLURM Job ID: ${SLURM_JOB_ID:-local}"
echo "Date: $(date)"
echo "Host: $(hostname)"
echo "Allocated nodes: ${SLURM_NNODES:-1}"
echo "Total tasks: ${SLURM_NTASKS:-8}"
echo "Node list: ${SLURM_NODELIST:-localhost}"
echo "Python: $(which python3) ($(python3 --version 2>&1))"
echo "========================================="
echo ""

# -------------------------------------------------------------
# 0. Build binaries & generate proto stubs
# -------------------------------------------------------------
echo "=== Step 0: Compiling C++ binaries and Protobuf stubs ==="
mkdir -p weather_baselines/build weather_mapreduce/build matmul_mapreduce/results

echo "Building MapReduce C++ binaries..."
(cd weather_mapreduce && make)

echo "Building sequential reference and MPI binaries..."
(cd weather_baselines && g++ -O3 -std=c++17 -o build/q8_sequential src/q8_sequential.cpp)
if command -v mpicxx >/dev/null 2>&1; then
    (cd weather_baselines && mpicxx -O3 -std=c++17 -o build/q8_mpi src/q8_mpi.cpp 2>/dev/null || true)
fi

if python3 -c "import grpc_tools.protoc" 2>/dev/null; then
    (cd weather_grpc_streaming && python3 -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/weather.proto)
    (cd collab_docs_grpc && python3 -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/docs.proto)
fi
echo "✓ Binaries and protos ready."
echo ""

# -------------------------------------------------------------
# 1. Section 1 Q1: Matrix Multiplication (MapReduce)
# -------------------------------------------------------------
echo "=== Step 1: Section 1 Q1 — Matrix Multiplication (Row-Row MapReduce) ==="
echo "Running correctness suite (29 tests)..."
(cd matmul_mapreduce && bash tests/run_tests.sh)

echo "Running SLURM matmul benchmark..."
mkdir -p matmul_mapreduce/bench_data
python3 matmul_mapreduce/gen_matrices.py --m 200 --n 200 --p 200 --seed 42 \
    --out-a matmul_mapreduce/bench_data/A_200.txt --out-b matmul_mapreduce/bench_data/B_200.txt

export MR_TIMINGS="$PWD/matmul_mapreduce/results/matmul_timings.csv"
bash matmul_mapreduce/matmul_slurm.sh \
    matmul_mapreduce/bench_data/A_200.txt \
    matmul_mapreduce/bench_data/B_200.txt \
    matmul_mapreduce/bench_data/C_200.txt ${SLURM_NTASKS:-8}
echo "✓ S1 Q1 completed (timing recorded to matmul_mapreduce/results/matmul_timings.csv)."
echo ""

# -------------------------------------------------------------
# 2. Section 2 Q1: Weather Analytics (Hadoop MapReduce / Slurm)
# -------------------------------------------------------------
echo "=== Step 2: Section 2 Q1 — Weather Analytics (MapReduce) ==="
echo "Running correctness suite (24 tests)..."
(cd weather_mapreduce && bash tests/run_tests.sh)

echo "Running MPI vs MapReduce benchmark (generating CSV only)..."
(cd weather_mapreduce && bash benchmark.sh)
echo "✓ S2 Q1 completed (results written to weather_mapreduce/results/comparison.csv)."
echo ""

# -------------------------------------------------------------
# 3. Section 2 Q2: Weather Analytics (gRPC Real-Time Streaming)
# -------------------------------------------------------------
echo "=== Step 3: Section 2 Q2 — Weather Analytics (gRPC Streaming) ==="
if python3 -c "import grpc" 2>/dev/null; then
    echo "Running correctness suite..."
    (cd weather_grpc_streaming && python3 tests/run_tests.py)
    echo "Running gRPC throughput & latency benchmark (generating CSV only)..."
    (cd weather_grpc_streaming && python3 benchmark_grpc.py --sizes 100000 --workers 1 2 4 --batches 1000 5000 --reps 2 --queries 1 --no-plots)
    echo "✓ S2 Q2 completed (results written to weather_grpc_streaming/results/grpc_benchmark.csv)."
else
    echo "WARNING: python3 'grpc' module not found on cluster node."
    echo "To install on cluster, run: ./run_cluster.sh setup-env"
fi
echo ""

# -------------------------------------------------------------
# 4. Section 3 Q1: Collaborative Document Editing (gRPC)
# -------------------------------------------------------------
echo "=== Step 4: Section 3 Q1 — Collaborative Document Editing (gRPC) ==="
if python3 -c "import grpc" 2>/dev/null; then
    echo "Running correctness suite (13 tests)..."
    (cd collab_docs_grpc && python3 tests/run_tests.py)
    echo "Generating recorded two-client demo transcript..."
    (cd collab_docs_grpc && python3 demo_transcript.py)
    echo "✓ S3 Q1 completed (transcript written to collab_docs_grpc/demo_transcript.txt)."
else
    echo "WARNING: python3 'grpc' module not found on cluster node."
    echo "To install on cluster, run: ./run_cluster.sh setup-env"
fi
echo ""

# -------------------------------------------------------------
# 5. Summary
# -------------------------------------------------------------
echo "========================================="
echo "Cluster CSV Generation Complete!"
echo "Date: $(date)"
echo "CSV files generated:"
ls -lh matmul_mapreduce/results/*.csv 2>/dev/null || true
ls -lh weather_mapreduce/results/*.csv 2>/dev/null || true
ls -lh weather_grpc_streaming/results/*.csv 2>/dev/null || true
echo "========================================="
