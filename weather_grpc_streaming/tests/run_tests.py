#!/usr/bin/env python3
"""
End-to-end correctness tests for the streaming weather system (Section 2 Q2).

For each (workers, batch size) configuration the test:
  1. starts a coordinator and W worker processes,
  2. replays a generated dataset through the streaming client,
  3. compares the final analytics byte-for-byte with the baseline
     sequential program (the same oracle used for the MPI and MapReduce
     tests),
  4. replays again and issues dashboard queries WHILE the stream is
     running (they must succeed and observe partial progress).

Also checks Reset clears all state. Ports are offset per configuration to
avoid TIME_WAIT collisions.
"""

import os
import signal
import subprocess
import sys
import time

import grpc

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)                 # weather_grpc_streaming/
REPO = os.path.dirname(ROOT)                 # repo root
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)

import weather_pb2                            # noqa: E402
import weather_pb2_grpc                       # noqa: E402

ORACLE = os.path.join(REPO, "weather_baselines", "build", "q8_sequential")
GENERATOR = os.path.join(REPO, "weather_baselines", "tools", "gen_dataset.py")
TMP = "/tmp/opencode/q8grpc_tests"
os.makedirs(TMP, exist_ok=True)

failures = []


def wait_port_free(port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with grpc.insecure_channel("127.0.0.1:%d" % port) as channel:
                stub = weather_pb2_grpc.WeatherCoordinatorStub(channel)
                try:
                    stub.GetAnalytics(weather_pb2.AnalyticsRequest(), timeout=0.5)
                    return True
                except grpc.RpcError:
                    pass
        except Exception:
            pass
        time.sleep(0.2)
    return False


def start_system(port, workers, base):
    """Start coordinator + workers. Retries the next port if the socket is
    wedged (e.g. an orphaned connection from a killed previous run)."""
    for attempt in range(5):
        procs = []
        coord_addr = "127.0.0.1:%d" % port
        coordinator = subprocess.Popen(
            [sys.executable, os.path.join(SRC, "server.py"), "0.0.0.0:%d" % port],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL)
        if wait_port_free(port):
            break
        coordinator.kill()
        coordinator.wait()
        port += 10
    else:
        raise AssertionError("could not start coordinator near port %d" % port)
    base = port
    for index in range(workers):
        wport = base + 1 + index
        procs.append(subprocess.Popen(
            [sys.executable, os.path.join(SRC, "worker.py"),
             "0.0.0.0:%d" % wport, coord_addr,
             "--advertise", "127.0.0.1:%d" % wport],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL))
    # wait until all workers are registered (channel stays open for callers)
    channel = grpc.insecure_channel(coord_addr)
    stub = weather_pb2_grpc.WeatherCoordinatorStub(channel)
    deadline = time.time() + 15
    while time.time() < deadline:
        report = stub.GetAnalytics(weather_pb2.AnalyticsRequest())
        if len(report.workers) >= workers:
            return procs, coord_addr, stub, channel
        time.sleep(0.2)
    channel.close()
    raise AssertionError("only %d/%d workers registered"
                         % (len(report.workers), workers))


def stop_system(procs):
    for process in procs:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
    for process in procs:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


def run_config(name, dataset, expected_path, workers, batch_size, base):
    port = base
    procs, coord_addr, stub, channel = start_system(port, workers, base)
    try:
        out_path = os.path.join(TMP, "out_%s.txt" % name)
        result = subprocess.run(
            [sys.executable, os.path.join(SRC, "client.py"),
             "--dataset", dataset, "--coordinator", coord_addr,
             "--batch-size", str(batch_size), "--out", out_path,
             "--quiet"],
            capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            failures.append(name)
            print("%s: RUN FAIL\n%s" % (name, result.stderr[-800:]))
            return
        with open(out_path) as stream:
            got = stream.read()
        with open(expected_path) as stream:
            expected = stream.read()
        if got == expected:
            print("%s: PASS" % name)
        else:
            failures.append(name)
            print("%s: FAIL" % name)
            expected_lines = expected.splitlines()
            got_lines = got.splitlines()
            for index in range(max(len(expected_lines), len(got_lines))):
                e = expected_lines[index] if index < len(expected_lines) else "<missing>"
                g = got_lines[index] if index < len(got_lines) else "<missing>"
                if e != g:
                    print("  line %d: expected %r got %r" % (index + 1, e, g))
    finally:
        stop_system(procs)
        channel.close()


def run_midstream_query_test(dataset, base):
    """Queries must succeed and show progress while ingestion is running."""
    port = base
    procs, coord_addr, stub, channel = start_system(port, 2, base)
    try:
        client = subprocess.Popen(
            [sys.executable, os.path.join(SRC, "client.py"),
             "--dataset", dataset, "--coordinator", coord_addr,
             "--batch-size", "500", "--rate", "40000", "--quiet"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        observations = []
        deadline = time.time() + 60
        while client.poll() is None and time.time() < deadline:
            report = stub.GetAnalytics(weather_pb2.AnalyticsRequest(), timeout=5)
            observations.append((report.records_received,
                                 report.ingestion_active))
            time.sleep(0.05)
        client.wait(timeout=30)
        midstream = [obs for obs in observations if obs[1]]
        if any(count > 0 for count, _ in midstream) and client.returncode == 0:
            print("midstream_queries: PASS (%d queries, max %d records seen)"
                  % (len(observations),
                     max(count for count, _ in observations)))
        else:
            failures.append("midstream_queries")
            print("midstream_queries: FAIL (observations: %s... rc=%s)"
                  % (observations[:5], client.returncode))
    finally:
        stop_system(procs)
        channel.close()


def run_reset_test(dataset, base):
    port = base
    procs, coord_addr, stub, channel = start_system(port, 1, base)
    try:
        subprocess.run(
            [sys.executable, os.path.join(SRC, "client.py"),
             "--dataset", dataset, "--coordinator", coord_addr,
             "--batch-size", "1000", "--quiet"],
            capture_output=True, timeout=60)
        stub.Reset(weather_pb2.ResetRequest(), timeout=10)
        report = stub.GetAnalytics(weather_pb2.AnalyticsRequest(), timeout=5)
        if report.records_received == 0 and report.aggregate.count == 0:
            print("reset: PASS")
        else:
            failures.append("reset")
            print("reset: FAIL (records_received=%d)" % report.records_received)
    finally:
        stop_system(procs)
        channel.close()


def main():
    # ---- fixtures ----
    dataset = os.path.join(TMP, "in_20k.txt")
    if not os.path.exists(dataset):
        subprocess.run([sys.executable, GENERATOR, "--n", "20000",
                        "--k", "7", "--s", "40", "--seed", "11",
                        "--out", dataset], check=True)
    expected_path = os.path.join(TMP, "expected_20k.txt")
    subprocess.run([ORACLE, dataset], stdout=open(expected_path, "w"),
                   check=True)

    run_config("grpc_W1_B1000", dataset, expected_path, 1, 1000, 50160)
    run_config("grpc_W2_B500", dataset, expected_path, 2, 500, 50170)
    run_config("grpc_W4_B997", dataset, expected_path, 4, 997, 50180)
    run_config("grpc_W4_B100000", dataset, expected_path, 4, 100000, 50190)

    run_midstream_query_test(dataset, 50300)
    run_reset_test(dataset, 50350)

    print("----------------------------------------")
    if failures:
        print("%d FAILED: %s" % (len(failures), ", ".join(failures)))
        return 1
    print("all streaming tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
