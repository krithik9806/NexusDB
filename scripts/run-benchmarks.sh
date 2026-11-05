#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo "             NexusDB Benchmark Suite Runner               "
echo "=========================================================="

echo "Running Index vs Table Scan Benchmark..."
mvn test -Dtest=IndexVsSeqScanBenchmarkTest

echo "Running Concurrency and Thread Scaling Benchmark..."
mvn test -Dtest=ClusterBenchmarkRunnerTest

echo "Benchmarks Completed Successfully!"
