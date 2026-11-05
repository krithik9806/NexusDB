# Phase 3 Benchmark: B+Tree IndexScan vs Sequential Scan

- **Rows**: `1000000`
- **Seed**: `42`
- **Heap Pages (4 KB)**: `10639`

| Access Method | Cache State | Latency (ms) | Latency (us) | Relative Speedup vs SeqScan |
| :--- | :--- | ---: | ---: | ---: |
| `SeqScanExecutor` | Cold Cache | 298.7868 ms | 298786.80 us | 1.00x |
| `SeqScanExecutor` | Warm Cache | 226.8810 ms | 226881.04 us | 1.00x |
| `IndexScanExecutor` (`BPlusTree`) | Cold Cache | 3.2006 ms | 3200.60 us | 93.35x |
| `IndexScanExecutor` (`BPlusTree`) | Warm Cache | 0.2065 ms | 206.54 us | 1098.47x |
