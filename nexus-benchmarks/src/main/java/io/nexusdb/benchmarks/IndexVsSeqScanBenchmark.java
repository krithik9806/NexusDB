package io.nexusdb.benchmarks;

import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.executor.FilterExecutor;
import io.nexusdb.core.executor.IndexScanExecutor;
import io.nexusdb.core.executor.SeqScanExecutor;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.planner.BoundExpr;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.storage.DiskManager;
import io.nexusdb.core.storage.PageAllocator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;

/**
 * Phase 3 reproducible benchmark comparing full {@link SeqScanExecutor} against
 * {@link BPlusTree}-backed {@link IndexScanExecutor} under cold-cache and warm-cache conditions.
 *
 * <p>All reported latencies and speedups are measured from live execution with {@link System#nanoTime()}.
 */
public final class IndexVsSeqScanBenchmark {

    public static final long DEFAULT_SEED = 42L;
    public static final int DEFAULT_ROWS = 1_000_000;
    public static final int DEFAULT_WARM_LOOKUPS = 100;
    private static final int BUFFER_POOL_PAGES = 16_384; // 64 MB buffer pool (4 KB pages)

    public record BenchmarkResult(
            int rowCount,
            long seed,
            int heapPages,
            double seqScanColdMs,
            double seqScanWarmMs,
            double indexScanColdMs,
            double indexScanWarmMs,
            double coldSpeedup,
            double warmSpeedup
    ) {
        public String toJson() {
            return String.format(Locale.US, """
                    {
                      "benchmark": "IndexVsSeqScanBenchmark",
                      "phase": "v0.3-index",
                      "rowCount": %d,
                      "seed": %d,
                      "heapPages": %d,
                      "metrics": {
                        "seqScanColdMs": %.4f,
                        "seqScanWarmMs": %.4f,
                        "indexScanColdMs": %.4f,
                        "indexScanWarmMs": %.4f,
                        "coldSpeedup": %.2f,
                        "warmSpeedup": %.2f
                      }
                    }
                    """,
                    rowCount,
                    seed,
                    heapPages,
                    seqScanColdMs,
                    seqScanWarmMs,
                    indexScanColdMs,
                    indexScanWarmMs,
                    coldSpeedup,
                    warmSpeedup);
        }

        public String toMarkdown() {
            return String.format(Locale.US, """
                    # Phase 3 Benchmark: B+Tree IndexScan vs Sequential Scan

                    - **Rows**: `%d`
                    - **Seed**: `%d`
                    - **Heap Pages (4 KB)**: `%d`

                    | Access Method | Cache State | Latency (ms) | Latency (us) | Relative Speedup vs SeqScan |
                    | :--- | :--- | ---: | ---: | ---: |
                    | `SeqScanExecutor` | Cold Cache | %.4f ms | %.2f us | 1.00x |
                    | `SeqScanExecutor` | Warm Cache | %.4f ms | %.2f us | 1.00x |
                    | `IndexScanExecutor` (`BPlusTree`) | Cold Cache | %.4f ms | %.2f us | %.2fx |
                    | `IndexScanExecutor` (`BPlusTree`) | Warm Cache | %.4f ms | %.2f us | %.2fx |
                    """,
                    rowCount,
                    seed,
                    heapPages,
                    seqScanColdMs, seqScanColdMs * 1000.0,
                    seqScanWarmMs, seqScanWarmMs * 1000.0,
                    indexScanColdMs, indexScanColdMs * 1000.0, coldSpeedup,
                    indexScanWarmMs, indexScanWarmMs * 1000.0, warmSpeedup);
        }
    }

    public static BenchmarkResult runBenchmark(
            Path workDir,
            Path outputDir,
            int rowCount,
            int warmLookupCount,
            long seed) throws IOException {
        Objects.requireNonNull(workDir, "workDir cannot be null");
        Objects.requireNonNull(outputDir, "outputDir cannot be null");
        if (rowCount <= 0) {
            throw new IllegalArgumentException("rowCount must be > 0");
        }
        if (warmLookupCount <= 0) {
            throw new IllegalArgumentException("warmLookupCount must be > 0");
        }

        Files.createDirectories(workDir);
        Files.createDirectories(outputDir);
        Path dbFile = workDir.resolve("benchmark_phase3.db");
        Files.deleteIfExists(dbFile);

        Schema schema = new Schema(List.of(
                new Column("id", DataType.INT, 0, false, true),
                new Column("code", DataType.INT, 0, false, false),
                new Column("payload", DataType.VARCHAR, 32, false, false)
        ));

        // Deterministic shuffled key insertion order using the fixed seed
        List<Integer> keys = new ArrayList<>(rowCount);
        for (int i = 1; i <= rowCount; i++) {
            keys.add(i);
        }
        Collections.shuffle(keys, new Random(seed));

        int firstHeapPageId;
        int lastHeapPageId;
        int indexRootPageId;
        int heapPages;

        // 1. Populate TableHeap and BPlusTree
        try (DiskManager dm = DiskManager.open(dbFile)) {
            PageAllocator allocator = new PageAllocator(dm);
            BufferPoolManager bpm = new BufferPoolManager(BUFFER_POOL_PAGES, dm, allocator);
            TableHeap heap = TableHeap.create(bpm, schema);
            BPlusTree index = BPlusTree.create(bpm, DataType.INT, true);

            Value payloadVal = Value.ofVarchar("nexus-bench-row-payload-data");
            for (int i = 0; i < rowCount; i++) {
                int k = keys.get(i);
                Value keyVal = Value.ofInt(k);
                Tuple tuple = new Tuple(List.of(keyVal, keyVal, payloadVal));
                RecordId rid = heap.insert(tuple);
                index.insert(keyVal, rid);
            }

            firstHeapPageId = heap.getFirstPageId();
            lastHeapPageId = heap.getLastPageId();
            indexRootPageId = index.getRootPageId();
            heapPages = heap.getPageCount();
            bpm.flushAll();
        }

        Random queryRng = new Random(seed ^ 0x5DEECE66DL);
        int coldTargetKey = keys.get(queryRng.nextInt(rowCount));

        // 2. Measure Cold-Cache SeqScan (brand new DiskManager & empty BufferPoolManager)
        double seqScanColdMs;
        double seqScanWarmMs;
        try (DiskManager dm = DiskManager.open(dbFile)) {
            PageAllocator allocator = new PageAllocator(dm);
            BufferPoolManager bpm = new BufferPoolManager(BUFFER_POOL_PAGES, dm, allocator);
            TableHeap heap = TableHeap.openWithoutScan(
                    bpm, schema, firstHeapPageId, lastHeapPageId, rowCount, heapPages);

            long coldStart = System.nanoTime();
            int matches = executePointSeqScan(heap, coldTargetKey);
            long coldElapsed = System.nanoTime() - coldStart;
            if (matches != 1) {
                throw new IllegalStateException("Expected 1 match in cold SeqScan but found " + matches);
            }
            seqScanColdMs = coldElapsed / 1_000_000.0;

            // 3. Measure Warm-Cache SeqScan on the same TableHeap
            int warmSeqRuns = Math.min(5, warmLookupCount);
            long warmSeqTotalNanos = 0L;
            for (int i = 0; i < warmSeqRuns; i++) {
                int target = keys.get(queryRng.nextInt(rowCount));
                long start = System.nanoTime();
                int found = executePointSeqScan(heap, target);
                warmSeqTotalNanos += (System.nanoTime() - start);
                if (found != 1) {
                    throw new IllegalStateException("Expected 1 match in warm SeqScan but found " + found);
                }
            }
            seqScanWarmMs = (warmSeqTotalNanos / (double) warmSeqRuns) / 1_000_000.0;
        }

        // 4. Measure Cold-Cache IndexScan (brand new DiskManager & empty BufferPoolManager)
        double indexScanColdMs;
        double indexScanWarmMs;
        try (DiskManager dm = DiskManager.open(dbFile)) {
            PageAllocator allocator = new PageAllocator(dm);
            BufferPoolManager bpm = new BufferPoolManager(BUFFER_POOL_PAGES, dm, allocator);
            TableHeap heap = TableHeap.openWithoutScan(
                    bpm, schema, firstHeapPageId, lastHeapPageId, rowCount, heapPages);
            BPlusTree index = BPlusTree.open(bpm, indexRootPageId, DataType.INT, true);
            IndexMetadata idxMeta = new IndexMetadata("idx_bench_id", "bench", "id", indexRootPageId, true);
            TableMetadata tableMeta = new TableMetadata(
                    "bench",
                    schema,
                    firstHeapPageId,
                    heap,
                    List.of(idxMeta),
                    new RecordId(1, 0));

            long coldStart = System.nanoTime();
            int matches = executePointIndexScan(tableMeta, idxMeta, index, coldTargetKey);
            long coldElapsed = System.nanoTime() - coldStart;
            if (matches != 1) {
                throw new IllegalStateException("Expected 1 match in cold IndexScan but found " + matches);
            }
            indexScanColdMs = coldElapsed / 1_000_000.0;

            // Warm up buffer pool with random index lookups
            for (int i = 0; i < warmLookupCount; i++) {
                int target = keys.get(queryRng.nextInt(rowCount));
                executePointIndexScan(tableMeta, idxMeta, index, target);
            }

            // 5. Measure Warm-Cache IndexScan
            long warmIdxTotalNanos = 0L;
            for (int i = 0; i < warmLookupCount; i++) {
                int target = keys.get(queryRng.nextInt(rowCount));
                long start = System.nanoTime();
                int found = executePointIndexScan(tableMeta, idxMeta, index, target);
                warmIdxTotalNanos += (System.nanoTime() - start);
                if (found != 1) {
                    throw new IllegalStateException("Expected 1 match in warm IndexScan but found " + found);
                }
            }
            indexScanWarmMs = (warmIdxTotalNanos / (double) warmLookupCount) / 1_000_000.0;
        }

        double coldSpeedup = seqScanColdMs / Math.max(indexScanColdMs, 0.0001);
        double warmSpeedup = seqScanWarmMs / Math.max(indexScanWarmMs, 0.0001);

        BenchmarkResult result = new BenchmarkResult(
                rowCount,
                seed,
                heapPages,
                seqScanColdMs,
                seqScanWarmMs,
                indexScanColdMs,
                indexScanWarmMs,
                coldSpeedup,
                warmSpeedup
        );

        Path jsonFile = outputDir.resolve("benchmark-phase3.json");
        Path mdFile = outputDir.resolve("benchmark-phase3.md");
        Files.writeString(jsonFile, result.toJson(), StandardCharsets.UTF_8);
        Files.writeString(mdFile, result.toMarkdown(), StandardCharsets.UTF_8);

        Files.deleteIfExists(dbFile);
        return result;
    }

    private static int executePointSeqScan(TableHeap heap, int targetKey) {
        SeqScanExecutor seqScan = new SeqScanExecutor("bench", heap);
        BoundExpr predicate = new BoundExpr.BoundBinaryExpr(
                new BoundExpr.BoundColumnRef(0, "bench.id", DataType.INT),
                Expr.BinaryOp.EQ,
                new BoundExpr.BoundLiteral(Value.ofInt(targetKey), DataType.INT),
                DataType.BOOLEAN
        );
        FilterExecutor filter = new FilterExecutor(seqScan, predicate);
        filter.init();
        int count = 0;
        try {
            while (filter.next() != null) {
                count++;
            }
        } finally {
            filter.close();
        }
        return count;
    }

    private static int executePointIndexScan(
            TableMetadata tableMeta,
            IndexMetadata idxMeta,
            BPlusTree index,
            int targetKey) {
        Value key = Value.ofInt(targetKey);
        IndexScanExecutor indexScan = new IndexScanExecutor(
                tableMeta,
                idxMeta,
                index,
                key,
                true,
                key,
                true
        );
        indexScan.init();
        int count = 0;
        try {
            while (indexScan.next() != null) {
                count++;
            }
        } finally {
            indexScan.close();
        }
        return count;
    }

    public static void main(String[] args) throws IOException {
        int rows = DEFAULT_ROWS;
        int warmLookups = DEFAULT_WARM_LOOKUPS;
        long seed = DEFAULT_SEED;
        Path outDir = Path.of(".");

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--rows" -> rows = Integer.parseInt(args[++i]);
                case "--warm-lookups" -> warmLookups = Integer.parseInt(args[++i]);
                case "--seed" -> seed = Long.parseLong(args[++i]);
                case "--out-dir" -> outDir = Path.of(args[++i]);
                default -> {
                    // ignore unknown args
                }
            }
        }

        Path workDir = Files.createTempDirectory("nexusdb-bench-phase3-");
        BenchmarkResult result = runBenchmark(workDir, outDir, rows, warmLookups, seed);
        Files.deleteIfExists(workDir);
        System.out.println(result.toMarkdown());
    }
}
