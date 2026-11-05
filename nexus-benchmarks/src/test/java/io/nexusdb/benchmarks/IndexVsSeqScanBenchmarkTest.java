package io.nexusdb.benchmarks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class IndexVsSeqScanBenchmarkTest {

    @TempDir
    Path tempDir;

    @Test
    void testBenchmarkPipelineRunsAndWritesJsonAndMarkdown() throws IOException {
        Path workDir = tempDir.resolve("work");
        Path outDir = tempDir.resolve("out");

        IndexVsSeqScanBenchmark.BenchmarkResult result = IndexVsSeqScanBenchmark.runBenchmark(
                workDir,
                outDir,
                10_000,
                50,
                IndexVsSeqScanBenchmark.DEFAULT_SEED
        );

        assertEquals(10_000, result.rowCount());
        assertEquals(IndexVsSeqScanBenchmark.DEFAULT_SEED, result.seed());
        assertTrue(result.heapPages() > 10, "10,000 rows must span multiple 4KB heap pages");
        assertTrue(result.seqScanColdMs() > 0.0);
        assertTrue(result.seqScanWarmMs() > 0.0);
        assertTrue(result.indexScanColdMs() > 0.0);
        assertTrue(result.indexScanWarmMs() > 0.0);
        assertTrue(result.warmSpeedup() > 1.0,
                "Warm IndexScan must be faster than full SeqScan: " + result);

        Path jsonFile = outDir.resolve("benchmark-phase3.json");
        Path mdFile = outDir.resolve("benchmark-phase3.md");
        assertTrue(Files.exists(jsonFile), "benchmark-phase3.json must be written");
        assertTrue(Files.exists(mdFile), "benchmark-phase3.md must be written");

        String json = Files.readString(jsonFile);
        assertTrue(json.contains("\"benchmark\": \"IndexVsSeqScanBenchmark\""));
        assertTrue(json.contains("\"rowCount\": 10000"));
        assertTrue(json.contains("\"warmSpeedup\":"));

        String md = Files.readString(mdFile);
        assertTrue(md.contains("# Phase 3 Benchmark: B+Tree IndexScan vs Sequential Scan"));
        assertTrue(md.contains("`IndexScanExecutor` (`BPlusTree`)"));
    }
}
