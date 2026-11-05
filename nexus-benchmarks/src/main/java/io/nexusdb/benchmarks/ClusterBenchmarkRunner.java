package io.nexusdb.benchmarks;

import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * End-to-end benchmark measuring single-node vs replicated workloads and concurrent client threads.
 */
public final class ClusterBenchmarkRunner {

    public record ConcurrencyResult(int threads, int totalOps, double totalTimeMs, double throughputOpsSec, double avgLatencyMs) {}

    public static ConcurrencyResult runConcurrencyBenchmark(int threads, int opsPerThread) throws Exception {
        Path tempDir = Files.createTempDirectory("nexus_bench_concurrency");
        try (Database db = Database.open(tempDir.resolve("bench.db"))) {
            db.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);");
            db.execute("INSERT INTO accounts VALUES (1, 1000), (2, 2000), (3, 3000);");

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                tasks.add(() -> {
                    for (int i = 0; i < opsPerThread; i++) {
                        db.execute("SELECT balance FROM accounts WHERE id = 1;");
                    }
                    return null;
                });
            }

            long start = System.nanoTime();
            List<Future<Void>> futures = pool.invokeAll(tasks);
            for (Future<Void> f : futures) {
                f.get();
            }
            long elapsedNanos = System.nanoTime() - start;
            pool.shutdown();

            double totalTimeMs = elapsedNanos / 1_000_000.0;
            int totalOps = threads * opsPerThread;
            double throughput = (totalOps / (totalTimeMs / 1000.0));
            double avgLatency = totalTimeMs / totalOps;

            return new ConcurrencyResult(threads, totalOps, totalTimeMs, throughput, avgLatency);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Running NexusDB End-to-End Concurrency Benchmark...");
        for (int threads : new int[]{1, 10, 50, 100}) {
            ConcurrencyResult res = runConcurrencyBenchmark(threads, 200);
            System.out.printf(Locale.US, "Threads: %3d | Ops: %5d | Time: %7.2f ms | Throughput: %8.1f ops/s | Avg Latency: %6.3f ms%n",
                    res.threads(), res.totalOps(), res.totalTimeMs(), res.throughputOpsSec(), res.avgLatencyMs());
        }
    }
}
