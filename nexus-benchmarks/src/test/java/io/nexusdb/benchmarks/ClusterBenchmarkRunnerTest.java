package io.nexusdb.benchmarks;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterBenchmarkRunnerTest {

    @Test
    @DisplayName("Verify concurrency benchmark runner execution")
    void testConcurrencyRunner() throws Exception {
        ClusterBenchmarkRunner.ConcurrencyResult res = ClusterBenchmarkRunner.runConcurrencyBenchmark(2, 50);
        assertNotNull(res);
        assertTrue(res.totalOps() == 100);
        assertTrue(res.throughputOpsSec() > 0);
    }
}
