package io.nexusdb.management.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class MetricsService {

    public record MetricPoint(
            long timestamp,
            double throughputOpsPerSec,
            double avgLatencyMs,
            double p95LatencyMs,
            double p99LatencyMs,
            double cacheHitRatio,
            long memoryUsedBytes,
            int activeTransactions,
            long replicationLag
    ) {}

    private final int maxCapacity = 300; // ~10 minutes at 2-sec intervals
    private final Deque<MetricPoint> ringBuffer = new ArrayDeque<>();
    private final AtomicReference<MetricPoint> latestPoint = new AtomicReference<>(
            new MetricPoint(System.currentTimeMillis(), 0, 0, 0, 0, 1.0, 50 * 1024 * 1024L, 0, 0)
    );
    private final AtomicLong totalQueries = new AtomicLong(0);

    public MetricsService(MeterRegistry registry) {
        Gauge.builder("nexus_cluster_cache_hit_ratio", this, s -> s.latestPoint.get().cacheHitRatio())
                .description("NexusDB cache hit ratio")
                .register(registry);

        Gauge.builder("nexus_cluster_active_transactions", this, s -> s.latestPoint.get().activeTransactions())
                .description("NexusDB active transactions")
                .register(registry);

        Gauge.builder("nexus_cluster_throughput_ops", this, s -> s.latestPoint.get().throughputOpsPerSec())
                .description("NexusDB throughput in ops/sec")
                .register(registry);
    }

    public synchronized void recordSample(MetricPoint point) {
        latestPoint.set(point);
        if (ringBuffer.size() >= maxCapacity) {
            ringBuffer.pollFirst();
        }
        ringBuffer.addLast(point);
    }

    public synchronized List<MetricPoint> getTimeSeries() {
        return new ArrayList<>(ringBuffer);
    }

    public MetricPoint getLatest() {
        return latestPoint.get();
    }

    public void incrementQueryCount() {
        totalQueries.incrementAndGet();
    }
}
