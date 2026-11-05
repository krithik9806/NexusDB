package io.nexusdb.server.admin;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transport-layer fault injector simulating network partitions, message drops, and latency.
 */
public final class FaultInjector {
    private final Set<String> droppedPeers = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> latenciesMs = new ConcurrentHashMap<>();

    public void setDropTraffic(String peerId, boolean drop) {
        if (drop) {
            droppedPeers.add(peerId);
        } else {
            droppedPeers.remove(peerId);
        }
    }

    public boolean isDropping(String peerId) {
        return droppedPeers.contains(peerId);
    }

    public void setLatency(String peerId, long latencyMs) {
        if (latencyMs <= 0) {
            latenciesMs.remove(peerId);
        } else {
            latenciesMs.put(peerId, latencyMs);
        }
    }

    public long getLatency(String peerId) {
        return latenciesMs.getOrDefault(peerId, 0L);
    }

    public void clearAll() {
        droppedPeers.clear();
        latenciesMs.clear();
    }
}
