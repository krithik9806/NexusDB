package io.nexusdb.distributed.consensus;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Deterministic simulated network transport supporting link-level partitions, node isolation,
 * configurable message drop, delay, duplication, and reordering.
 */
public final class SimTransport implements RaftTransport {

    public record Envelope(
            long sequenceNumber,
            long deliverAtMillis,
            String fromNodeId,
            String toNodeId,
            RaftMessage message) {
    }

    private record DirectedLink(String from, String to) {
    }

    private final RaftClock clock;
    private final Random random;
    private final PriorityQueue<Envelope> queue = new PriorityQueue<>(
            Comparator.comparingLong(Envelope::deliverAtMillis)
                    .thenComparingLong(Envelope::sequenceNumber));

    private final Set<DirectedLink> blockedLinks = new HashSet<>();
    private final Set<String> crashedNodes = new HashSet<>();

    private long nextSeq = 1L;
    private long baseDelayMillis = 1L;
    private long maxJitterMillis = 0L;
    private double dropProbability = 0.0;
    private double duplicateProbability = 0.0;
    private double reorderProbability = 0.0;
    private long reorderMaxExtraDelayMillis = 10L;

    public SimTransport(RaftClock clock, long seed) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.random = new Random(seed);
    }

    public void setNetworkBehavior(
            long baseDelayMillis,
            long maxJitterMillis,
            double dropProbability,
            double duplicateProbability,
            double reorderProbability) {
        if (baseDelayMillis < 0 || maxJitterMillis < 0) {
            throw new IllegalArgumentException("Delays must be >= 0");
        }
        this.baseDelayMillis = baseDelayMillis;
        this.maxJitterMillis = maxJitterMillis;
        this.dropProbability = clampProb(dropProbability);
        this.duplicateProbability = clampProb(duplicateProbability);
        this.reorderProbability = clampProb(reorderProbability);
    }

    private static double clampProb(double p) {
        return Math.max(0.0, Math.min(1.0, p));
    }

    public void blockLink(String fromNodeId, String toNodeId) {
        blockedLinks.add(new DirectedLink(fromNodeId, toNodeId));
    }

    public void unblockLink(String fromNodeId, String toNodeId) {
        blockedLinks.remove(new DirectedLink(fromNodeId, toNodeId));
    }

    public void disconnectBidirectional(String nodeA, String nodeB) {
        blockLink(nodeA, nodeB);
        blockLink(nodeB, nodeA);
    }

    public void healBidirectional(String nodeA, String nodeB) {
        unblockLink(nodeA, nodeB);
        unblockLink(nodeB, nodeA);
    }

    /**
     * Isolates {@code groupA} from {@code groupB} bidirectionally while leaving intra-group links intact.
     */
    public void partition(Set<String> groupA, Set<String> groupB) {
        for (String a : groupA) {
            for (String b : groupB) {
                if (!a.equals(b)) {
                    disconnectBidirectional(a, b);
                }
            }
        }
    }

    public void isolateNode(String nodeId, List<String> allNodes) {
        for (String other : allNodes) {
            if (!other.equals(nodeId)) {
                disconnectBidirectional(nodeId, other);
            }
        }
    }

    public void healAllPartitions() {
        blockedLinks.clear();
    }

    public void markNodeCrashed(String nodeId) {
        crashedNodes.add(nodeId);
    }

    public void markNodeRecovered(String nodeId) {
        crashedNodes.remove(nodeId);
    }

    public boolean isNodeCrashed(String nodeId) {
        return crashedNodes.contains(nodeId);
    }

    public boolean isLinkBlocked(String fromNodeId, String toNodeId) {
        return blockedLinks.contains(new DirectedLink(fromNodeId, toNodeId));
    }

    @Override
    public void send(String fromNodeId, String toNodeId, RaftMessage message) {
        Objects.requireNonNull(fromNodeId, "fromNodeId must not be null");
        Objects.requireNonNull(toNodeId, "toNodeId must not be null");
        Objects.requireNonNull(message, "message must not be null");

        if (crashedNodes.contains(fromNodeId) || crashedNodes.contains(toNodeId)) {
            return;
        }
        if (isLinkBlocked(fromNodeId, toNodeId)) {
            return;
        }
        if (dropProbability > 0.0 && random.nextDouble() < dropProbability) {
            return;
        }

        long delay = computeDelayMillis();
        enqueue(fromNodeId, toNodeId, message, clock.nowMillis() + delay);

        if (duplicateProbability > 0.0 && random.nextDouble() < duplicateProbability) {
            long dupDelay = delay + 1L + (maxJitterMillis > 0 ? random.nextLong(maxJitterMillis + 1) : 0L);
            enqueue(fromNodeId, toNodeId, message, clock.nowMillis() + dupDelay);
        }
    }

    private long computeDelayMillis() {
        long delay = baseDelayMillis;
        if (maxJitterMillis > 0) {
            delay += random.nextLong(maxJitterMillis + 1);
        }
        if (reorderProbability > 0.0 && random.nextDouble() < reorderProbability) {
            delay += 1L + random.nextLong(Math.max(1L, reorderMaxExtraDelayMillis));
        }
        return delay;
    }

    private void enqueue(String fromNodeId, String toNodeId, RaftMessage message, long deliverAtMillis) {
        queue.add(new Envelope(nextSeq++, deliverAtMillis, fromNodeId, toNodeId, message));
    }

    /**
     * Delivers all queued messages whose {@code deliverAtMillis <= clock.nowMillis()} and whose
     * link/target is not currently partitioned or crashed.
     */
    public int deliverDueMessages(BiConsumer<Envelope, RaftMessage> handler) {
        long now = clock.nowMillis();
        int delivered = 0;
        while (!queue.isEmpty() && queue.peek().deliverAtMillis() <= now) {
            Envelope env = queue.poll();
            if (crashedNodes.contains(env.toNodeId()) || isLinkBlocked(env.fromNodeId(), env.toNodeId())) {
                continue;
            }
            delivered++;
            handler.accept(env, env.message());
        }
        return delivered;
    }

    public int pendingMessageCount() {
        return queue.size();
    }

    public List<Envelope> snapshotPendingMessages() {
        return new ArrayList<>(queue);
    }

    public void clearQueue() {
        queue.clear();
    }
}
