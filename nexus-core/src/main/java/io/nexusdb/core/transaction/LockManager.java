package io.nexusdb.core.transaction;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enforces Strict Two-Phase Locking (Strict 2PL) with {@link LockMode#SHARED} and
 * {@link LockMode#EXCLUSIVE} locks, in-place {@code SHARED -> EXCLUSIVE} lock upgrades,
 * FIFO wait queues, configurable lock timeouts, and background waits-for graph cycle detection.
 *
 * <p>Concurrency & Crash-Safety Invariants:
 * <ul>
 *   <li><b>Strict 2PL</b>: Transactions hold all acquired {@code SHARED} and {@code EXCLUSIVE}
 *       locks until {@link #releaseAll(long)} is invoked during {@code commit} or {@code rollback}.</li>
 *   <li><b>FIFO Fairness & Upgrade Priority</b>: New lock requests cannot jump ahead of waiting
 *       requests in a resource's FIFO queue, preventing writer starvation. Lock upgrade requests
 *       ({@code SHARED -> EXCLUSIVE} by a current {@code SHARED} holder) are inserted ahead of
 *       non-upgrade waiters on the same resource.</li>
 *   <li><b>Deadlock Detection & Youngest-Victim Selection</b>: A background daemon thread
 *       periodically builds the directed waits-for graph ({@code waiter -> blocker}) across all
 *       resources and aborts the youngest transaction ({@code max(txnId)}) in any cycle by waking
 *       it with {@link DeadlockException}.</li>
 * </ul>
 */
public final class LockManager implements AutoCloseable {
    public static final long DEFAULT_LOCK_TIMEOUT_MS = 5_000L;
    public static final long DEFAULT_DEADLOCK_SCAN_INTERVAL_MS = 15L;

    private final Object lockTableMutex = new Object();
    private final Map<LockId, ResourceBucket> lockTable = new HashMap<>();
    private final Map<Long, Set<LockId>> txnHeldLocks = new HashMap<>();

    private volatile long defaultTimeoutMillis;
    private final ScheduledExecutorService deadlockDetectorExecutor;

    private final AtomicLong totalLockWaits = new AtomicLong();
    private final AtomicLong currentWaitingCount = new AtomicLong();
    private final AtomicLong deadlockCount = new AtomicLong();

    private static final class ResourceBucket {
        final Map<Long, LockMode> holders = new LinkedHashMap<>();
        final ArrayDeque<WaitRequest> waitQueue = new ArrayDeque<>();

        boolean isEmpty() {
            return holders.isEmpty() && waitQueue.isEmpty();
        }
    }

    private static final class WaitRequest {
        final long txnId;
        final LockId lockId;
        final LockMode mode;
        final boolean upgrade;
        boolean granted;
        String deadlockMessage;

        WaitRequest(long txnId, LockId lockId, LockMode mode, boolean upgrade) {
            this.txnId = txnId;
            this.lockId = lockId;
            this.mode = mode;
            this.upgrade = upgrade;
        }
    }

    public LockManager() {
        this(DEFAULT_LOCK_TIMEOUT_MS, DEFAULT_DEADLOCK_SCAN_INTERVAL_MS);
    }

    public LockManager(long defaultTimeoutMillis, long deadlockScanIntervalMillis) {
        if (defaultTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("defaultTimeoutMillis must be > 0");
        }
        this.defaultTimeoutMillis = defaultTimeoutMillis;
        if (deadlockScanIntervalMillis > 0L) {
            this.deadlockDetectorExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "nexusdb-deadlock-detector");
                t.setDaemon(true);
                return t;
            });
            this.deadlockDetectorExecutor.scheduleAtFixedRate(
                    this::detectAndResolveDeadlocks,
                    deadlockScanIntervalMillis,
                    deadlockScanIntervalMillis,
                    TimeUnit.MILLISECONDS);
        } else {
            this.deadlockDetectorExecutor = null;
        }
    }

    public long getDefaultTimeoutMillis() {
        return defaultTimeoutMillis;
    }

    public void setDefaultTimeoutMillis(long defaultTimeoutMillis) {
        if (defaultTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("defaultTimeoutMillis must be > 0");
        }
        this.defaultTimeoutMillis = defaultTimeoutMillis;
    }

    public void acquire(Transaction txn, LockId lockId, LockMode mode) {
        acquire(txn, lockId, mode, defaultTimeoutMillis);
    }

    public void acquire(Transaction txn, LockId lockId, LockMode mode, long timeoutMillis) {
        Objects.requireNonNull(txn, "txn must not be null");
        boolean releaseImmediatelyOnDeadlock = txn.getLoggedModificationCount() == 0;
        acquireInternal(txn.getTxnId(), lockId, mode, timeoutMillis, releaseImmediatelyOnDeadlock);
    }

    public void acquire(long txnId, LockId lockId, LockMode mode) {
        acquire(txnId, lockId, mode, defaultTimeoutMillis);
    }

    public void acquire(long txnId, LockId lockId, LockMode mode, long timeoutMillis) {
        acquireInternal(txnId, lockId, mode, timeoutMillis, true);
    }

    private void acquireInternal(
            long txnId,
            LockId lockId,
            LockMode mode,
            long timeoutMillis,
            boolean releaseImmediatelyOnDeadlock) {
        Objects.requireNonNull(lockId, "lockId must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
        if (timeoutMillis <= 0L) {
            throw new IllegalArgumentException("timeoutMillis must be > 0");
        }

        final WaitRequest waitReq;
        synchronized (lockTableMutex) {
            ResourceBucket bucket = lockTable.computeIfAbsent(lockId, ignored -> new ResourceBucket());
            LockMode existingMode = bucket.holders.get(txnId);

            if (existingMode != null) {
                // Re-entrant lock check
                if (existingMode == LockMode.EXCLUSIVE || mode == LockMode.SHARED) {
                    return;
                }
                // Upgrade request: SHARED -> EXCLUSIVE
                if (bucket.holders.size() == 1) {
                    bucket.holders.put(txnId, LockMode.EXCLUSIVE);
                    return;
                }
                waitReq = new WaitRequest(txnId, lockId, LockMode.EXCLUSIVE, true);
                enqueueUpgradeRequestLocked(bucket, waitReq);
            } else {
                // New lock request: grant immediately only if queue is empty and compatible with holders
                if (bucket.waitQueue.isEmpty() && isCompatibleWithHoldersLocked(bucket, txnId, mode)) {
                    bucket.holders.put(txnId, mode);
                    txnHeldLocks.computeIfAbsent(txnId, ignored -> new HashSet<>()).add(lockId);
                    return;
                }
                waitReq = new WaitRequest(txnId, lockId, mode, false);
                bucket.waitQueue.addLast(waitReq);
            }

            totalLockWaits.incrementAndGet();
            currentWaitingCount.incrementAndGet();
            // Run immediate cycle check in case this request closed a cycle
            detectAndResolveDeadlocksLocked();
        }

        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        synchronized (lockTableMutex) {
            try {
                while (true) {
                    if (waitReq.granted) {
                        return;
                    }
                    if (waitReq.deadlockMessage != null) {
                        if (releaseImmediatelyOnDeadlock) {
                            releaseAllLocked(txnId);
                        }
                        throw new DeadlockException(txnId, waitReq.deadlockMessage);
                    }
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0L) {
                        ResourceBucket bucket = lockTable.get(lockId);
                        if (bucket != null) {
                            bucket.waitQueue.remove(waitReq);
                            grantQueuedRequestsLocked(lockId, bucket);
                            if (bucket.isEmpty()) {
                                lockTable.remove(lockId);
                            }
                        }
                        throw new LockTimeoutException(txnId, lockId, mode, timeoutMillis);
                    }
                    long waitMillis = remainingNanos / 1_000_000L;
                    int waitNanos = (int) (remainingNanos % 1_000_000L);
                    try {
                        lockTableMutex.wait(waitMillis, waitNanos);
                    } catch (InterruptedException ie) {
                        ResourceBucket bucket = lockTable.get(lockId);
                        if (bucket != null) {
                            bucket.waitQueue.remove(waitReq);
                            grantQueuedRequestsLocked(lockId, bucket);
                            if (bucket.isEmpty()) {
                                lockTable.remove(lockId);
                            }
                        }
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while waiting for lock on " + lockId, ie);
                    }
                }
            } finally {
                currentWaitingCount.decrementAndGet();
            }
        }
    }

    private static void enqueueUpgradeRequestLocked(ResourceBucket bucket, WaitRequest upgradeReq) {
        if (bucket.waitQueue.isEmpty() || !bucket.waitQueue.peekFirst().upgrade) {
            bucket.waitQueue.addFirst(upgradeReq);
            return;
        }
        List<WaitRequest> list = new ArrayList<>(bucket.waitQueue);
        int insertIdx = 0;
        while (insertIdx < list.size() && list.get(insertIdx).upgrade) {
            insertIdx++;
        }
        list.add(insertIdx, upgradeReq);
        bucket.waitQueue.clear();
        bucket.waitQueue.addAll(list);
    }

    private static boolean isCompatibleWithHoldersLocked(ResourceBucket bucket, long requesterTxnId, LockMode mode) {
        for (Map.Entry<Long, LockMode> entry : bucket.holders.entrySet()) {
            if (entry.getKey() == requesterTxnId) {
                continue;
            }
            if (!mode.isCompatibleWith(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Releases a single lock held by {@code txnId} on {@code lockId} ( primarily for unit testing;
     * Strict 2PL transactions call {@link #releaseAll(long)} at commit/rollback).
     */
    public void release(long txnId, LockId lockId) {
        Objects.requireNonNull(lockId, "lockId must not be null");
        synchronized (lockTableMutex) {
            ResourceBucket bucket = lockTable.get(lockId);
            if (bucket == null) {
                return;
            }
            if (bucket.holders.remove(txnId) != null) {
                Set<LockId> held = txnHeldLocks.get(txnId);
                if (held != null) {
                    held.remove(lockId);
                    if (held.isEmpty()) {
                        txnHeldLocks.remove(txnId);
                    }
                }
                grantQueuedRequestsLocked(lockId, bucket);
                if (bucket.isEmpty()) {
                    lockTable.remove(lockId);
                }
                lockTableMutex.notifyAll();
            }
        }
    }

    public void releaseAll(Transaction txn) {
        if (txn != null) {
            releaseAll(txn.getTxnId());
        }
    }

    /**
     * Releases all locks held by {@code txnId} at transaction commit or rollback (Strict 2PL).
     */
    public void releaseAll(long txnId) {
        synchronized (lockTableMutex) {
            releaseAllLocked(txnId);
        }
    }

    private void releaseAllLocked(long txnId) {
        Set<LockId> held = txnHeldLocks.remove(txnId);
        if (held == null || held.isEmpty()) {
            return;
        }
        for (LockId lockId : held) {
            ResourceBucket bucket = lockTable.get(lockId);
            if (bucket != null) {
                bucket.holders.remove(txnId);
                grantQueuedRequestsLocked(lockId, bucket);
                if (bucket.isEmpty()) {
                    lockTable.remove(lockId);
                }
            }
        }
        lockTableMutex.notifyAll();
    }

    private void grantQueuedRequestsLocked(LockId lockId, ResourceBucket bucket) {
        Iterator<WaitRequest> it = bucket.waitQueue.iterator();
        while (it.hasNext()) {
            WaitRequest req = it.next();
            if (req.deadlockMessage != null) {
                it.remove();
                continue;
            }
            if (req.upgrade) {
                if (bucket.holders.size() == 1 && bucket.holders.containsKey(req.txnId)) {
                    it.remove();
                    bucket.holders.put(req.txnId, LockMode.EXCLUSIVE);
                    req.granted = true;
                    lockTableMutex.notifyAll();
                }
                // Whether granted or still waiting, an upgrade requires EXCLUSIVE access, blocking later waiters
                break;
            } else {
                if (isCompatibleWithHoldersLocked(bucket, req.txnId, req.mode)) {
                    it.remove();
                    bucket.holders.put(req.txnId, req.mode);
                    txnHeldLocks.computeIfAbsent(req.txnId, ignored -> new HashSet<>()).add(lockId);
                    req.granted = true;
                    lockTableMutex.notifyAll();
                    if (req.mode == LockMode.EXCLUSIVE) {
                        break;
                    }
                } else {
                    // Enforce strict FIFO fairness: later arrivals cannot bypass a blocked waiter
                    break;
                }
            }
        }
    }

    /**
     * Returns {@code true} if {@code txnId} currently holds {@code mode} (or stronger) on {@code lockId}.
     */
    public boolean holdsLock(long txnId, LockId lockId, LockMode mode) {
        synchronized (lockTableMutex) {
            ResourceBucket bucket = lockTable.get(lockId);
            if (bucket == null) {
                return false;
            }
            LockMode held = bucket.holders.get(txnId);
            if (held == null) {
                return false;
            }
            return held == LockMode.EXCLUSIVE || held == mode;
        }
    }

    /**
     * Returns {@code true} if any transaction other than {@code requesterTxnId} currently holds an
     * {@link LockMode#EXCLUSIVE} lock on {@code lockId}.
     */
    public boolean isExclusivelyLockedByOther(long requesterTxnId, LockId lockId) {
        synchronized (lockTableMutex) {
            ResourceBucket bucket = lockTable.get(lockId);
            if (bucket == null) {
                return false;
            }
            for (Map.Entry<Long, LockMode> entry : bucket.holders.entrySet()) {
                if (entry.getKey() != requesterTxnId && entry.getValue() == LockMode.EXCLUSIVE) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Scans the current waits-for graph for cycles and aborts the youngest transaction in each cycle.
     */
    public void detectAndResolveDeadlocks() {
        synchronized (lockTableMutex) {
            detectAndResolveDeadlocksLocked();
        }
    }

    private void detectAndResolveDeadlocksLocked() {
        while (true) {
            Map<Long, Set<Long>> waitsFor = buildWaitsForGraphLocked();
            if (waitsFor.isEmpty()) {
                return;
            }
            List<Long> cycle = findCycle(waitsFor);
            if (cycle == null || cycle.isEmpty()) {
                return;
            }
            long youngestTxnId = Collections.max(cycle);
            if (!abortVictimWaitRequestLocked(youngestTxnId, cycle)) {
                return;
            }
        }
    }

    private Map<Long, Set<Long>> buildWaitsForGraphLocked() {
        Map<Long, Set<Long>> graph = new TreeMap<>();
        for (ResourceBucket bucket : lockTable.values()) {
            if (bucket.waitQueue.isEmpty()) {
                continue;
            }
            List<WaitRequest> queue = new ArrayList<>(bucket.waitQueue);
            for (int i = 0; i < queue.size(); i++) {
                WaitRequest waiter = queue.get(i);
                if (waiter.granted || waiter.deadlockMessage != null) {
                    continue;
                }
                long waiterTxn = waiter.txnId;
                // 1. Waiter waits for incompatible current holders
                for (Map.Entry<Long, LockMode> holder : bucket.holders.entrySet()) {
                    long holderTxn = holder.getKey();
                    if (holderTxn == waiterTxn) {
                        continue;
                    }
                    if (waiter.upgrade || !waiter.mode.isCompatibleWith(holder.getValue())) {
                        graph.computeIfAbsent(waiterTxn, ignored -> new TreeSet<>()).add(holderTxn);
                    }
                }
                // 2. Waiter also waits for earlier conflicting requests in the FIFO queue
                for (int j = 0; j < i; j++) {
                    WaitRequest earlier = queue.get(j);
                    if (earlier.granted || earlier.deadlockMessage != null || earlier.txnId == waiterTxn) {
                        continue;
                    }
                    if (earlier.upgrade
                            || waiter.upgrade
                            || !waiter.mode.isCompatibleWith(earlier.mode)) {
                        graph.computeIfAbsent(waiterTxn, ignored -> new TreeSet<>()).add(earlier.txnId);
                    }
                }
            }
        }
        return graph;
    }

    private static List<Long> findCycle(Map<Long, Set<Long>> graph) {
        Set<Long> visited = new HashSet<>();
        LinkedHashSet<Long> activeStack = new LinkedHashSet<>();

        for (Long startNode : graph.keySet()) {
            if (!visited.contains(startNode)) {
                List<Long> cycle = dfsCycle(startNode, graph, visited, activeStack);
                if (cycle != null) {
                    return cycle;
                }
            }
        }
        return null;
    }

    private static List<Long> dfsCycle(
            Long current,
            Map<Long, Set<Long>> graph,
            Set<Long> visited,
            LinkedHashSet<Long> activeStack) {
        visited.add(current);
        activeStack.add(current);

        Set<Long> neighbors = graph.get(current);
        if (neighbors != null) {
            for (Long next : neighbors) {
                if (activeStack.contains(next)) {
                    List<Long> cycle = new ArrayList<>();
                    boolean collecting = false;
                    for (Long node : activeStack) {
                        if (node.equals(next)) {
                            collecting = true;
                        }
                        if (collecting) {
                            cycle.add(node);
                        }
                    }
                    return cycle;
                }
                if (!visited.contains(next)) {
                    List<Long> found = dfsCycle(next, graph, visited, activeStack);
                    if (found != null) {
                        return found;
                    }
                }
            }
        }

        activeStack.remove(current);
        return null;
    }

    private boolean abortVictimWaitRequestLocked(long victimTxnId, List<Long> cycle) {
        String msg = "Deadlock detected in waits-for cycle " + cycle
                + "; aborting youngest transaction txnId=" + victimTxnId;
        boolean abortedAny = false;
        for (Map.Entry<LockId, ResourceBucket> entry : lockTable.entrySet()) {
            LockId lockId = entry.getKey();
            ResourceBucket bucket = entry.getValue();
            Iterator<WaitRequest> it = bucket.waitQueue.iterator();
            boolean removedFromBucket = false;
            while (it.hasNext()) {
                WaitRequest req = it.next();
                if (req.txnId == victimTxnId && !req.granted && req.deadlockMessage == null) {
                    req.deadlockMessage = msg;
                    it.remove();
                    removedFromBucket = true;
                    abortedAny = true;
                }
            }
            if (removedFromBucket) {
                grantQueuedRequestsLocked(lockId, bucket);
            }
        }
        if (abortedAny) {
            deadlockCount.incrementAndGet();
            lockTableMutex.notifyAll();
        }
        return abortedAny;
    }

    public long getTotalLockWaits() {
        return totalLockWaits.get();
    }

    public long getCurrentWaitingCount() {
        return currentWaitingCount.get();
    }

    public long getDeadlockCount() {
        return deadlockCount.get();
    }

    @Override
    public void close() {
        if (deadlockDetectorExecutor != null) {
            deadlockDetectorExecutor.shutdownNow();
        }
    }
}
