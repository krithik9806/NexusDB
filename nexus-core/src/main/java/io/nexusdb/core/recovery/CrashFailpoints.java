package io.nexusdb.core.recovery;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Deterministic crash failpoint hooks for testing ARIES-lite crash recovery under mid-operation
 * process termination.
 *
 * <p>Only active when configured explicitly via system properties or programmatic test setup;
 * zero overhead in normal execution when {@code activeFailpoint == NONE}.
 */
public final class CrashFailpoints {

    public enum Point {
        NONE,
        /** Halt right after appending COMMIT to in-memory log buffer, BEFORE flushing WAL to disk. */
        BEFORE_COMMIT_WAL_FLUSH,
        /** Halt right after flushing COMMIT to WAL on disk, BEFORE any dirty data pages are flushed. */
        AFTER_COMMIT_WAL_FLUSH,
        /** Halt mid-way through writing a dirty page to DiskManager (after WAL flush). */
        MID_PAGE_WRITE,
        /** Halt mid-way through the ARIES Undo pass after writing at least one CLR record. */
        DURING_UNDO_RECOVERY
    }

    private static final AtomicReference<Point> ACTIVE_POINT = new AtomicReference<>(Point.NONE);
    private static final AtomicInteger TRIGGER_AFTER_COUNT = new AtomicInteger(1);
    private static final AtomicInteger HIT_COUNT = new AtomicInteger(0);

    static {
        String prop = System.getProperty("nexusdb.failpoint");
        if (prop != null && !prop.isBlank()) {
            try {
                ACTIVE_POINT.set(Point.valueOf(prop.trim()));
            } catch (IllegalArgumentException ignored) {
                ACTIVE_POINT.set(Point.NONE);
            }
        }
        String countProp = System.getProperty("nexusdb.failpoint.after");
        if (countProp != null && !countProp.isBlank()) {
            try {
                TRIGGER_AFTER_COUNT.set(Math.max(1, Integer.parseInt(countProp.trim())));
            } catch (NumberFormatException ignored) {
                TRIGGER_AFTER_COUNT.set(1);
            }
        }
    }

    private CrashFailpoints() {
    }

    public static void configure(Point point, int triggerAfterHits) {
        HIT_COUNT.set(0);
        TRIGGER_AFTER_COUNT.set(Math.max(1, triggerAfterHits));
        ACTIVE_POINT.set(point == null ? Point.NONE : point);
    }

    public static void clear() {
        ACTIVE_POINT.set(Point.NONE);
        HIT_COUNT.set(0);
        TRIGGER_AFTER_COUNT.set(1);
    }

    public static boolean isArmed(Point point) {
        return ACTIVE_POINT.get() == point;
    }

    /**
     * Checks whether {@code point} is armed and has reached its trigger threshold; if so,
     * runs {@code beforeHaltAction} (if non-null) and immediately halts the JVM with exit code 99
     * (simulating an abrupt OS/power crash without running shutdown hooks).
     */
    public static void hit(Point point, Runnable beforeHaltAction) {
        if (ACTIVE_POINT.get() != point) {
            return;
        }
        int hits = HIT_COUNT.incrementAndGet();
        if (hits >= TRIGGER_AFTER_COUNT.get()) {
            if (beforeHaltAction != null) {
                beforeHaltAction.run();
            }
            Runtime.getRuntime().halt(99);
        }
    }

    public static void hit(Point point) {
        hit(point, null);
    }
}
