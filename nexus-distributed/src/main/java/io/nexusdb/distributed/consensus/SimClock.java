package io.nexusdb.distributed.consensus;

/**
 * Deterministic logical clock for single-threaded Raft simulation.
 */
public final class SimClock implements RaftClock {
    private long currentMillis;

    public SimClock() {
        this(0L);
    }

    public SimClock(long initialMillis) {
        if (initialMillis < 0) {
            throw new IllegalArgumentException("initialMillis must be >= 0");
        }
        this.currentMillis = initialMillis;
    }

    @Override
    public long nowMillis() {
        return currentMillis;
    }

    public void advanceMillis(long deltaMillis) {
        if (deltaMillis < 0) {
            throw new IllegalArgumentException("deltaMillis must be >= 0");
        }
        this.currentMillis += deltaMillis;
    }

    public void setMillis(long millis) {
        if (millis < this.currentMillis) {
            throw new IllegalArgumentException("SimClock is monotonic: cannot move backwards from "
                    + this.currentMillis + " to " + millis);
        }
        this.currentMillis = millis;
    }
}
