package io.nexusdb.core.page;

public record RecordId(int pageId, int slotId) implements Comparable<RecordId> {
    public RecordId {
        if (pageId < 0) {
            throw new IllegalArgumentException("pageId must be >= 0, got: " + pageId);
        }
        if (slotId < 0) {
            throw new IllegalArgumentException("slotId must be >= 0, got: " + slotId);
        }
    }

    @Override
    public int compareTo(RecordId other) {
        int cmp = Integer.compare(this.pageId, other.pageId);
        return cmp != 0 ? cmp : Integer.compare(this.slotId, other.slotId);
    }
}
