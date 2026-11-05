package io.nexusdb.core.recovery;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.catalog.Catalog;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.SlottedPage;
import io.nexusdb.core.storage.DiskManager;
import io.nexusdb.core.transaction.TransactionManager;
import io.nexusdb.core.wal.LogManager;
import io.nexusdb.core.wal.LogReader;
import io.nexusdb.core.wal.LogRecord;
import io.nexusdb.core.wal.LogRecordType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Implements 3-pass ARIES-lite crash recovery ({@code Analysis -> Redo -> Undo}).
 *
 * <p>Concurrency & Crash-Safety Invariants:
 * <ul>
 *   <li><b>Pass 1 — Analysis</b>: Scans forward from the last valid checkpoint (recorded in Page 0's
 *       {@code checkpointLsn} header) or from the beginning of the WAL to reconstruct the Dirty Page
 *       Table ({@code pageId -> recLsn}) and the Active Transaction Table ({@code txnId -> lastLsn}
 *       for loser transactions still {@code ACTIVE} at crash time).</li>
 *   <li><b>Pass 2 — Redo (Repeating History)</b>: Scans forward from
 *       {@code minRecLsn = min(checkpointBeginLsn, min(DPT.recLsn))} (or LSN 1 if no checkpoint exists)
 *       and replays every logged page/slot modification (for both winner and loser transactions,
 *       including {@code CLR}s) whenever {@code page.getPageLsn() < record.lsn()}.</li>
 *   <li><b>Pass 3 — Undo</b>: Walks all loser transactions in global reverse LSN order following
 *       {@code prevLsn} and {@code CLR.undoNextLsn}. Each undone tuple modification writes a
 *       {@code CLR} record pointing to {@code record.prevLsn()} and stamps the page with {@code clrLsn}
 *       so that repeated crashes during recovery never undo the same operation twice. Once a loser's
 *       {@code BEGIN} record is reached, an {@code ABORT} record is appended.</li>
 *   <li><b>Index Synchronization</b>: After Redo and Undo restore all catalog and heap pages,
 *       {@link Catalog#reload()} and {@link TableMetadata#rebuildIndexesFromHeap()} synchronize all
 *       primary and secondary B+Tree indexes with the recovered table heaps.</li>
 * </ul>
 */
public final class RecoveryManager {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final BufferPoolManager bufferPoolManager;
    private final LogManager logManager;
    private final Catalog catalog;
    private final TransactionManager transactionManager;

    public record RecoveryReport(
            long checkpointBeginLsn,
            long redoStartLsn,
            Map<Integer, Long> dirtyPageTable,
            Map<Long, Long> loserTransactions,
            int redoneRecordCount,
            int undoneRecordCount) {
        public RecoveryReport {
            dirtyPageTable = Collections.unmodifiableMap(new LinkedHashMap<>(dirtyPageTable));
            loserTransactions = Collections.unmodifiableMap(new LinkedHashMap<>(loserTransactions));
        }
    }

    public RecoveryManager(
            BufferPoolManager bufferPoolManager,
            LogManager logManager,
            Catalog catalog,
            TransactionManager transactionManager) {
        this.bufferPoolManager = Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        this.logManager = Objects.requireNonNull(logManager, "logManager must not be null");
        this.catalog = catalog;
        this.transactionManager = transactionManager;
    }

    /**
     * Executes the 3-pass ARIES-lite crash recovery protocol.
     */
    public RecoveryReport recover() {
        List<LogRecord> records = LogReader.readAll(logManager.getWalPath(), true);
        if (records.isEmpty()) {
            return new RecoveryReport(0L, 0L, Map.of(), Map.of(), 0, 0);
        }

        // Index all WAL records by LSN for Undo chain traversal and track maximum txnId
        Map<Long, LogRecord> recordsByLsn = new HashMap<>(records.size() * 2);
        long maxTxnId = 0L;
        for (LogRecord rec : records) {
            recordsByLsn.put(rec.lsn(), rec);
            if (rec.txnId() > maxTxnId) {
                maxTxnId = rec.txnId();
            }
            if (rec.type() == LogRecordType.CHECKPOINT_END) {
                for (Long txnId : rec.activeTransactionTable().keySet()) {
                    if (txnId > maxTxnId) {
                        maxTxnId = txnId;
                    }
                }
            }
        }

        // ====================================================================
        // PASS 1: ANALYSIS
        // ====================================================================
        DiskManager diskManager = bufferPoolManager.getDiskManager();
        long storedCheckpointLsn = diskManager.getCheckpointLsn();
        int analysisStartIdx = findValidCheckpointStartIndex(records, storedCheckpointLsn);
        long effectiveCheckpointBeginLsn = analysisStartIdx > 0 ? records.get(analysisStartIdx).lsn() : 0L;

        Map<Integer, Long> dpt = new LinkedHashMap<>();
        Map<Long, Long> att = new LinkedHashMap<>();
        Set<Long> terminatedTxns = new HashSet<>();

        for (int i = analysisStartIdx; i < records.size(); i++) {
            LogRecord rec = records.get(i);
            switch (rec.type()) {
                case BEGIN -> {
                    if (rec.txnId() > 0L) {
                        att.put(rec.txnId(), rec.lsn());
                        terminatedTxns.remove(rec.txnId());
                    }
                }
                case COMMIT, ABORT -> {
                    att.remove(rec.txnId());
                    terminatedTxns.add(rec.txnId());
                }
                case INSERT, UPDATE, DELETE, CLR -> {
                    if (rec.txnId() > 0L && !terminatedTxns.contains(rec.txnId())) {
                        att.put(rec.txnId(), rec.lsn());
                    }
                    if (rec.pageId() > 0) {
                        dpt.putIfAbsent(rec.pageId(), rec.lsn());
                    }
                }
                case CHECKPOINT_BEGIN -> {
                    // Marker handled via CHECKPOINT_END
                }
                case CHECKPOINT_END -> {
                    for (Map.Entry<Integer, Long> entry : rec.dirtyPageTable().entrySet()) {
                        dpt.merge(entry.getKey(), entry.getValue(), Math::min);
                    }
                    for (Map.Entry<Long, Long> entry : rec.activeTransactionTable().entrySet()) {
                        long txnId = entry.getKey();
                        if (!terminatedTxns.contains(txnId)) {
                            att.merge(txnId, entry.getValue(), Math::max);
                        }
                    }
                }
            }
        }

        Map<Integer, Long> analysisDptSnapshot = new LinkedHashMap<>(dpt);
        Map<Long, Long> analysisAttSnapshot = new LinkedHashMap<>(att);

        // ====================================================================
        // PASS 2: REDO (Repeating History)
        // ====================================================================
        long redoStartLsn;
        if (effectiveCheckpointBeginLsn > 0L) {
            redoStartLsn = dpt.isEmpty()
                    ? effectiveCheckpointBeginLsn
                    : Math.min(effectiveCheckpointBeginLsn, Collections.min(dpt.values()));
        } else {
            redoStartLsn = records.get(0).lsn();
        }

        int redoneCount = 0;
        for (LogRecord rec : records) {
            if (rec.lsn() < redoStartLsn || rec.pageId() <= 0) {
                continue;
            }
            if (effectiveCheckpointBeginLsn > 0L) {
                Long recLsnInDpt = dpt.get(rec.pageId());
                if (recLsnInDpt == null || rec.lsn() < recLsnInDpt) {
                    continue;
                }
            }
            if (redoRecordIfNeeded(diskManager, rec)) {
                redoneCount++;
            }
        }

        // ====================================================================
        // PASS 3: UNDO (Roll back loser transactions in reverse LSN order)
        // ====================================================================
        Map<Long, Long> nextUndoLsn = new LinkedHashMap<>(att);
        Map<Long, Long> lastWrittenLsn = new LinkedHashMap<>(att);
        int undoneCount = 0;

        LogManager.TransactionLogContext prevThreadCtx = logManager.getThreadTransactionContext();
        logManager.clearThreadTransactionContext();
        try {
            while (!nextUndoLsn.isEmpty()) {
                long targetTxnId = -1L;
                long maxLsn = Long.MIN_VALUE;
                for (Map.Entry<Long, Long> entry : nextUndoLsn.entrySet()) {
                    if (entry.getValue() > maxLsn) {
                        maxLsn = entry.getValue();
                        targetTxnId = entry.getKey();
                    }
                }

                if (maxLsn <= 0L) {
                    long abortLsn = logManager.appendAbort(targetTxnId, lastWrittenLsn.getOrDefault(targetTxnId, 0L));
                    lastWrittenLsn.put(targetTxnId, abortLsn);
                    nextUndoLsn.remove(targetTxnId);
                    continue;
                }

                LogRecord rec = recordsByLsn.get(maxLsn);
                if (rec == null || rec.type() == LogRecordType.BEGIN) {
                    long abortLsn = logManager.appendAbort(targetTxnId, lastWrittenLsn.getOrDefault(targetTxnId, maxLsn));
                    lastWrittenLsn.put(targetTxnId, abortLsn);
                    nextUndoLsn.remove(targetTxnId);
                    continue;
                }

                if (rec.type() == LogRecordType.CLR) {
                    // CLRs are never undone; jump directly to undoNextLsn
                    long undoNext = rec.undoNextLsn();
                    if (undoNext <= 0L) {
                        long abortLsn = logManager.appendAbort(targetTxnId, lastWrittenLsn.get(targetTxnId));
                        lastWrittenLsn.put(targetTxnId, abortLsn);
                        nextUndoLsn.remove(targetTxnId);
                    } else {
                        nextUndoLsn.put(targetTxnId, undoNext);
                    }
                    continue;
                }

                if (undoSingleRecord(diskManager, targetTxnId, lastWrittenLsn, rec)) {
                    undoneCount++;
                    CrashFailpoints.hit(CrashFailpoints.Point.DURING_UNDO_RECOVERY, logManager::flush);
                }
                nextUndoLsn.put(targetTxnId, rec.prevLsn());
            }
        } finally {
            if (prevThreadCtx != null) {
                logManager.setThreadTransactionContext(prevThreadCtx);
            }
        }

        // Advance transaction ID generator past all historical transactions
        if (transactionManager != null && maxTxnId > 0L) {
            transactionManager.ensureNextTxnIdAtLeast(maxTxnId + 1L);
        }

        // Reload catalog and rebuild B+Tree indexes so they strictly match the recovered heaps
        if (catalog != null && diskManager.getCatalogMetaPageId() != -1) {
            catalog.reload();
            for (TableMetadata table : catalog.listTables()) {
                table.rebuildIndexesFromHeap();
            }
        }

        logManager.flush();
        bufferPoolManager.flushAll();

        return new RecoveryReport(
                effectiveCheckpointBeginLsn,
                redoStartLsn,
                analysisDptSnapshot,
                analysisAttSnapshot,
                redoneCount,
                undoneCount);
    }

    private static int findValidCheckpointStartIndex(List<LogRecord> records, long checkpointLsn) {
        if (checkpointLsn <= 0L) {
            return 0;
        }
        int beginIdx = -1;
        for (int i = 0; i < records.size(); i++) {
            LogRecord rec = records.get(i);
            if (rec.lsn() == checkpointLsn && rec.type() == LogRecordType.CHECKPOINT_BEGIN) {
                beginIdx = i;
                break;
            }
        }
        if (beginIdx < 0) {
            return 0;
        }
        for (int i = beginIdx + 1; i < records.size(); i++) {
            if (records.get(i).type() == LogRecordType.CHECKPOINT_END) {
                return beginIdx;
            }
        }
        return 0;
    }

    private boolean redoRecordIfNeeded(DiskManager diskManager, LogRecord rec) {
        int pageId = rec.pageId();
        diskManager.ensurePageCountAtLeast(pageId + 1);

        Page page = bufferPoolManager.fetchPage(pageId);
        page.wlatch();
        boolean modified = false;
        try {
            if (page.getPageLsn() >= rec.lsn()) {
                return false;
            }

            switch (rec.type()) {
                case UPDATE -> {
                    if (rec.slotId() < 0) {
                        byte[] img = rec.afterImage();
                        if (img.length == page.getPageSize()) {
                            System.arraycopy(img, 0, page.getData(), 0, img.length);
                            bufferPoolManager.recordPageModification(page, rec.lsn());
                            modified = true;
                        }
                    } else {
                        SlottedPage sp = ensureSlottedPageInitialized(page);
                        if (!sp.putSlot(rec.slotId(), rec.afterImage())) {
                            throw new IllegalStateException("Redo failed to apply UPDATE on page " + pageId + " slot " + rec.slotId());
                        }
                        bufferPoolManager.recordPageModification(page, rec.lsn());
                        modified = true;
                    }
                }
                case INSERT -> {
                    if (rec.slotId() >= 0) {
                        SlottedPage sp = ensureSlottedPageInitialized(page);
                        if (!sp.putSlot(rec.slotId(), rec.afterImage())) {
                            throw new IllegalStateException("Redo failed to apply INSERT on page " + pageId + " slot " + rec.slotId());
                        }
                        bufferPoolManager.recordPageModification(page, rec.lsn());
                        modified = true;
                    }
                }
                case DELETE -> {
                    if (rec.slotId() >= 0) {
                        SlottedPage sp = ensureSlottedPageInitialized(page);
                        if (sp.isValidSlot(rec.slotId())) {
                            sp.delete(rec.slotId());
                        }
                        bufferPoolManager.recordPageModification(page, rec.lsn());
                        modified = true;
                    }
                }
                case CLR -> {
                    if (rec.slotId() >= 0) {
                        SlottedPage sp = ensureSlottedPageInitialized(page);
                        if (rec.afterImage().length == 0) {
                            if (sp.isValidSlot(rec.slotId())) {
                                sp.delete(rec.slotId());
                            }
                        } else {
                            if (!sp.putSlot(rec.slotId(), rec.afterImage())) {
                                throw new IllegalStateException("Redo failed to apply CLR on page " + pageId + " slot " + rec.slotId());
                            }
                        }
                        bufferPoolManager.recordPageModification(page, rec.lsn());
                        modified = true;
                    }
                }
                default -> {
                }
            }
            return modified;
        } finally {
            page.wunlatch();
            bufferPoolManager.unpinPage(pageId, modified);
        }
    }

    private boolean undoSingleRecord(
            DiskManager diskManager,
            long txnId,
            Map<Long, Long> lastWrittenLsn,
            LogRecord rec) {
        int pageId = rec.pageId();
        int slotId = rec.slotId();
        if (pageId <= 0 || slotId < 0) {
            return false;
        }
        diskManager.ensurePageCountAtLeast(pageId + 1);

        switch (rec.type()) {
            case INSERT -> {
                long prevLsn = lastWrittenLsn.getOrDefault(txnId, rec.lsn());
                long clrLsn = logManager.appendClr(
                        txnId,
                        prevLsn,
                        rec.tableName(),
                        pageId,
                        slotId,
                        EMPTY_BYTES,
                        rec.prevLsn());
                lastWrittenLsn.put(txnId, clrLsn);

                Page page = bufferPoolManager.fetchPage(pageId);
                page.wlatch();
                try {
                    SlottedPage sp = ensureSlottedPageInitialized(page);
                    if (sp.isValidSlot(slotId)) {
                        sp.delete(slotId);
                    }
                    bufferPoolManager.recordPageModification(page, clrLsn);
                } finally {
                    page.wunlatch();
                    bufferPoolManager.unpinPage(pageId, true);
                }
                return true;
            }
            case DELETE, UPDATE -> {
                byte[] beforeBytes = rec.beforeImage();
                long prevLsn = lastWrittenLsn.getOrDefault(txnId, rec.lsn());
                long clrLsn = logManager.appendClr(
                        txnId,
                        prevLsn,
                        rec.tableName(),
                        pageId,
                        slotId,
                        beforeBytes,
                        rec.prevLsn());
                lastWrittenLsn.put(txnId, clrLsn);

                Page page = bufferPoolManager.fetchPage(pageId);
                page.wlatch();
                try {
                    SlottedPage sp = ensureSlottedPageInitialized(page);
                    if (!sp.putSlot(slotId, beforeBytes)) {
                        throw new IllegalStateException("Undo failed to restore slot " + slotId + " on page " + pageId);
                    }
                    bufferPoolManager.recordPageModification(page, clrLsn);
                } finally {
                    page.wunlatch();
                    bufferPoolManager.unpinPage(pageId, true);
                }
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    private static SlottedPage ensureSlottedPageInitialized(Page page) {
        return SlottedPage.wrap(page);
    }
}
