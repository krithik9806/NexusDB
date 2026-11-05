# NexusDB Storage Engine Specification

This document specifies the binary on-disk formats for NexusDB data files, pages, tuples, catalog metadata, B+Tree nodes, and Write-Ahead Log (WAL) records in `nexus-core`. All multi-byte integers are stored in **big-endian** (`ByteOrder.BIG_ENDIAN`) byte order.

---

## 1. Database File & Header Page (Page 0)

A NexusDB database file is a sequence of fixed-size pages (`pageSize`, configurable at creation time, default **4096 bytes**, minimum 512 bytes, power of two). Page `0` is reserved as the **File Header Page**.

### Page 0 Layout (Byte Offsets)

```
+-------------------+-------------------+-----------------------------------------------+
| Byte Offset       | Size (Bytes)      | Field Description                             |
+-------------------+-------------------+-----------------------------------------------+
| 0..3              | 4                 | Magic Number: 0x4E584442 ("NXDB")             |
| 4..7              | 4                 | Format Version: 1                             |
| 8..11             | 4                 | Page Size in bytes (default 4096)             |
| 12..15            | 4                 | Total Allocated Page Count (including page 0) |
| 16..19            | 4                 | Free Page List Head (pageId, or -1 if empty)  |
| 20..23            | 4                 | Catalog Meta Page ID (pageId, or -1 if none)  |
| 24..31            | 8                 | Checkpoint LSN (or 0 if none)                 |
| 32..pageSize-1    | pageSize - 32     | Reserved (zero-filled)                        |
+-------------------+-------------------+-----------------------------------------------+
```

### Free-Page List Format
When a page is deallocated via `PageAllocator`, its first 8 bytes store:
- Bytes `0..3`: Page Type marker (`0xFFFFFFFF` = Free Page)
- Bytes `4..7`: `nextFreePageId` (`int`, `-1` if end of free list)
The file header's `freeListHead` points to the most recently freed page (LIFO stack), ensuring $O(1)$ page allocation and deallocation.

---

## 2. Slotted Page Format

Data pages (heap pages and catalog pages) use a **Slotted Page** organization so tuples can be inserted, updated, deleted, and compacted without changing their external `RecordId(pageId, slotId)`.

### Slotted Page Layout

```
+-----------------------------------------------------------------------+
| Page Header (16 bytes)                                                |
|  [0..7]   pageLSN          (long, 8 bytes)                            |
|  [8..9]   pageFlags        (short, 2 bytes: page type / flags)        |
|  [10..11] slotCount        (short, 2 bytes: number of slots)          |
|  [12..13] freeSpacePointer (short, 2 bytes: start of tuple data area) |
|  [14..15] fragmentedBytes  (short, 2 bytes: dead space from deletes)  |
+-----------------------------------------------------------------------+
| Optional Linked-Page Header (for TableHeap pages: 8 bytes at 16..23)  |
|  [16..19] prevPageId       (int, 4 bytes, -1 if none)                 |
|  [20..23] nextPageId       (int, 4 bytes, -1 if none)                 |
+-----------------------------------------------------------------------+
| Slot Array (grows downward from header end, 4 bytes per slot)         |
|  Slot i:                                                              |
|   [0..1] tupleOffset (short, 2 bytes; -1 if slot is deleted/empty)    |
|   [2..3] tupleLength (short, 2 bytes; 0 if deleted/empty)             |
+-----------------------------------------------------------------------+
|                         <--- Free Space --->                          |
+-----------------------------------------------------------------------+
| Tuple Data Area (grows upward from pageSize toward Slot Array)        |
+-----------------------------------------------------------------------+
```

- **`RecordId`**: Immutable record `(int pageId, int slotId)` uniquely identifying a tuple's location.
- **Deletion**: Sets the slot's `tupleOffset = -1` and `tupleLength = 0`, adding the old length to `fragmentedBytes`.
- **Insertion**: Reuses the first slot with `tupleOffset == -1` if available, or appends a new slot at `slotCount`. If contiguous space (`freeSpacePointer - slotArrayEnd`) is insufficient but total free space (`contiguous + fragmentedBytes`) is sufficient, in-page **compaction** packs active tuples continuously at the end of the page while preserving all `slotId`s.
- **Updates & Cross-Page Relocation (`TableHeap.update`)**: When a tuple is updated, `SlottedPage.update` updates it in-place if `newLength <= oldLength` or relocates it within the same page (compacting if required) if `newLength - oldLength <= totalFreeSpace`, preserving its `RecordId(pageId, slotId)`. If the updated tuple exceeds the current page's free capacity, `TableHeap.update` deletes the old slot on the source page and inserts the tuple into the tail heap page (allocating a new linked page if needed), returning the relocated `RecordId`.

---

## 3. Data Types & Tuple Binary Layout

### Supported Data Types

| Type | SQL Keyword | Fixed/Variable | Binary Representation |
| :--- | :--- | :--- | :--- |
| `INT` | `INT`, `INTEGER` | Fixed (4 B) | 32-bit signed two's complement integer |
| `BIGINT` | `BIGINT` | Fixed (8 B) | 64-bit signed two's complement integer |
| `DOUBLE` | `DOUBLE`, `FLOAT` | Fixed (8 B) | IEEE 754 double-precision float |
| `BOOLEAN` | `BOOLEAN`, `BOOL` | Fixed (1 B) | `0x00` = `false`, `0x01` = `true` |
| `VARCHAR(n)` | `VARCHAR(n)` | Variable | `short` byte length (2 B) + UTF-8 bytes (max $n$ chars) |

### Tuple Layout

```
+-----------------------------------------------------------------------+
| Null Bitmap: ceil(columnCount / 8) bytes                              |
|  Bit i == 1 means column i is NULL (no payload bytes written for i)   |
+-----------------------------------------------------------------------+
| Column 0 Payload (if bit 0 == 0)                                      |
|  - Fixed-size types: inline 1, 4, or 8 bytes                          |
|  - VARCHAR(n): 2-byte UTF-8 byte length + UTF-8 byte sequence         |
+-----------------------------------------------------------------------+
| Column 1 Payload (if bit 1 == 0) ...                                  |
+-----------------------------------------------------------------------+
```

---

## 4. Persistent Catalog Layout

The system catalog stores table schemas, heap starting pages, and index metadata in a dedicated doubly-linked chain of system `SlottedPage`s rooted at `catalogMetaPageId` (recorded in Page 0, typically Page 1).

Each catalog entry slot contains a big-endian binary record with:
1. `tableName` (`2-byte UTF-8 length + UTF-8 bytes`)
2. `firstHeapPageId` (`4-byte int`)
3. `columnCount` (`4-byte int`), followed by each column's `(name, 1-byte DataType ordinal, 4-byte maxLength, 1-byte nullable, 1-byte primaryKey)`
4. `indexCount` (`4-byte int`), followed by each index's `(indexName, columnName, 4-byte rootPageId, 1-byte unique)`

---

## 5. B+Tree Index Page Layout

B+Tree indexes (`BPlusTree` / `BPlusTreePage` in `io.nexusdb.core.index`) store both **Leaf** (`nodeType = 1`) and **Internal** (`nodeType = 2`) nodes in `BufferPoolManager` pages using a fixed `rootPageId` (when the root splits, left and right child pages are allocated while `rootPageId` is rewritten in-place as the new internal root).

### B+Tree Node Header (32 Bytes, Big-Endian)

```
+-----------------------------------------------------------------------+
| B+Tree Node Header (32 bytes)                                         |
|  [0..7]   pageLSN           (long, 8 bytes)                           |
|  [8]      nodeType          (byte, 1 byte: 1 = LEAF, 2 = INTERNAL)    |
|  [9]      keyType           (byte, 1 byte: DataType ordinal)          |
|  [10]     unique            (byte, 1 byte: 1 = unique, 0 = non-unique)|
|  [11]     reserved          (byte, 1 byte: 0)                         |
|  [12..13] keyCount          (short, 2 bytes: unsigned entry count m)  |
|  [14..15] freeSpacePointer  (short, 2 bytes: start of payload area)   |
|  [16..17] fragmentedBytes   (short, 2 bytes: dead space from deletes) |
|  [18..19] reserved2         (short, 2 bytes: 0)                       |
|  [20..23] prevLeafPageId    (int, 4 bytes: -1 if none / internal)     |
|  [24..27] nextLeafPageId    (int, 4 bytes: -1 if none / internal)     |
|  [28..31] firstChildPageId  (int, 4 bytes: child C_0 for internal)    |
+-----------------------------------------------------------------------+
| Sorted Slot Offset Directory (starts at byte 32, 2 bytes per entry)   |
|  Slot i at [32 + 2*i]: unsigned short offset to entry i's payload     |
+-----------------------------------------------------------------------+
|                         <--- Free Space --->                          |
+-----------------------------------------------------------------------+
| Entry Payload Area (grows upward from pageSize toward Slot Array)     |
+-----------------------------------------------------------------------+
```

### Entry Payload Layout

- **Leaf Entry (`nodeType = 1`)**:
  - `recordPageId` (`int`, 4 B) + `recordSlotId` (`int`, 4 B)
  - `nullFlag` (`byte`, 1 B: `0x00` = `NULL`, `0x01` = non-null)
  - `keyPayload` (omitted when `nullFlag == 0x00`; inline `1/4/8` B for fixed types, or `2-byte UTF-8 length + UTF-8 bytes` for `VARCHAR`)
- **Internal Entry (`nodeType = 2`, representing separator $K_i$ and right child $C_{i+1}$)**:
  - `rightChildPageId` (`int`, 4 B)
  - `sepRecordPageId` (`int`, 4 B) + `sepRecordSlotId` (`int`, 4 B)
  - `nullFlag` (`byte`, 1 B) + `keyPayload`
- **Ordering, Rebalancing & Concurrency**:
  - Unique indexes order entries by `Value` key; non-unique indexes order both leaf entries and internal separators by composite `(Value key, RecordId recordId)`.
  - On deletion (`BPlusTree.delete` / `deleteKey`), underflowing non-root nodes first attempt to borrow (redistribute) an entry from an adjacent left or right sibling (updating or rotating through the parent separator), or merge into the left page and free the right page via `BufferPoolManager.deletePage`. When an internal root loses its last separator key (`keyCount == 0`), its single child is copied into `rootPageId` and freed, shrinking the tree while keeping `rootPageId` constant.
  - Range scans (`BPlusTree.rangeScan`) descend to the lower-bound leaf and walk `nextLeafPageId` links one page at a time without holding page pins across caller iterations.
  - Tree-level concurrency is guarded by a `ReentrantReadWriteLock` (`treeLock`) paired with per-page `rlatch()`/`wlatch()` latches, pinning at most one buffer pool page at any instant.

---

## 6. Write-Ahead Log (WAL) Binary Record Format

The Write-Ahead Log (`LogManager`, `LogRecord`, `LogReader` in `io.nexusdb.core.wal`) stores sequential, big-endian log records framed with a 12-byte header and a 32-bit CRC32 checksum.

### WAL Record Header & Envelope

```
+-----------------------------------------------------------------------+
| WAL Record Header (12 bytes, Big-Endian)                              |
|  [0..3]   magic          (int, 4 B: 0x57414C52 = "WALR")              |
|  [4..7]   payloadLength  (int, 4 B: byte length of payload)           |
|  [8..11]  crc32          (int, 4 B: CRC32 over [magic+len+payload])   |
+-----------------------------------------------------------------------+
| Common Payload Prefix (25 bytes, Big-Endian)                          |
|  [12..19] lsn            (long, 8 B: monotonically increasing >= 1)   |
|  [20..27] txnId          (long, 8 B: transaction ID, 0 for ckpt)      |
|  [28..35] prevLsn        (long, 8 B: previous LSN of txn, or 0)       |
|  [36]     recordType     (byte, 1 B: LogRecordType ordinal)           |
+-----------------------------------------------------------------------+
| Type-Specific Payload (variable length)                               |
+-----------------------------------------------------------------------+
```

### Record Types (`LogRecordType`) & Type-Specific Payloads

| Ordinal | Type | Type-Specific Payload Layout |
| :--- | :--- | :--- |
| `0` | `BEGIN` | *(empty)* |
| `1` | `COMMIT` | *(empty)* |
| `2` | `ABORT` | *(empty)* |
| `3` | `UPDATE` | `tableName` (`2B UTF-8 len + bytes`), `pageId` (`int`), `slotId` (`int`; `-1` for full-page image), `beforeImageLen` (`int`) + `beforeImage`, `afterImageLen` (`int`) + `afterImage` |
| `4` | `INSERT` | `tableName` (`2B UTF-8 len + bytes`), `pageId` (`int`), `slotId` (`int`), `afterImageLen` (`int`) + `afterImage` |
| `5` | `DELETE` | `tableName` (`2B UTF-8 len + bytes`), `pageId` (`int`), `slotId` (`int`), `beforeImageLen` (`int`) + `beforeImage` |
| `6` | `CLR` | `tableName` (`2B UTF-8 len + bytes`), `pageId` (`int`), `slotId` (`int`), `undoNextLsn` (`long`), `afterImageLen` (`int`) + `afterImage` |
| `7` | `CHECKPOINT_BEGIN` | *(empty)* |
| `8` | `CHECKPOINT_END` | `dptSize` (`int`) + `dptSize * (pageId: int, recLsn: long)` + `attSize` (`int`) + `attSize * (txnId: long, lastLsn: long)` |

- **Torn-Write & Corrupt-Tail Handling**: `LogReader` validates `magic == 0x57414C52`, `payloadLength` bounds, file remaining bytes, and `CRC32` over `[magic + payloadLength + payload]`. If a crash leaves a partial or corrupted record at the tail of the WAL file, `LogReader` stops cleanly at the last valid record boundary (`validEndOffset`) and truncates the trailing bytes when opened in recovery/repair mode.

### Write-Ahead Protocol, Dirty Page Table & Fuzzy Checkpoints

1. **Write-Ahead Rule (`flushedLSN >= pageLSN`)**:
   - Every mutation to a `TableHeap`, `BPlusTree`, or `Catalog` page appends a WAL record to `LogManager` first and stamps `page.setPageLsn(lsn)`.
   - Before `BufferPoolManager` writes any dirty frame to `DiskManager` (during LRU eviction, `flushPage`, or `flushAll`), it invokes `LogManager.flushUpTo(pageLsn)` and asserts `logManager.getFlushedLsn() >= pageLsn`.
2. **Dirty Page Table (DPT)**:
   - `BufferPoolManager` maintains an in-memory `dirtyPageTable` mapping each currently dirty `pageId` to its `recLsn`—the LSN of the **first** WAL record that dirtied the page since it was last flushed to disk (`dirtyPageTable.putIfAbsent(pageId, lsn)`).
   - Subsequent modifications to an already-dirty page advance `page.getPageLsn()` without overwriting its `recLsn` in the DPT. Writing the page to disk clears its dirty flag and removes `pageId` from the DPT.
3. **Fuzzy Checkpoint & Log Truncation Policy (`CheckpointManager`)**:
   - A fuzzy checkpoint writes `CHECKPOINT_BEGIN` (`beginLsn`), snapshots the current `DirtyPageTable` (`pageId -> recLsn`) and `ActiveTransactionTable` (`txnId -> lastLsn`) without stalling execution to force dirty data pages to disk, writes `CHECKPOINT_END(dpt, att)` (`endLsn`), forces the WAL up to `endLsn`, and stores `beginLsn` in Page 0's `checkpointLsn` header field.
   - **Log Truncation Boundary**: During recovery, the Redo pass must scan from $\min(\{ \text{beginLsn} \} \cup \{ \text{recLsn} \in \text{DPT} \})$, while the Undo pass must reach the earliest `BEGIN` LSN of any transaction in the `ATT`. Only WAL segments strictly preceding both `minRecLsn` and the earliest active transaction's start LSN are eligible for truncation/compaction.

### Transaction Management, Commit & Undo Rollback (`TransactionManager`)

1. **Transaction Lifecycle (`Transaction`, `TransactionState`)**:
   - Each transaction (`txnId >= 1`) starts in `ACTIVE` state, appends a `BEGIN` WAL record, and links every subsequent modification (`INSERT`, `UPDATE`, `DELETE`, `CLR`) through its `prevLsn` chain (`lastLsn`).
   - SQL sessions execute in **autocommit** mode by default (wrapping each individual DML statement in an implicit transaction) or in explicit multi-statement mode initiated by `BEGIN [TRANSACTION]` and terminated by `COMMIT [TRANSACTION]` or `ROLLBACK [TRANSACTION]`.
2. **Commit Protocol (`TransactionManager.commit`)**:
   - Appends a `COMMIT` WAL record linked to `txn.getLastLsn()`, forces the WAL up to `commitLsn` (`logManager.flushUpTo(commitLsn)`), transitions state to `COMMITTED`, and removes the transaction from the Active Transaction Table (`ATT`). Under the **No-Force** buffer pool policy, dirty data pages do not need to be flushed to disk at commit time.
3. **Rollback & Statement-Level Atomicity (`TransactionManager.rollback` / `rollbackToLsn`)**:
   - Walking `txn.getUndoRecords()` in reverse LSN order (`LIFO`), each logged tuple modification is inverted on its slotted heap page (`INSERT` $\rightarrow$ `SlottedPage.delete(slotId)`; `DELETE`/`UPDATE` $\rightarrow$ `SlottedPage.putSlot(slotId, beforeImage)`), a Compensation Log Record (`CLR`) is appended with `undoNextLsn = record.prevLsn()`, the page's `pageLSN` is updated to `clrLsn`, and all affected B+Tree indexes on the modified tables are rebuilt/synchronized with the restored heap.
   - Full rollback finishes by writing an `ABORT` WAL record and transitioning the transaction to `ABORTED`. When a multi-row `INSERT` or `UPDATE` fails mid-statement (e.g., `DuplicateKeyException`) inside an explicit `BEGIN` block, `rollbackToSavepoint(txn, statementSavepoint)` undoes only the failed statement's changes while keeping earlier statements in the transaction intact.

### ARIES-Lite Crash Recovery (`RecoveryManager`)

On `Database.open(path)`, `RecoveryManager` automatically executes 3-pass ARIES-lite crash recovery:

1. **Pass 1 — Analysis**:
   - Scans forward from the last valid checkpoint (`DiskManager.getCheckpointLsn()`) or from LSN 1 if no checkpoint exists.
   - Reconstructs the **Dirty Page Table (`DPT`)** (`pageId -> recLsn`) and the **Active Transaction Table (`ATT`)** (`txnId -> lastLsn`) containing all loser transactions that were still `ACTIVE` at the moment of the crash.
2. **Pass 2 — Redo (Repeating History)**:
   - Computes $\text{redoStartLsn} = \min(\{\text{checkpointBeginLsn}\} \cup \{\text{recLsn} \in \text{DPT}\})$ (or LSN 1 without a checkpoint) and scans forward to the end of the WAL.
   - Replays every logged modification (`pageWrite`, `INSERT`, `UPDATE`, `DELETE`, `CLR`) for both winner and loser transactions whenever `page.getPageLsn() < record.lsn()`, restoring the database to its exact pre-crash state.
3. **Pass 3 — Undo**:
   - Rolls back all loser transactions in `ATT` in global reverse LSN order (`max(nextUndoLsn)` across all active losers).
   - For each undone tuple modification (`INSERT`, `DELETE`, `UPDATE`), appends a `CLR` record with `undoNextLsn = record.prevLsn()` and stamps `page.setPageLsn(clrLsn)`. If recovery encounters a `CLR` written by an earlier interrupted recovery attempt, it never undoes the `CLR` and instead jumps directly to `CLR.undoNextLsn`, making crash recovery strictly idempotent across repeated crashes during recovery.
   - Once a loser transaction reaches its `BEGIN` record (`prevLsn == 0`), an `ABORT` record is appended. Finally, `Catalog.reload()` and `TableMetadata.rebuildIndexesFromHeap()` synchronize all primary and secondary B+Tree indexes with the recovered table heaps.

---

## 7. Concurrency Control, Strict 2PL & Isolation Guarantees

`LockManager` (`io.nexusdb.core.transaction`) enforces **Strict Two-Phase Locking (Strict 2PL)**:

1. **Lock Modes & Granularities (`LockMode`, `LockId`)**:
   - `SHARED` (`S`) and `EXCLUSIVE` (`X`) locks over `TABLE`, physical `ROW` (`RecordId`), and logical unique-index `KEY` (`(tableName, indexName, keyValue)`) resources.
   - `SELECT` operators (`SeqScanExecutor`, `FilterExecutor`, `IndexScanExecutor`) acquire `SHARED` locks on matching rows before reading; `INSERT`, `UPDATE`, and `DELETE` operators acquire `EXCLUSIVE` locks on affected rows and unique index keys.
   - All `SHARED` and `EXCLUSIVE` locks are held until `TransactionManager.commit(txn)` or `TransactionManager.rollback(txn)` completes.
2. **Lock Upgrades & FIFO Fairness**:
   - A transaction holding `SHARED` on a resource that requests `EXCLUSIVE` upgrades in-place immediately if it is the sole `SHARED` holder, or is queued ahead of non-upgrade waiters on that resource.
   - New lock arrivals cannot bypass queued waiters in a resource's FIFO queue, preventing writer starvation.
3. **Waits-For Graph Deadlock Detection**:
   - A background daemon thread (`nexusdb-deadlock-detector`) and on-demand cycle check construct the directed waits-for graph ($T_{\text{waiter}} \to T_{\text{blocker}}$) and abort the **youngest transaction** ($\max(\text{txnId})$) in any cycle with `DeadlockException`, automatically rolling back the victim transaction's uncommitted modifications and releasing its locks.
4. **Achieved Isolation Level & Phantom Read Limitations**:
   - Holding row-level `SHARED` and `EXCLUSIVE` locks until transaction completion prevents **dirty writes**, **dirty reads**, **non-repeatable reads**, and **lost updates** (**Repeatable Read** semantics on existing rows, and **Serializable** for point-key lookups/inserts on unique indexes).
   - **Phantom Read Limitation**: Because range predicates (`WHERE balance BETWEEN 100 AND 500`) lock existing matching `RecordId`s rather than acquiring next-key / predicate range locks across B+Tree gaps, a concurrent transaction inserting a new row into that range can produce a phantom row if the first transaction re-executes the range query before committing.
5. **Latch & Pin-Ordering Audit (`BufferPoolManager`, `TableHeap`, `BPlusTree`)**:
   - **Global 6-Tier Lock/Latch Hierarchy**: Every thread in `nexus-core` acquires locks and latches strictly in the following acyclic top-down order:
     1. `Database.ddlLock` (`ReentrantReadWriteLock`: read lock for DML/SELECT/transactions, write lock for DDL/checkpoints)
     2. `LockManager` logical transaction locks (`lockTableMutex`: table, unique-key, and row `RecordId` locks)
     3. Structure mutex (`TableHeap.heapLock` / `BPlusTree.treeLock` / `Catalog.catalogLock`)
     4. `BufferPoolManager.poolLock`
     5. Per-page reader/writer latch (`Page.rlatch()` / `Page.wlatch()`)
     6. `LogManager` WAL buffer/flush locks (`bufferLock` / `flushLock`)
   - **Single-Page Pin Discipline**: `TableHeap`, `TableIterator`, `BPlusTree`, `BPlusTreeRangeIterator`, and `Catalog` pin at most **one** buffer pool frame at any instant, unlatching (`runlatch`/`wunlatch`) and unpinning (`unpinPage`) the current page before fetching or allocating any sibling, child, or overflow page. This guarantees that even a 1-frame buffer pool never suffers pin starvation or coupled-pin deadlocks.
   - **Audit Fix 1 — Eliminating `Page.latch <-> poolLock` Inversion**: `BufferPoolManager.dirtyPageTable` uses a `ConcurrentHashMap<Integer, Long>` so `recordPageModification(page, lsn)` (invoked while holding `page.wlatch()`) updates `pageLSN` and `dirtyPageTable` without acquiring `poolLock`. Conversely, `BufferPoolManager.writeBackLocked(frame)` (invoked under `poolLock`) acquires `page.rlatch()` before reading `page.getPageLsn()` and `page.getData()`, preventing concurrent flushes from observing half-mutated pages or inverting latch acquisition order.
   - **Audit Fix 2 — Atomic Unique-Key Index Lookup & In-Place Update Maintenance**: `IndexScanExecutor` acquires the transaction's `SHARED` or `EXCLUSIVE` lock on `LockId.key(tableName, indexName, key)` before probing a unique `BPlusTree`, and `UpdateExecutor` / `TransactionManager` skip `BPlusTree.delete` + `BPlusTree.insert` when neither the indexed column value nor the tuple's `RecordId` changed, eliminating transient index-entry disappearance windows under concurrent `SELECT` + `UPDATE` workloads.
6. **Transaction Observability (`TransactionMetrics`)**:
   - `TransactionManager.getMetrics()` and `Database.getTransactionMetrics()` return an immutable `TransactionMetrics(activeTransactions, lockWaits, deadlockCount, committedCount, abortedCount)` snapshot for monitoring and Studio telemetry.








