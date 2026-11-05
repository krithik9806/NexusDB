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

The system catalog stores table schemas, heap starting pages, and index metadata in a dedicated `TableHeap` rooted at `catalogMetaPageId` (recorded in Page 0, typically Page 1).

Each catalog entry is serialized as a record containing:
1. `tableName` (`VARCHAR`)
2. `firstHeapPageId` (`INT`)
3. `columnCount` (`INT`), followed by each column's `(name, DataType, maxLength, nullable, primaryKey)`
4. `indexCount` (`INT`), followed by each index's `(indexName, columnName, rootPageId, unique)`

