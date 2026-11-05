# Architectural Decision Records (ADRs)

This document captures the foundational design decisions and trade-offs in NexusDB.

---

## ADR-001: Steal/No-Force Buffer Pool with ARIES-Lite WAL

### Context
A disk-backed database engine must guarantee atomicity (all-or-nothing) and durability (committed changes survive crashes) while maximizing throughput and supporting transactions larger than main memory.

### Decision
We adopt a **Steal / No-Force** buffer management policy paired with an **ARIES-lite Write-Ahead Log (WAL)**:
- **Steal**: The buffer pool is allowed to evict and write dirty pages modified by uncommitted transactions to disk if memory pressure requires frames, provided the WAL records describing those modifications have already been flushed to disk (`flushedLSN >= pageLSN`).
- **No-Force**: At transaction commit time, modified pages in the buffer pool do **not** need to be synchronously flushed to disk; only the `COMMIT` WAL record (and all prior log records up to its LSN) must be forced (`fsync`) to the sequential WAL file.
- **ARIES-Lite Recovery**: Crash recovery executes three passes over the WAL from the last checkpoint:
  1. **Analysis**: Reconstructs the active transaction table (ATT) and dirty page table (DPT).
  2. **Redo (Repeating History)**: Replays all logged physical/physiological actions forward from the minimum `recLSN` in the DPT unless `pageLSN >= logRecord.LSN`.
  3. **Undo**: Rolls back all loser transactions in reverse LSN order, writing Compensation Log Records (CLRs) so recovery is idempotent across repeated crashes during recovery.

### Consequences
- **Pros**: Sequential WAL writes on commit instead of random page I/O; transactions are not bounded by buffer pool size.
- **Cons**: Recovery implementation is significantly more complex than Shadow Paging or No-Steal/Force, requiring strict tracking of `pageLSN`, `prevLSN`, and `undoNextLSN` (via CLRs).

---

## ADR-002: Strict Two-Phase Locking (Strict 2PL) Before MVCC

### Context
Concurrent transactions accessing shared tuples and pages require isolation to prevent lost updates, dirty reads, and non-repeatable states, and must interact cleanly with physical Undo during crash recovery.

### Decision
We implement **Strict Two-Phase Locking (Strict 2PL)** as the baseline concurrency control protocol before introducing Multi-Version Concurrency Control (MVCC):
- Transactions acquire shared (`S`) locks before reading and exclusive (`X`) locks before modifying records/keys.
- **Strictness invariant**: All locks (`S` and `X`) are held until the transaction either commits or aborts (after WAL commit flush or full undo completion).
- Deadlocks are handled deterministically via a wait-for graph cycle detector (or wound-wait/wait-die timestamp ordering).

### Consequences
- **Pros**: Guarantees conflict serializability and strict schedules (preventing cascading aborts and ensuring physical/logical WAL undo never clobbers another active transaction's uncommitted writes). Simpler storage layout (single tuple version in slotted pages) while verifying core storage and recovery invariants.
- **Cons**: Readers block writers and writers block readers, reducing read concurrency under high contention compared to snapshot-isolated MVCC.

---

## ADR-003: One Raft Group Per Shard Instead of Separate Primary-Replica Replication

### Context
To achieve high availability and fault tolerance across nodes, shards must replicate state changes across multiple failure domains and handle leader failover without split-brain data loss.

### Decision
Every shard is replicated as an independent **Raft consensus group** (Multi-Raft architecture), where `nexus-distributed` wraps a shard's `nexus-core` instance via the `StateMachine` interface, rather than building ad-hoc asynchronous/semi-synchronous primary-replica shipping with an external failover controller.

### Consequences
- **Pros**: Consensus, leader election, and replication are unified under a single formally specified state machine protocol (Raft). Eliminates split-brain bugs and external ZooKeeper/etcd dependencies for per-shard failover.
- **Cons**: Write latency includes a quorum RTT prior to state machine application, and each shard maintains both a Raft log and local storage engine state.

---

## ADR-004: No Cross-Shard Distributed Transactions

### Context
When tables or key ranges are partitioned across multiple shards (each backed by its own Raft group), operations spanning multiple shards would require Two-Phase Commit (2PC) coordinated across Raft groups and a distributed deadlock/clock mechanism (e.g., Percolator or Spanner TrueTime).

### Decision
NexusDB explicitly **does not support cross-shard distributed transactions**. ACID transactions are strictly scoped to a **single shard** (single Raft group / single `StateMachine`). Multi-shard operations are rejected at the routing layer or executed as explicitly non-atomic batch operations.

### Consequences
- **Pros**: Avoids blocking 2PC coordinator states, distributed deadlock detection across network partitions, and clock-skew uncertainty window stalls. Keeps Tier 2 and Tier 3 deterministic and focused on Raft consensus and single-shard serializability.
- **Cons**: Applications requiring multi-record ACID guarantees must co-locate related records within the same shard key / partition.

---

## ADR-005: Separation of Latches vs. Locks

### Context
Database engines face two fundamentally different synchronization problems: protecting in-memory data structures (buffer frames, B+Tree nodes, hash tables) from concurrent OS/JVM threads, and isolating concurrent logical transactions from each other.

### Decision
We maintain a strict architectural and code-level separation between **Latches** and **Locks**:

| Dimension | Locks (Transaction Manager) | Latches (Storage & Buffer Engine) |
| :--- | :--- | :--- |
| **Protected Entity** | Logical database contents (tuples, keys, tables) | Physical in-memory structures (pages, B+Tree nodes, buffer frames) |
| **Holder** | A Transaction (`txnId`) | A Java Thread (`Thread` / `ReentrantReadWriteLock` / `StampedLock`) |
| **Duration** | Held until transaction Commit or Abort (Strict 2PL) | Held only for the duration of a single critical section (nanoseconds/microseconds) |
| **Deadlock Handling** | Wait-for graph detection / transaction abort & rollback | Deadlock-free by coding discipline (top-down B+Tree latch crabbing, fixed lock ordering) |
| **Recovery Visibility** | Backed by Undo logs on abort | Invisible to WAL and transactions; released immediately after page mutation |

### Consequences
- **Pros**: Prevents latch-lock deadlocks (threads never wait for a logical transaction lock while holding a page latch), enables high-concurrency B+Tree latch crabbing (coupling), and keeps `LockManager` completely decoupled from `BufferPoolManager` and page layouts.
- **Cons**: Requires strict developer discipline—never perform blocking network calls or wait on `LockManager` while holding a page or B+Tree latch.

