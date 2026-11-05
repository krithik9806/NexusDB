# NexusDB

NexusDB is a modular, educational, and production-inspired relational and distributed database engine built in Java 21.

## Architecture & Documentation

- [System Architecture & Scope Tiers](docs/architecture.md)
- [Architectural Decision Records (ADRs)](docs/design-decisions.md)
- [Storage Engine Specification](docs/storage-engine.md)
- [SQL Engine Specification](docs/sql-engine.md)
- [Distributed System & Raft Consensus Specification](docs/distributed-system.md)

## Modules

- `nexus-core`: Pure Java 21 storage engine library (disk manager, buffer pool, steal/no-force ARIES-lite WAL, B+Tree index, Strict 2PL concurrency control, and query execution).
- `nexus-distributed`: Multi-Raft consensus and sharding layer wrapping `nexus-core` via a `StateMachine` interface.
- `nexus-server`: Node daemon exposing gRPC endpoints for clients, Raft peers, and cluster management.
- `nexus-client`: Client SDK and CLI driver (`NexusCli`).
- `nexus-management`: Cluster control plane and observability service communicating with nodes strictly over gRPC.
- `nexus-benchmarks`: JMH microbenchmarks and transactional workload benchmarks.

## Build & Verify

```bash
mvn -q verify
```

## Running the SQL Command-Line Client (`NexusCli`)

After building with `mvn -q package`, launch the interactive SQL REPL against a database directory:

```bash
java -jar nexus-client/target/nexus-client-0.1.0-SNAPSHOT.jar --db ./nexusdb-data
```

Inside the REPL:
- Enter single-line or multi-line SQL statements terminated by `;`
- `.tables` — list all tables in the catalog
- `.schema [table]` — inspect `CREATE TABLE` DDL for one or all tables
- `.history` — display in-memory command history
- `.quit` / `.exit` — close the database and exit

To execute a SQL script non-interactively:

```bash
java -jar nexus-client/target/nexus-client-0.1.0-SNAPSHOT.jar --db ./nexusdb-data --file script.sql
```

## Known Limitations

- **Current Implementation Stage**: Phases 1–4b (`v0.1-storage`, `v0.2-sql`, `v0.3-index`, `v0.4-transactions`, `v0.5-sql-complete`) are complete. Multi-Raft consensus and distributed sharding are scheduled in Phases 5–7.
- **Strict 2PL Isolation & Phantom Read Limitation (ADR-002)**: Row-level and unique-key Strict 2PL (`SHARED` locks held until commit/abort for `SELECT`, `EXCLUSIVE` locks held until commit/abort for `INSERT`/`UPDATE`/`DELETE`) provides **Repeatable Read** isolation (preventing dirty reads, non-repeatable reads, and lost updates), while readers and writers on the same row block one another. Because next-key / predicate gap locking is not implemented, range scans can observe **phantom reads** when concurrent transactions insert new rows into an open range.
- **Single-Column Index Predicates**: `BPlusTree` indexes and `CostBasedPlanner` index selection currently target single-column primary and secondary indexes; composite multi-column indexes and histograms (beyond uniform selectivity estimation) are not implemented.
- **Tree-Level B+Tree Lock (`BPlusTree`)**: Concurrent B+Tree searches and mutations are synchronized via a tree-level `ReentrantReadWriteLock` paired with per-page latches rather than fine-grained optimistic latch crabbing.
- **In-Memory Sorting (`SortExecutor`)**: `ORDER BY` currently materializes and sorts matching rows in heap memory; external merge sort for datasets exceeding available memory is not implemented.
- **Single-Page Tuple Size Limit**: Serialized tuples and individual catalog entries must fit within a single slotted page (`pageSize - 28` bytes; 4068 bytes for default 4 KB pages). Overflow pages for large blobs are not implemented.
- **No Cross-Shard Distributed Transactions (ADR-004)**: ACID transactions are strictly scoped to a single shard (single Raft group). Cross-shard Two-Phase Commit (2PC) is intentionally omitted.
- **Strict 2PL Concurrency Control (ADR-002)**: Readers acquire shared locks (`S`) and writers acquire exclusive locks (`X`) held until transaction completion; readers and writers on the same key/tuple can block one another until MVCC is introduced.
- **Dual Logging in Replicated Mode (ADR-003)**: Each shard replica logs consensus commands in the Raft log (`nexus-distributed`) and applies committed entries to `nexus-core`, which maintains its own local WAL and buffer pool.
- **Single-Host / Unencrypted Transport by Default**: gRPC channels between `nexus-client`, `nexus-management`, and `nexus-server` do not enforce mTLS or RBAC out of the box.
