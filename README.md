# NexusDB

NexusDB is a modular, educational, and production-inspired relational and distributed database engine built in Java 21.

## Architecture & Documentation

- [System Architecture & Scope Tiers](docs/architecture.md)
- [Architectural Decision Records (ADRs)](docs/design-decisions.md)

## Modules

- `nexus-core`: Pure Java 21 storage engine library (disk manager, buffer pool, steal/no-force ARIES-lite WAL, B+Tree index, Strict 2PL concurrency control, and query execution).
- `nexus-distributed`: Multi-Raft consensus and sharding layer wrapping `nexus-core` via a `StateMachine` interface.
- `nexus-server`: Node daemon exposing gRPC endpoints for clients, Raft peers, and cluster management.
- `nexus-client`: Client SDK and CLI driver.
- `nexus-management`: Cluster control plane and observability service communicating with nodes strictly over gRPC.
- `nexus-benchmarks`: JMH microbenchmarks and transactional workload benchmarks.

## Build & Verify

```bash
mvn -q verify
```

## Known Limitations

- **Current Implementation Stage**: The repository is currently at the monorepo bootstrap and architectural specification stage; core storage, WAL, B+Tree, and Raft components are scheduled across Tiers 1–3.
- **No Cross-Shard Distributed Transactions (ADR-004)**: ACID transactions are strictly scoped to a single shard (single Raft group). Cross-shard Two-Phase Commit (2PC) is intentionally omitted.
- **Strict 2PL Concurrency Control (ADR-002)**: Readers acquire shared locks (`S`) and writers acquire exclusive locks (`X`) held until transaction completion; readers and writers on the same key/tuple can block one another until MVCC is introduced.
- **Dual Logging in Replicated Mode (ADR-003)**: Each shard replica logs consensus commands in the Raft log (`nexus-distributed`) and applies committed entries to `nexus-core`, which maintains its own local WAL and buffer pool.
- **Single-Host / Unencrypted Transport by Default**: gRPC channels between `nexus-client`, `nexus-management`, and `nexus-server` do not enforce mTLS or RBAC out of the box.
