# NexusDB

NexusDB is a modular, educational, and production-inspired relational and distributed database engine built in Java 21.

## Modules

- `nexus-core`: Core storage engine, buffer pool, WAL (ARIES-lite), B+Tree index, concurrency control, and query execution.
- `nexus-distributed`: Raft consensus, replication state machine, and sharding coordinator.
- `nexus-server`: Network server and gRPC/wire protocol endpoints.
- `nexus-client`: Client SDK and CLI driver.
- `nexus-management`: Cluster administration and observability service.
- `nexus-benchmarks`: JMH and workload benchmarks.

## Build & Verify

```bash
mvn -q verify
```

## Known Limitations

- Initial bootstrap scaffold only; storage engine, WAL, indexing, and distributed protocols are not yet implemented.

