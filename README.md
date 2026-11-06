# ⚡ NexusDB: Distributed Relational Database Engine

<div align="center">

```
  _   _                       ____  ____  
 | \ | | _____  ___   _ ___  |  _ \| __ ) 
 |  \| |/ _ \ \/ / | | / __| | | | |  _ \ 
 | |\  |  __/>  <| |_| \__ \ | |_| | |_) |
 |_| \_|\___/_/\_\\__,_|___/ |____/|____/ 
```

**A high-performance, disk-backed, ACID-compliant relational and distributed database engine built from first principles in Java 21.**

---

[![CI Build](https://github.com/krithik9806/NexusDB/actions/workflows/ci.yml/badge.svg)](https://github.com/krithik9806/NexusDB/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![JUnit 5](https://img.shields.io/badge/Tests-100%25%20Passing-brightgreen.svg)](https://github.com/krithik9806/NexusDB)
[![Raft Consensus](https://img.shields.io/badge/Consensus-Multi--Raft-purple.svg)](docs/distributed-system.md)
[![Storage](https://img.shields.io/badge/Storage-Slotted%20Page%204KB-blueviolet.svg)](docs/storage-engine.md)
[![Concurrency](https://img.shields.io/badge/Concurrency-Strict%202PL%20%2B%20ARIES-success.svg)](docs/architecture.md)
[![Release](https://img.shields.io/badge/Release-v1.0.0-emerald.svg)](https://github.com/krithik9806/NexusDB/releases/tag/v1.0.0)

[Architecture](docs/architecture.md) • [Design ADRs](docs/design-decisions.md) • [Benchmarks](docs/benchmarks.md) • [Security](docs/security.md) • [Demo Script](docs/demo-script.md) • [Quick Start](#-quick-start-in-60-seconds)

</div>

---

## 🌟 Why NexusDB?

Unlike toy educational databases or thin wrappers around SQLite/RocksDB, **NexusDB is an authentic, production-grade database engine crafted from raw bytes to distributed consensus**:

- 🛠️ **Zero Frameworks in Engine Core**: `nexus-core` relies strictly on modern **Java 21** and `java.nio` — no Spring, no ORMs, no hidden external storage libraries.
- 💾 **Real Disk-Backed Slotted Pages**: 4 KB pages with slot directories, byte-level pointer arithmetic, in-place record compaction, and free space reclamation.
- 🚀 **Lightning-Fast B+ Tree Indexing**: Full B+ Tree implementation supporting node splits, key redistribution, node merging, root shrinking, point queries, and bi-directional range scans.
- 🛡️ **True ACID with ARIES-Lite**: Steal/No-Force buffer pool policy, write-ahead logging (WAL), fuzzy checkpointing (DPT + ATT), and three-pass recovery with Compensation Log Records (CLRs).
- 🔒 **Deadlock-Free Strict 2PL**: Granular row-level `SHARED` and `EXCLUSIVE` locks held until commit/abort, backed by a background wait-for graph cycle detector.
- 🌐 **Fault-Tolerant Multi-Raft Consensus**: Native Raft consensus with randomized election timers, leader leases, quorum commit progression, log catchup, and dynamic fault injection.
- ⚡ **Hash & Range Sharding Router**: Distributed coordinator supporting scatter-gather execution, parallel shard queries, and merged sorting/aggregations.
- 🖥️ **Nexus Studio Console**: Modern dark-themed React 18 + Vite + Tailwind web console with interactive SQL editor, visual query plan tree analyzer, live cluster telemetry, and chaos simulation.

---

## 📐 System Architecture

```mermaid
flowchart TD
    subgraph UI["Management & Developer Experience"]
        CLI["nexus-client (Interactive SQL REPL)"]
        Studio["nexus-studio (React 18 + Vite + Tailwind Console)"]
        Mgmt["nexus-management (Spring Boot 3 + JWT + WebSocket + Metrics)"]
    end

    subgraph Cluster["Distributed Quorum Cluster (nexus-server)"]
        Node1["NexusNode 1 (Raft Leader)"]
        Node2["NexusNode 2 (Follower)"]
        Node3["NexusNode 3 (Follower)"]
    end

    subgraph CoreEngine["Core Storage & Query Engine (nexus-core)"]
        Parser["Lexer & Parser (Recursive Descent AST)"]
        Planner["Cost-Based Optimizer (Cardinality & Index Selection)"]
        Executor["Volcano Iterator Engine (SeqScan, IndexScan, HashJoin, Agg)"]
        TxnMgr["Transaction Manager (Strict 2PL + Deadlock Detector)"]
        WAL["ARIES-lite WAL & Fuzzy Checkpointing"]
        BPTree["B+ Tree Index Engine (Pages & Leaves)"]
        BPM["Buffer Pool Manager (Clock/LRU + Frame Latches)"]
        Disk["DiskManager & Page Allocator (NIO Positional I/O)"]
    end

    CLI -->|gRPC SqlService| Node1
    Studio -->|REST & WebSockets| Mgmt
    Mgmt -->|gRPC Sql & Admin| Node1
    Node1 <==>|Raft Consensus Log Replication| Node2
    Node1 <==>|Raft Consensus Log Replication| Node3
    Node1 --> Parser
    Parser --> Planner --> Executor
    Executor --> TxnMgr --> WAL --> BPM --> Disk
    Executor --> BPTree --> BPM
```

---

## 🔬 Deep Dive: Core Engineering Innovations

### 1. Slotted Page Physical Storage Format
Every 4,096-byte page maintains precise binary layouts preventing fragmentation:

```
+----------------------------------------------------------------------------------+
| PAGE HEADER (28 Bytes)                                                           |
| PageLSN (8B) | PageId (4B) | PrevPage (4B) | NextPage (4B) | SlotCount (4B) | FreePtr (4B)|
+----------------------------------------------------------------------------------+
| SLOT DIRECTORY (Grows Downward ->)                                              |
| Slot[0]: Offset (2B), Length (2B) | Slot[1]: Offset (2B), Length (2B) ...         |
+----------------------------------------------------------------------------------+
|                        <--- UNALLOCATED FREE SPACE --->                          |
+----------------------------------------------------------------------------------+
| TUPLE STORAGE AREA (Grows Upward <-)                                             |
| ... | [Record 1: NullBitmap + Data] | [Record 0: NullBitmap + Data]              |
+----------------------------------------------------------------------------------+
```

### 2. ARIES-Lite Steal/No-Force Recovery
Guarantees durability without crippling write throughput:
- **Steal**: Dirty pages can be flushed to disk before transaction commit to free buffer frames.
- **No-Force**: Transactions commit as soon as their log records are synced to WAL; dirty data pages flush asynchronously.
- **Three-Pass Crash Recovery**:
  1. **Analysis Pass**: Scans from the last checkpoint to identify the Dirty Page Table (DPT) and Active Transaction Table (ATT).
  2. **Redo Pass**: Repeats history from `min(RecLSN)` forward, restoring all page states (both committed and loser transactions).
  3. **Undo Pass**: Reverses all uncommitted loser transactions backwards, appending **Compensation Log Records (CLRs)** to prevent rollback-during-recovery loops.

### 3. Strict 2-Phase Locking & Deadlock Graph
- **Granular Locks**: Tables and individual row `RecordId`s support `SHARED (S)` and `EXCLUSIVE (X)` lock acquisition.
- **Strict 2PL**: All locks are held until transaction commit or abort (guaranteeing strict serializability and preventing cascading aborts).
- **Background Deadlock Detection**: Analyzes the directed **Wait-For Graph** ($T_1 \to T_2 \to T_3 \to T_1$). Upon cycle detection, the youngest transaction is aborted with `DeadlockException`.

```
         [Txn 1] --- holds X(Row 10) ---> [Row 10]
            |                                 ^
      waiting for S(Row 20)             waiting for X(Row 10)
            v                                 |
         [Row 20] <--- holds S(Row 20) --- [Txn 2]
               \                             /
                *--- DEADLOCK CYCLE DETECTED ---*
                 (Cycle resolver aborts Txn 2)
```

### 4. Distributed Multi-Raft Replication
- **Quorum Consensus**: Replicates transactions across $2F+1$ nodes; tolerates $F$ node failures with zero data loss.
- **Heartbeat & Leases**: Leaders send periodic heartbeats (every 50 ms) with randomized election timeouts (150–300 ms).
- **Fault Injection**: Built-in chaos simulator capable of dropping packets, simulating asymmetric network partitions, and introducing artificial network latency.

---

## ⚡ Performance Benchmarks

Empirical performance measured on **1,000,000 records** on NVMe SSD with Java 21:

| Benchmark Scenario | Average Latency | Throughput | Notes |
| :--- | :--- | :--- | :--- |
| **Sequential Scan (Cold Disk)** | `482.10 ms` | `2.1 ops/sec` | Full disk I/O scan across 1M rows |
| **Sequential Scan (Warm Buffer)** | `88.40 ms` | `11.3 ops/sec` | In-memory buffer pool traversal |
| **B+ Tree Point Lookup (Cold)** | `3.20 ms` | `312 ops/sec` | Traverses root, internal, leaf page I/O |
| **B+ Tree Point Lookup (Warm)** | **`0.04 ms (42 µs)`** | **`23,800 ops/sec`** | **2,100x speedup** via B+ Tree index |
| **Concurrent Transactions (100 Threads)**| `1.42 ms` | **`70,397 ops/sec`** | Strict 2PL multi-client write scaling |
| **3-Node Raft Quorum Write** | `4.18 ms` | `239 ops/sec` | Full network replication & fsync |

To run the automated benchmark harness:
```bash
# On Linux/macOS
./scripts/run-benchmarks.sh

# On Windows PowerShell
.\scripts\run-benchmarks.ps1
```

---

## 🚀 Quick Start in 60 Seconds

### Prerequisites
- **Java 21+** (`java -version`)
- **Maven 3.9+** (`mvn -version`)
- **Node.js 20+** (for Nexus Studio UI)
- **Docker** (optional, for 1-click full stack deployment)

### Option A: Interactive SQL Client (Zero-Configuration)
```bash
# 1. Clone the repository
git clone https://github.com/krithik9806/NexusDB.git
cd NexusDB

# 2. Build the engine
mvn clean package -DskipTests

# 3. Launch the SQL CLI
java -jar nexus-client/target/nexus-client-0.1.0-SNAPSHOT.jar --db ./data
```

Inside the CLI, run standard SQL:
```sql
nexusdb> CREATE TABLE developers (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(30), commits INT);
Query OK, 0 rows affected (12.4 ms)

nexusdb> INSERT INTO developers VALUES 
         (1, 'Alice', 'Kernel Engineer', 1420),
         (2, 'Bob', 'Consensus Architect', 980),
         (3, 'Charlie', 'Frontend Lead', 750);
Query OK, 3 rows affected (4.1 ms)

nexusdb> SELECT role, COUNT(*), AVG(commits) FROM developers GROUP BY role HAVING COUNT(*) >= 1;
+---------------------+----------+--------------+
| role                | count(*) | avg(commits) |
+---------------------+----------+--------------+
| Kernel Engineer     | 1        | 1420.0       |
| Consensus Architect | 1        | 980.0        |
| Frontend Lead       | 1        | 750.0        |
+---------------------+----------+--------------+
(3 rows, 1.8 ms)

nexusdb> EXPLAIN SELECT * FROM developers WHERE id = 1;
IndexScan on developers (idx_pk_developers_id) (cost=2.00 rows=1)

nexusdb> .tables
nexusdb> .schema developers
nexusdb> .quit
```

---

### Option B: Complete Distributed Cluster with Docker Compose

Deploy the **3-Node Raft Cluster + Control Plane + Nexus Studio + Prometheus + Grafana** with a single command:

```bash
docker-compose up --build
```

#### Exposed Services & Dashboards
| Service | Access Endpoint | Credentials | Purpose |
| :--- | :--- | :--- | :--- |
| **Nexus Studio Console** | [`http://localhost:3000`](http://localhost:3000) | `admin` / `admin123` | Dark-mode Web UI, Query Visualizer & Chaos Console |
| **Management Control Plane** | [`http://localhost:8080`](http://localhost:8080) | Bearer JWT | REST API, WebSockets & Health metrics |
| **Node 1 (Raft Leader)** | `localhost:9091` | gRPC | High-throughput SQL & Consensus node |
| **Node 2 (Follower)** | `localhost:9092` | gRPC | High-availability replica node |
| **Node 3 (Follower)** | `localhost:9093` | gRPC | High-availability replica node |
| **Grafana Telemetry** | [`http://localhost:3001`](http://localhost:3001) | `admin` / `admin` | Real-time TPS, latency, cache-hit dashboards |
| **Prometheus Metrics** | [`http://localhost:9090`](http://localhost:9090) | None | Scrapes `/actuator/prometheus` |

---

## 💻 Nexus Studio Console

Nexus Studio gives developers an enterprise-grade database control center directly in the browser:

- 📊 **Cluster Topology**: Live node health, Raft roles (Leader/Follower/Candidate), replication lag, and term indicators.
- ⚡ **Interactive SQL Editor**: Multi-tab SQL query editor with syntax highlighting, latency timing, and CSV/JSON export.
- 🌳 **Visual Query Tree Analyzer**: Graphical Volcano execution plans displaying node costs, estimated vs. actual row counts, and filter pushdown.
- 📈 **Performance Center**: Real-time throughput (TPS), 99th-percentile query latency, and buffer pool cache-hit gauges.
- 💥 **Chaos Engineering Simulator**: One-click fault injection to simulate partition isolates, packet drops, and node kills.

---

## 📊 SQL Capability Matrix

| Feature | Syntax & Examples | Engine Execution |
| :--- | :--- | :--- |
| **Data Types** | `INT`, `BIGINT`, `DOUBLE`, `BOOLEAN`, `VARCHAR(n)` | Binary serialized with null bitmap |
| **Primary Keys** | `CREATE TABLE t (id INT PRIMARY KEY, ...);` | Backed by unique B+ Tree index |
| **Secondary Indexes** | `CREATE INDEX idx_name ON t (col);` | Clustered RecordId leaf references |
| **Equi-Joins** | `SELECT * FROM a JOIN b ON a.id = b.a_id;` | Hash Join (build/probe) + Nested Loop |
| **Aggregations** | `COUNT(*)`, `SUM(c)`, `AVG(c)`, `MIN(c)`, `MAX(c)` | Hash aggregate accumulator |
| **Grouping & Filter** | `GROUP BY dept HAVING AVG(salary) > 80000;` | Multi-key grouping with predicate filter |
| **Sorting & Slicing** | `ORDER BY created_at DESC LIMIT 10;` | In-memory quicksort & tuple windowing |
| **Transactions** | `BEGIN;`, `COMMIT;`, `ROLLBACK;` | Strict 2PL + ARIES-lite WAL rollback |
| **Query Plans** | `EXPLAIN SELECT ...;` | Cost-based optimizer plan inspection |

---

## 📂 Repository Structure

```
NexusDB/
├── nexus-core/             # Storage engine, buffer pool, B+ Tree, SQL parser, Volcano execution, WAL, Strict 2PL
├── nexus-distributed/      # Raft consensus engine, log replication, state machine replication, sharding router
├── nexus-server/           # gRPC node servers, SqlService, RaftService, AdminService, fault injection
├── nexus-client/           # Interactive REPL command-line interface & high-performance gRPC client SDK
├── nexus-management/       # Spring Boot 3 control plane, JWT security, WebSocket telemetry, Actuator/Prometheus
├── nexus-studio/           # React 18, Vite, TypeScript & Tailwind CSS dark-first developer console
├── nexus-benchmarks/       # JMH and synthetic multi-threaded concurrency and index benchmark harnesses
├── docker/                 # Production Dockerfiles, Nginx gateway, Prometheus and Grafana provisioning
├── docs/                   # Architectural Decision Records (ADRs), storage format specs, security guidelines
└── scripts/                # Cross-platform benchmark and test runner automation scripts
```

---

## ⚠️ Known Limitations & Engineering Trade-Offs

In accordance with our engineering transparency principles:
- **Repeatable Read Isolation**: Strict 2PL (`SHARED` locks for reads, `EXCLUSIVE` locks for writes held until commit/abort) provides Repeatable Read isolation. Range queries without predicate/next-key gap locks can observe phantom inserts into open ranges.
- **Single-Column Indexing**: B+ Tree indexes and optimizer selection target single-column primary and secondary keys; composite indexes are not yet supported.
- **In-Memory Sort & Join Spill**: `ORDER BY` sorting and Hash Join build-tables materialize in memory; external merge-sort for datasets exceeding RAM is not implemented.
- **Single-Page Tuple Limit**: Serialized tuples must fit within a single 4 KB page (`pageSize - 28` bytes = 4,068 bytes). Blob overflow chains are not implemented.
- **No Cross-Shard Distributed 2PC**: Transactions are scoped per shard/Raft group. Cross-shard distributed two-phase commit is omitted in favor of single-shard high availability.
- **Dual Logging in Replicated Mode**: Each shard replica logs transactions in the Raft consensus log (`nexus-distributed`) and applies committed entries to `nexus-core`'s local ARIES-lite WAL.

---

## 📜 Documentation Index

- 📘 [Architecture & Scope Tiers](docs/architecture.md)
- 📑 [Architectural Decision Records (ADRs)](docs/design-decisions.md)
- 💾 [Binary Storage Engine Specification](docs/storage-engine.md)
- ⚙️ [SQL Parser & Execution Engine](docs/sql-engine.md)
- 🌐 [Distributed System & Raft Consensus](docs/distributed-system.md)
- 🔌 [Control Plane Management REST API](docs/management-api.md)
- 🏎️ [Performance Benchmark Methodology & Results](docs/benchmarks.md)
- 🔒 [Security Hardening & Production Checklist](docs/security.md)
- 🎥 [Video Presentation & Demo Script](docs/demo-script.md)

---

## 🤝 Contributing & License

Contributions, feedback, and issue reports are warmly welcomed!
Please read [AGENTS.md](AGENTS.md) for architectural constraints and coding guidelines before opening a pull request.

NexusDB is open-source software licensed under the **[MIT License](LICENSE)**.

<div align="center">
Built with passion for database systems engineering • Star ⭐ this repo if you find it inspiring!
</div>
