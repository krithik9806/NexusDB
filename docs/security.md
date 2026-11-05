# NexusDB Security Checklist & Hardening Guide

This document outlines the security architecture, risk mitigation protocols, and operational hardening checklist implemented across NexusDB v1.0.0.

---

## 1. Input Validation & SQL Injection Prevention

- **Lexer & Parser Sanitization**: All incoming SQL queries are tokenized by a deterministic recursive-descent lexer that rejects invalid character encodings, malformed identifiers, and unescaped quote literals with line/column pointers.
- **Binder Type Enforcement**: The semantic binder validates table and column schemas, resolving all identifiers against the catalog before execution. Constant expressions are strictly coerced into strongly typed sealed `Value` representations (`IntValue`, `BigIntValue`, `DoubleValue`, `BooleanValue`, `StringValue`).
- **Identifier Protection**: Table, column, and index identifiers are constrained to alphanumeric and underscore characters with a maximum length of 128 characters, matching pattern `^[A-Za-z_][A-Za-z0-9_]{0,127}$`.

---

## 2. Role-Based Access Control (RBAC)

Spring Security strictly enforces fine-grained authorization policies at the control-plane gateway:

| Role | Scope | Permitted Endpoints | Denied Actions |
| :--- | :--- | :--- | :--- |
| **`ADMIN`** | Full Cluster Control | All REST endpoints, SQL Execution, Cluster Topology, Metrics, Fault Injection (`POST /api/faults/action`), Audit Logs | None |
| **`DEVELOPER`** | Data & Query Analysis | SQL Execute (`POST /api/sql/execute`), EXPLAIN (`POST /api/sql/explain`), Query History, Schema Browser, Metrics | Fault Injection (`403 Forbidden`) |
| **`VIEWER`** | Observability Only | Cluster Topology (`GET /api/cluster/topology`), Node Telemetry, Schema Browser, Metrics Timeseries | SQL Execution (`403 Forbidden`), Fault Injection (`403 Forbidden`) |

---

## 3. Authentication & Password Hashing

- **Password Storage**: Passwords are never stored in plaintext. Passwords are salted and hashed using **BCrypt** with an adaptive work factor (default 10 rounds).
- **Stateless Authentication**: Client sessions authenticate via HMAC-SHA256 Signed JSON Web Tokens (`JJWT`). Tokens carry user role and expire after 24 hours (`86,400,000 ms`).
- **Authorization Header**: Bearer tokens are validated on every request via `JwtAuthenticationFilter`. Requests with missing or expired tokens are rejected with `401 Unauthorized` / `403 Forbidden`.

---

## 4. Control Plane Security Audit Log

- **Audit Persistence**: Every administrative and destructive fault action (`DROP_TRAFFIC`, `ADD_LATENCY`, `CLEAR_FAULTS`, `STOP_NODE`, `FORCE_ELECTION`) is permanently logged to the embedded metadata database in table `audit_log`.
- **Logged Attributes**:
  - `username`: Operator identity extracted from verified JWT claims.
  - `action`: Specific administrative or chaos action executed.
  - `target_node`: Node identifier targeted by the action.
  - `details`: Duration, parameter payload, and execution result.
  - `created_at`: Exact UTC timestamp.

---

## 5. Network Boundaries & Process Isolation

- **Zero Framework Core**: `nexus-core` contains zero third-party framework dependencies, completely eliminating transitive dependency vulnerabilities (e.g. Log4Shell, Jackson deserialization gadget chains).
- **gRPC Transport Isolation**: Internal cluster consensus traffic runs over dedicated gRPC communication channels separated from external management HTTP/WebSocket ports.
- **CORS Protection**: The management API implements configurable Cross-Origin Resource Sharing (CORS) filters preventing cross-site scripting (XSS) forgery.
