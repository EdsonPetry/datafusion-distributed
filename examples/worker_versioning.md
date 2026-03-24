# Worker Versioning Example

Workers expose a `GetWorkerInfo` gRPC endpoint that reports metadata including a
user-defined version string. This is useful during rolling deployments when
workers running different code versions coexist in the cluster.

For full API details and production patterns, see the
[Worker Versioning](../docs/source/user-guide/worker.md#worker-versioning) section
of the user guide.

## Quick start

### 1. Start versioned workers

Open separate terminals and start workers with different version strings:

```bash
# Terminal 1 — v1.0 worker
cargo run --example localhost_worker -- 8080 --version 1.0

# Terminal 2 — v2.0 worker
cargo run --example localhost_worker -- 8081 --version 2.0

# Terminal 3 — v2.0 worker
cargo run --example localhost_worker -- 8082 --version 2.0
```

The `--version` flag is optional. Workers started without it report an empty
version string.

### 2. Run a query against only v2.0 workers

```bash
cargo run --example localhost_run -- \
  "SELECT city, count(*) FROM weather GROUP BY city" \
  --cluster-ports 8080,8081,8082 \
  --version 2.0
```

```
Excluding worker on port 8080 (version mismatch)
Using 2/3 workers matching version '2.0'

+--------+----------+
| city   | count(*) |
...
```

The `--version` flag queries each worker's version via `GetWorkerInfo` and
excludes any worker that doesn't match before the query is planned.

Without `--version`, all workers in `--cluster-ports` are used regardless of
what version they report.
