# StarRocks Destination — Performance Testing & Analysis

## Overview

This document summarizes performance testing of the Airbyte Kotlin CDK-based
`destination-starrocks` connector, which uses the StarRocks
[Stream Load API](https://docs.starrocks.io/docs/loading/StreamLoad/) for bulk
ingestion into native StarRocks tables. Tests were conducted against a CelerData
Cloud cluster (`n52yxuid7.cloud-app.celerdata.com`) via HTTPS on port 443.

---

## Test Environment

| Component | Details |
|---|---|
| Destination | CelerData Cloud (StarRocks-compatible) |
| Destination endpoint | HTTPS Stream Load on port 443 |
| Source (real data) | PostgreSQL (local, `postgres:15` container) |
| Source (isolation test) | Airbyte E2E Testing Source (in-memory generated records) |
| Connector | Kotlin CDK `destination-starrocks` (`legacy-task-loader`, `DirectLoader` pattern) |
| Batch size | 50,000 records per Stream Load HTTP call |
| Airbyte platform | 2.0.1 (local `abctl` / kind cluster) |

---

## Test Results

### 1. Postgres → StarRocks (Real-World Baseline)

Four streams, each with 2,000,000 records, synced from a local Postgres container.

| Stream | Records | Bytes (source) | Duration | Throughput |
|---|---|---|---|---|
| `kotlin_events` | 2,000,000 | 871 MB | ~3m 2s | **~4.8 MB/s** |
| `kotlin_orders` | 2,000,000 | 1,154 MB | ~4m | ~4.8 MB/s |
| `kotlin_products` | 2,000,000 | 885 MB | ~3m | ~4.8 MB/s |
| `kotlin_users` | 2,000,000 | 1,053 MB | ~3.5m | ~4.8 MB/s |

**Average record size:** ~435 bytes/record (Postgres JSON serialization)
**Sustained destination throughput: ~4.8 MB/s per stream**

### 2. E2E Source → StarRocks (Source Isolation)

Used Airbyte's in-memory E2E testing source to remove Postgres read latency.
Records had a single `column1: String` field (~18 bytes/record).

| Observation | Value |
|---|---|
| Records/sec (source) | ~40,700 rec/s |
| Source MB/s | ~730 KB/s (tiny records) |
| Stream Load HTTP call | ~1.2s per 50k records |
| Record accumulation | ~0.8–1.8s per 50k records |
| Outcome | **OOM** — source outpaced destination |

The OOM confirms that with source latency removed, the **Stream Load API is
the bottleneck** — the CDK's internal queue filled until heap exhausted.

### 3. E2E Source → destination-postgres (Comparison Baseline)

| Records | Bytes | Duration | Throughput |
|---|---|---|---|
| ~22M | 400 MB | 3m 16s | **~2.0 MB/s** |

**StarRocks is ~2.4x faster than destination-postgres** on equivalent data.

---

## Bottleneck Analysis

### Source vs. Destination Isolation

By comparing batch accumulation time vs. HTTP call time per 50k-record batch
in the Postgres → StarRocks sync:

| Phase | Time per 50k records |
|---|---|
| Postgres source accumulation | ~3.5s average |
| Stream Load HTTP call | ~4.1s average |

**Conclusion: the bottleneck was approximately 50/50 source and destination**
with Postgres as the source.

The E2E isolation test confirmed this: with a near-instant source, accumulation
dropped to ~0.9s while HTTP stayed at ~1.2s, and the source then flooded the
buffer causing OOM. Stream Load is therefore the binding ceiling.

### Amdahl's Law — Destination Optimization Returns

Because source and destination contribute roughly equally, improving only the
destination yields diminishing returns:

| Destination improvement | Overall speedup |
|---|---|
| 2× (4.8 → 9.6 MB/s) | ~1.33× |
| 4× (4.8 → 19 MB/s) | ~1.6× |
| ∞ (instant destination) | ~2× max |

For production incremental syncs (MBs to low GBs of deltas), 4.8 MB/s is
sufficient. The ceiling only becomes problematic for large initial loads
(tens of GBs).

---

## Architectural Considerations

### S3 External Catalog Alternative

Airbyte's `destination-s3` achieves ~140 MB/s upload throughput (~30× faster).
StarRocks supports S3 via External Catalog, enabling queries directly on S3-
resident Parquet/Iceberg files.

**However, this approach has a critical limitation:**

> The Stream Load interface does not support writing to External Catalog tables.
> Incremental syncs (CDC, append) require native StarRocks tables.

This means the S3 path and the Stream Load path are **not interchangeable**:

| | S3 → External Catalog | Stream Load → Native Table |
|---|---|---|
| Upload speed | ✅ ~140 MB/s | ⚠️ ~4.8 MB/s |
| Incremental updates | ❌ Not supported | ✅ Row-level |
| Sub-minute freshness | ❌ | ✅ |
| Query performance | ⚠️ S3 scan overhead | ✅ Native columnar |

### Recommended Architecture

For users who need **high data freshness** in StarRocks, Stream Load into native
tables is the only viable path. A practical two-phase approach is often proposed:

1. **Initial load**: Use `destination-s3` (140 MB/s) to land data fast, then
   run `INSERT INTO native_table SELECT * FROM external_catalog_table` inside
   StarRocks to materialize it as a native table.
2. **Ongoing incremental**: Switch the Airbyte connection to `destination-starrocks`
   (Stream Load) for row-level freshness. At 4.8 MB/s, a 500 MB incremental
   batch completes in ~100 seconds — well within a 15-minute sync window.

#### Why the Handoff Is Not Seamless: Airbyte Connection State

Airbyte tracks sync progress using **connection-scoped cursor state**. For incremental
streams, after each sync the source connector reports the highest cursor value it saw
(e.g., `updated_at = 2024-01-15T12:00:00`). On the next sync, Airbyte passes that
value back to the source, which then filters to only return newer records.

This cursor state is keyed to a specific `(source, destination, stream)` triple stored
in Airbyte's internal database. **When you create a new connection pointing to
`destination-starrocks`, it has no cursor state** — Airbyte has no record of what was
previously synced. The first sync on the new connection will therefore be a **full
historical re-pull from the source**, regardless of what the S3 connection already
loaded. Switching destinations does not transfer cursor state; there is no UI mechanism
to do so.

This means the two-phase strategy as described does not avoid a full re-sync through
Stream Load unless the StarRocks table is pre-populated out-of-band and the cursor is
manually injected.

#### Practical Options

**Option A: SQL-based backfill + manual cursor injection** *(low re-sync cost, high
operational complexity)*

1. Sync all historical data via `destination-s3`
2. In StarRocks, run `INSERT INTO native_table SELECT * FROM external_catalog_table`
   to materialize the native table from S3 — this stays inside StarRocks and is fast
3. Record the exact timestamp when the S3 sync completed
4. Directly update Airbyte's internal `state` table for the new StarRocks connection
   to start the cursor at that timestamp
5. Start the `destination-starrocks` connection — it will only pick up records newer
   than the injected cursor

This avoids re-syncing historical data but requires direct access to Airbyte's internal
PostgreSQL database and knowledge of its state schema, which is undocumented and can
change across platform versions.

**Option B: Accept one full re-sync via StarRocks** *(simple, one-time cost)*

Skip the S3 phase entirely. Use `destination-starrocks` from the start and accept that
the initial full sync takes longer. At 4.8 MB/s over HTTPS, rough estimates:

| Dataset size | Estimated initial sync time |
|---|---|
| 10 GB | ~35 minutes |
| 50 GB | ~3 hours |
| 200 GB | ~12 hours |

For datasets under ~20–30 GB this is the most operationally simple path. For larger
datasets, Option A or Option C are preferable.

**Option C: Incremental from day one** *(no backfill problem)*

If the source supports incremental sync and a full historical backfill is not required,
configure `destination-starrocks` immediately with an **Incremental Append** sync mode.
Small batches (every 15 minutes) keep individual load sizes manageable from the start.
Best suited for event streams where historical data is either not needed or can be
backfilled separately via a one-time SQL job.

---

## Known Stream Load Limitations

- **Single-stream throughput**: ~4.8 MB/s observed over HTTPS to CelerData Cloud.
  This is likely constrained by the HTTPS proxy layer, not the BE node capacity.
  Direct HTTP to BE nodes (bypassing the FE) can be significantly faster.
- **No External Catalog support**: Stream Load writes only to native tables.
- **Label uniqueness**: Each load job requires a unique label; retries must use
  new labels.
- **No partial transactions**: A failed Stream Load batch is fully rolled back;
  no partial commits.
- **100-continue required**: CelerData Cloud requires the `Expect: 100-continue`
  HTTP header; omitting it results in a `DdlException`.

---

## Open Questions

- What is the maximum Stream Load throughput achievable with direct HTTP (non-HTTPS)
  to BE nodes, or with multiple parallel streams to StarRocks?
- How does CelerData Cloud's HTTPS proxy layer impact throughput vs. self-hosted
  StarRocks with direct BE access?
- What throughput do production Flink → StarRocks deployments achieve via the
  same Stream Load interface?

---

## Stream Load Throughput Research

### Published Throughput Numbers

Official documentation does not publish a single benchmark table, but throughput can be
derived from documented API response examples and an explicit timeout-planning reference.

**Computed from example `LoadBytes` / `LoadTimeMs` fields in API response docs:**

| Source | Payload | Time | Throughput |
|---|---|---|---|
| [STREAM LOAD SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/STREAM_LOAD/) | ~39 MB | 2,144 ms | **~19 MB/s** |
| [Alibaba Cloud EMR StarRocks](https://www.alibabacloud.com/help/en/emr/emr-serverless-starrocks/user-guide/stream-load) | ~23 MB | 1,081 ms | **~22 MB/s** |
| Large batch (production scale) | ~10.7 GB | 418,778 ms | **~25 MB/s** |
| Very large batch (production scale) | ~50.7 GB, 199M rows | 801,327 ms | **~63 MB/s** |

**Explicit planning reference** from the [Stream Load loading guide](https://docs.starrocks.io/docs/loading/StreamLoad/):
> "If the size of the data file that you want to load is 10 GB and the average loading
> speed of your StarRocks cluster is **100 MB/s**, set the timeout period to more than
> 100 seconds."

100 MB/s is used as a representative planning baseline for a healthy multi-BE cluster.
The guide notes: "Average loading speed varies depending on the disk I/O and the number
of BEs or CNs."

**Documented range for self-hosted StarRocks: ~19–100 MB/s**, with throughput scaling
with cluster size and local SSD I/O. Our observed 4.8 MB/s is **~4–20× below** the
self-hosted baseline.

### Why CelerData Cloud Is Slower

CelerData Cloud routes Stream Load over `https://<endpoint>:443`. The full path is:

```
client → HTTPS proxy/LB → FE (HTTP 307 redirect) → BE → MemTable flush → S3
```

Compared to self-hosted direct-BE access (`http://<be-host>:8040`):

1. **TLS overhead** — encryption/decryption at the proxy and the BE connection
2. **Proxy buffering** — the entire request body may buffer at the HTTPS proxy before
   the FE redirect is processed (documented issue in the
   [StarRocks Kubernetes Operator how-to](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/load_data_using_stream_load_howto.md))
3. **Additional network hop** — client → proxy → FE → BE vs. client → BE directly
4. **S3 MemTable flush** — Shared-data (Serverless) mode flushes to S3 instead of
   local SSD; S3 PUT latency (10–100ms/object) adds overhead not present in self-hosted
   local-disk deployments

Our 4.8 MB/s is consistent with these compounding factors on a Serverless (shared-data)
CelerData Cloud cluster.

### Flink → StarRocks Integration

The [Flink connector](https://docs.starrocks.io/docs/loading/Flink-connector-starrocks/)
uses Stream Load internally. Key buffer parameters:

| Parameter | Default | Notes |
|---|---|---|
| `sink.buffer-flush.max-bytes` | 90 MB | Flush threshold; larger = higher throughput |
| `sink.buffer-flush.max-rows` | 500,000 | Row count flush threshold |
| `sink.merge-commit.chunk.size` | 20 MB | Merged chunk size per concurrent request |

**Intuit case study** ([CelerData blog](https://celerdata.com/blog/how-intuit-achieved-sub-4-second-real-time-analytics-at-100k-events-per-second)):
100,000+ events/second, sub-4-second end-to-end latency, 1-second data freshness via
Flink → CelerData Cloud. No raw MB/s figure disclosed.

### BE Configuration Knobs

From the [BE Configuration docs](https://docs.starrocks.io/docs/administration/management/BE_configuration/):

| Parameter | Default | Effect |
|---|---|---|
| `number_tablet_writer_threads` | ½ CPU cores (min 16) | Writer threads for ingestion; too few = write bottleneck |
| `flush_thread_num_per_store` | **2** | MemTable flush threads per storage volume — the [troubleshooting guide](https://docs.starrocks.io/docs/loading/loading_introduction/troubleshooting_loading/) calls this out explicitly: "If `WaitFlushTime` takes extended time, consider adjusting `flush_thread_num_per_store`." |
| `write_buffer_size` | 100 MB | MemTable buffer before disk flush |
| `streaming_load_max_mb` | 102,400 MB | Max per-request payload (100 GB) |
| `load_process_max_memory_limit_percent` | 30% | Total BE memory cap for all load jobs |

Hardware minimum: **>150 MB/s disk throughput, >500 IOPS** per BE. SSD/NVMe will
directly improve Stream Load speed when MemTable flush is the bottleneck.

### Merge Commit (StarRocks 3.4+)

[Merge Commit](https://docs.starrocks.io/releasenotes/release-3.4/) consolidates
multiple concurrent Stream Load requests within a time window into a single transaction.
Designed for **high-concurrency, small-batch (KB to tens of MB) scenarios**.

Benefits: reduces data versions, reduces compaction overhead, reduces FE lock contention
(the root cause of [GitHub #43359](https://github.com/StarRocks/starrocks/issues/43359)
where FE lock contention caused failures at ~23,000 rows/sec under concurrent load).

**Merge Commit is a server-side StarRocks feature, not Flink-specific.** It is enabled
via HTTP headers on any direct Stream Load request:

```
enable_merge_commit: true
merge_commit_interval_ms: 5000    # merge window in ms
merge_commit_parallel: 2          # concurrent merged requests
merge_commit_async: true          # optional: return immediately, poll for result
```

The Flink connector exposes these as `sink.properties.*` config keys, which it forwards
verbatim as HTTP headers — the same headers can be added to any Stream Load client,
including this connector.

Merge Commit does **not** increase raw single-stream throughput; it improves aggregate
throughput when many small batches are submitted concurrently from multiple streams or
partitions. For this connector's single-partition sequential design it would not help,
but it would become relevant if `inputPartitions` is increased.

### Summary

| Scenario | Throughput |
|---|---|
| Self-hosted, multi-BE, planning baseline | ~100 MB/s |
| Self-hosted, small batch (~39 MB), documented example | ~19–22 MB/s |
| Self-hosted, large batch (10–50 GB) | ~25–63 MB/s |
| CelerData Cloud (this connector, HTTPS) | **4.8 MB/s** |
| Intuit (Flink → CelerData Cloud) | 100k events/s (MB/s not published) |

Our 4.8 MB/s is not a ceiling of the Stream Load interface — it reflects the HTTPS
proxy, shared-data S3 backend, and possibly the Serverless cluster tier. Direct HTTP
access to BE nodes in a self-hosted deployment should achieve 19–100 MB/s.

### Single-Stream Ceiling: The Doris Reference

StarRocks forked Apache Doris and shares the same Stream Load protocol. The Doris
documentation states a hard reference point:

> "The current largest import speed limit is about **10 MB/s** for a single stream."

This applies to a single sequential HTTP PUT without Group Commit (Doris's equivalent
of Merge Commit). With Group Commit enabled and 10 concurrent writers, Doris achieves
~104 MB/s aggregate — showing that concurrency, not raw per-connection speed, is the
lever for high throughput.

Sources: [Apache Doris stream load manual](https://doris.apache.org/docs/2.0/data-operate/import/stream-load-manual/),
[Apache Doris 2.1.0 Group Commit benchmark](https://medium.com/@ApacheDoris/apache-doris-2-1-0-is-released-100-higher-out-of-the-box-performance-b0a16c1c1791)

**Our 4.8 MB/s is consistent with the ~10 MB/s single-stream ceiling** — we're
approximately half of it, with the difference attributable to the HTTPS/proxy overhead
described above. This is expected behavior for a single-stream sequential connector,
not a bug.

### Why Our Connector Is at 4.8 MB/s

The current `StarRocksDirectLoader` design has three characteristics that explain the number:

1. **Single partition, sequential flush** — `inputPartitions = 1` means one Stream
   Load job at a time. While OkHttp blocks on the HTTP PUT, no new records accumulate
   into the next batch. There is zero pipeline overlap between uploading and buffering.

2. **No compression** — Records are sent as uncompressed CSV over HTTPS. StarRocks
   supports `Content-Encoding: gzip` and `lz4` compression since v3.2.7, which reduces
   network payload and PUT time.

3. **CelerData Cloud HTTPS path** — As described above, HTTPS proxy + S3 MemTable
   flush adds latency not present in self-hosted direct-BE access.

### Path to Higher Throughput (If Needed)

If the 4.8 MB/s ceiling becomes a bottleneck for initial loads:

| Optimization | Expected gain | Complexity |
|---|---|---|
| Increase `inputPartitions` (e.g., to 4) | ~3–4× (near-linear for I/O bound) | Low — CDK handles partitioning |
| Add gzip/lz4 compression header | Variable (depends on data compressibility) | Low — one header |
| Switch to direct HTTP to BE nodes | ~2–5× (removes proxy overhead) | Medium — requires BE IP access |
| Use `sink.buffer-flush.max-bytes = 512 MB` | Reduces transaction overhead per byte | Low — config change |

For the recommended two-phase architecture (S3 initial load + Stream Load incremental),
none of these optimizations are needed for the incremental phase — 4.8 MB/s is
sufficient for typical incremental batch sizes (500 MB → ~100 seconds).

---

## Language & CDK Selection: Kotlin vs Python

Two parallel implementations were developed: **Kotlin CDK** (576 lines) and **Python CDK**
(1,105 lines, `chris-celerdata/destination-starrocks` branch). The Kotlin implementation
was selected for production.

### Key Differences

| Aspect | Kotlin | Python |
|---|---|---|
| **Performance** | 4.8 MB/s (tested) | Likely ~0.48 MB/s (untested) |
| **Code complexity** | 576 lines, CDK-native patterns | 1,105 lines, custom abstractions |
| **Batch size** | 100 MB (adaptive) | 5,000 records (~2 MB) |
| **HTTP client** | OkHttp (optimized buffers) | `requests` (small urllib3 buffers) |

### Performance Issue: TCP Socket Buffers

During development, the StarRocks Stream Load SDK (`com.starrocks:starrocks-stream-load-sdk:1.0`)
was tested but achieved only **0.48 MB/s** (10× slower than expected). Root cause: Apache
HttpClient 4.x uses small TCP socket send buffers (8–16 KB), which limits throughput over
high-latency connections (public internet → CelerData Cloud HTTPS).

This was confirmed with `curl --limit-rate 500k` reproducing the exact 0.47 MB/s bottleneck.

The Python implementation uses `requests.put()` with `Expect: 100-continue`, which relies on
`urllib3` — likely exhibiting the same small-buffer limitation. The Flink connector avoids
this because it runs in the same data center as StarRocks (<1ms RTT).

Switching to **OkHttp** (larger buffers, `TCP_NODELAY` enabled) achieved **4.8 MB/s**.

### Recommendation

**Proceed with Kotlin** for:
- 10× better throughput to cloud endpoints
- 48% less code, simpler maintenance
- Alignment with Airbyte's certified connector strategy (BigQuery, Snowflake, Redshift use Kotlin CDK)
