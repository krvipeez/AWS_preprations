# 14 — Data Transformation and Processing

> Related: [13 Ingestion](13-data-ingestion-patterns.md) · [17 SQL](17-sql-for-data-engineering-interviews.md) · [18 PySpark](18-pyspark-interview-preparation.md) · [20 Data Quality](20-data-quality-and-reliability.md) · [31 Cookbook](31-python-sql-pyspark-cookbook.md)

---

## 1. ETL vs. ELT

| Dimension | ETL | ELT |
|---|---|---|
| Where transforms run | External engine (Glue/EMR/Batch) before load | Inside the target (Redshift/Athena/Snowflake SQL, dbt) after load |
| Raw data in target | Often not | Yes (raw/staging schemas) |
| Strengths | Heavy/complex logic (ML features, nested parsing, non-SQL), PII removal before landing in warehouse | Simplicity, SQL-centric teams, warehouse scale, lineage via dbt |
| Weaknesses | More moving parts; two engines | Warehouse compute cost; complex non-SQL logic awkward; raw PII lands in warehouse |
| AWS examples | S3 Bronze → Glue Spark → Silver Parquet/Iceberg | S3 → Redshift COPY → dbt models in Redshift; Athena CTAS/INSERT pipelines; dbt-athena on Iceberg |

**Selection rule:** On a lakehouse, the line blurs: Spark/Glue for Bronze→Silver (parsing, dedupe, CDC apply, PII), SQL ELT (dbt on Athena/Redshift) for Silver→Gold business logic.

**Operational consequences:** ELT concentrates cost in the warehouse (watch concurrency/compute); ETL spreads across engines (more ops, but isolation).

**Limitations:** ELT requires that the target can handle raw data volumes and formats; ETL creates a second copy and potential drift between engines' semantics (e.g., timestamp handling).

---

## 2. Medallion processing responsibilities

| Step | Bronze → Silver | Silver → Gold |
|---|---|---|
| Typing & parsing | Cast to contract types; parse JSON; flatten | — |
| Validation | Contract checks; route failures to quarantine | Business rule checks (totals, referential) |
| Dedupe | By key + version | — |
| CDC apply | MERGE latest state; tombstones | — |
| Conformance | Standard keys, time zones (UTC), codes | Conformed dimensions |
| History | Optional SCD2 at Silver for key entities | SCD2 dimensions, snapshots |
| Aggregation | No (keep grain) | Yes (marts, metrics) |
| PII | Tokenize/mask | Only what's needed |

---

## 3. Joins, aggregations, windows — production notes

- Join on **conformed keys** with explicit types (string vs. bigint mismatch causes silent no-match or implicit casts that kill pushdown).
- Handle **null join keys** explicitly (filter or default key).
- For time-dependent joins (fact to SCD2 dim): `fact.event_ts >= dim.valid_from AND fact.event_ts < dim.valid_to` — **half-open intervals** avoid double matches.
- Aggregations on skewed keys: two-phase aggregation or pre-aggregation.
- Window functions partition by keys with bounded group sizes; a window over a single huge partition (no `PARTITION BY`) runs on one task.

---

## 4. Applying CDC

### 4.1 Input contract (from DMS-style Bronze)

| Column | Meaning |
|---|---|
| `op` | `I`, `U`, `D` |
| `commit_ts` | Source commit timestamp |
| `seq` | Monotonic tie-breaker (LSN/SCN/transaction sequence) |
| business columns | Full row image (after image; for deletes may be key-only) |

### 4.2 SQL (Iceberg MERGE — Spark SQL/Athena)

```sql
-- Runs in: Spark SQL with Iceberg catalog (EMR/Glue) or Athena (Iceberg). Requires AWS for Glue catalog.
WITH latest AS (
  SELECT * FROM (
    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY commit_ts DESC, seq DESC) AS rn
    FROM bronze_orders_cdc c
    WHERE batch_id = '2026-10-10T01'
  ) x WHERE rn = 1
)
MERGE INTO silver.orders t
USING latest s
ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND (s.commit_ts, s.seq) > (t.commit_ts, t.seq) THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'D' THEN INSERT *;
```

Notes:
- `UPDATE SET *` / `INSERT *` are Spark SQL Iceberg shortcuts (columns must match; `rn`/`op` must be excluded or present in target). Athena requires explicit column lists.
- Row-value comparison `(a, b) > (c, d)` support varies; expand to `s.commit_ts > t.commit_ts OR (s.commit_ts = t.commit_ts AND s.seq > t.seq)` for portability.
- The **guard on ordering** prevents an older replayed change from overwriting newer state (idempotent under replays).

### 4.3 PySpark (Hive-style partition rewrite when no table format)

```python
# Runs in: local PySpark / EMR / Glue. Runnable locally (see cookbook for test data).
from pyspark.sql import functions as F, Window

def apply_cdc(current, changes, key="order_id"):
    """current: state table incl. commit_ts, seq. changes: CDC rows incl. op, commit_ts, seq."""
    w = Window.partitionBy(key).orderBy(F.col("commit_ts").desc(), F.col("seq").desc())
    cur = current.withColumn("op", F.lit("I"))          # existing state behaves like an old insert
    unioned = cur.unionByName(changes, allowMissingColumns=True)
    resolved = unioned.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    return resolved.filter(F.col("op") != "D").drop("op")
```

This rewrites the whole table (or affected partitions) — acceptable for moderate sizes; use Iceberg MERGE for large tables.

---

## 5. Slowly Changing Dimensions

| Type | Behavior | Use |
|---|---|---|
| SCD0 | Never change | Immutable attributes |
| **SCD1** | Overwrite | Corrections, attributes where history is irrelevant |
| **SCD2** | New row per change with `valid_from`, `valid_to`, `is_current` | Historical accuracy (customer tier at time of order) |
| SCD3 | Previous value column | Limited history |
| SCD4/6 | History table / hybrid | Mixed needs |

### 5.1 SCD2 with hash-diff (Spark SQL / Iceberg)

```sql
-- Runs in: Spark SQL with Iceberg (EMR/Glue). Input: stg_customer (one row per customer_id, latest snapshot)
-- Target: dim_customer(customer_sk, customer_id, name, tier, city, row_hash, valid_from, valid_to, is_current)

-- 1) changed or new keys
CREATE OR REPLACE TEMP VIEW changes AS
SELECT s.*, sha2(concat_ws('||', s.name, s.tier, s.city), 256) AS row_hash
FROM stg_customer s
LEFT JOIN dim_customer d ON d.customer_id = s.customer_id AND d.is_current = true
WHERE d.customer_id IS NULL
   OR d.row_hash <> sha2(concat_ws('||', s.name, s.tier, s.city), 256);

-- 2) expire current versions + insert new versions in one MERGE using the "null merge key" trick
MERGE INTO dim_customer t
USING (
  SELECT customer_id AS merge_key, * FROM changes          -- matches current row → expire
  UNION ALL
  SELECT NULL AS merge_key, * FROM changes                 -- never matches → insert new version
) s
ON t.customer_id = s.merge_key AND t.is_current = true
WHEN MATCHED THEN UPDATE SET valid_to = s.effective_ts, is_current = false
WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT
  (customer_sk, customer_id, name, tier, city, row_hash, valid_from, valid_to, is_current)
  VALUES (uuid(), s.customer_id, s.name, s.tier, s.city, s.row_hash, s.effective_ts, TIMESTAMP '9999-12-31 00:00:00', true);
```

Assumes `stg_customer` has `effective_ts` (source change time). Pitfalls:
- `concat_ws` skips nulls → `('a', null)` and `(null, 'a')` hash the same. Use `coalesce(col, '<NULL>')` per column.
- Multiple changes per key in one batch: process in order or collapse to the latest unless every intermediate version must be kept (then generate versions with `LEAD()`).
- Late-arriving changes (effective_ts older than current version's valid_from) require **inserting in the middle** of history and re-chaining `valid_to` — design for it or reject.

Plain-SQL version for warehouses without MERGE semantics is in [17](17-sql-for-data-engineering-interviews.md); runnable PySpark SCD2 in [31](31-python-sql-pyspark-cookbook.md).

---

## 6. Incremental transformations

| Technique | How |
|---|---|
| Partition-scoped recompute | Recompute only partitions touched by new data (track affected `event_date`s) |
| Iceberg incremental read | Read appended data between snapshots (`start-snapshot-id`, `end-snapshot-id`) — appends only |
| Change-aware MERGE | MERGE only changed keys |
| dbt incremental models | `is_incremental()` with `unique_key` and lookback filter |
| Materialized views | Redshift incremental refresh where supported |

**Interview trap:** Incremental aggregates ("sum today's new rows and add to yesterday's total") break with late data and corrections. Prefer recomputing affected partitions from Silver.

---

## 7. Data contracts and quality inside transforms

- Validate **before** publish: row counts vs. input, null/unique on keys, referential integrity to dims, distribution checks.
- Fail vs. quarantine policy per rule severity (blocker → fail run; warning → quarantine rows + alert).
- Record DQ metrics per run into a results table (trend detection).

---

## 8. Error quarantine pattern

```python
# Runs in: any PySpark. Runnable locally.
from pyspark.sql import functions as F

def split_valid_invalid(df):
    rules = {
        "order_id_null": F.col("order_id").isNull(),
        "amount_negative": F.col("amount") < 0,
        "bad_status": ~F.col("status").isin("PLACED", "SHIPPED", "CANCELLED", "RETURNED"),
    }
    reasons = F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()]))
    tagged = df.withColumn("_dq_errors", reasons)
    valid = tagged.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
    invalid = tagged.filter(F.size("_dq_errors") > 0).withColumn("_quarantined_at", F.current_timestamp())
    return valid, invalid
```

(`array_compact` requires Spark 3.4+; on older versions use `F.filter(arr, lambda x: x.isNotNull())`.) Note `isin` on null status yields null → `~null` is null → `F.when(null)` doesn't fire: null status passes this rule; add an explicit `isNull()` check if required.

Quarantine table: original record + `_dq_errors` + `_source_object` + `_batch_id` → stewards fix upstream → reprocess by batch.

---

## 9. Performance, cost, security implications

| Area | Note |
|---|---|
| Performance | Filter and project early; avoid wide row explosions; broadcast small dims; sort/cluster output for downstream pruning |
| Cost | Incremental over full; avoid recomputing Gold from Bronze daily; compact outputs |
| Security | Apply masking/tokenization in Silver; separate PII columns into restricted tables; Lake Formation column filters |
| Reliability | Deterministic transforms (no `current_timestamp()` in business logic without parameter; seeded randomness) |

---

## 10. Interview questions and model answers

**Q1. Your SCD2 dimension has two current rows for some customers. Causes?**
Concurrent runs; batch containing multiple changes per key inserted without collapsing; hash including volatile columns; MERGE condition not restricting `is_current = true`; non-atomic expire/insert steps where one failed. Fix: single atomic MERGE, collapse per key, run-level locking, uniqueness check (`count(*) where is_current group by key having count > 1`) as a DQ blocker.

**Q2. ETL or ELT for a team with dbt skills and Redshift?**
ELT for Silver→Gold in dbt on Redshift (or Spectrum), keep Spark/Glue for Bronze parsing and CDC apply if volumes are high. Monitor Redshift concurrency for transformation workloads; separate workgroups.

**Q3. How do you make transformations deterministic?**
Pass logical date/run parameters; no wall-clock in logic; stable tie-breakers in `row_number`; avoid non-deterministic UDFs; fixed seeds; consistent time zones.

**Q4. Fact arrives with a customer_id not in the dim. What do you do?**
Inferred member: insert a placeholder dim row (flags `is_inferred`), link the fact; when the real record arrives, SCD1-update the placeholder attributes (keep the surrogate key).

**Q5. Why does incremental aggregation drift?**
Late/corrected data and deletes aren't reflected in previously added totals. Recompute affected partitions instead.

---

## 11. Self-check

1. **Q:** Why the ordering guard in CDC MERGE? **A:** To prevent replayed older changes from overwriting newer state.
2. **Q:** Why half-open intervals in SCD2 joins? **A:** To prevent a timestamp at a boundary matching two versions.
3. **Q:** Pitfall of `concat_ws` hashing? **A:** It ignores nulls, so different null patterns can collide.
4. **Q:** When is ELT a poor fit? **A:** Complex non-SQL processing, raw PII must not land in the warehouse, or warehouse cost/concurrency constraints.
5. **Q:** How do you handle a CDC batch with 3 updates to the same key? **A:** Collapse to the latest by commit sequence (or keep all if building history with ordered versions).
6. **Q:** What goes into a quarantine record? **A:** Original data, error reasons, source object/batch identifiers, and quarantine timestamp.
