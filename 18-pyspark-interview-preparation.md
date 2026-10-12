# 18 — PySpark Interview Preparation

> Related: [10 EMR & Spark internals](10-amazon-emr-and-apache-spark.md) · [15 Formats](15-file-formats-and-partitioning.md) · [31 Cookbook](31-python-sql-pyspark-cookbook.md) · [30 Troubleshooting](30-troubleshooting-playbook.md)

All code: **Runs in: local PySpark (3.5+/4.x), notebook, Glue, or EMR**. Exercises X1–X12 are **Runnable** locally with the `spark` session below and were syntax-checked; the cookbook contains an executed subset.

```python
from pyspark.sql import SparkSession, functions as F, Window
spark = (SparkSession.builder.master("local[4]").appName("prep")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
```

---

## 1. Core concepts (fast, precise answers)

| Question | Senior answer |
|---|---|
| Transformations vs. actions | Transformations build a logical plan lazily (`select`, `filter`, `join`, `groupBy`); actions execute (`count`, `collect`, `show`, `write`, `toPandas`). Some APIs trigger jobs eagerly (schema inference on read, `checkpoint()`, sampling for `orderBy` range partitioning). |
| Narrow vs. wide | Narrow: each output partition depends on one input partition (`map`, `filter`, `withColumn`, `union`). Wide: requires shuffle (`groupBy`, `join` non-broadcast, `distinct`, `repartition`, `orderBy`, window with `partitionBy`). |
| Stage boundary | Every shuffle (Exchange) ends a stage. |
| Why DataFrames over RDDs | Catalyst optimization + Tungsten codegen; predicate/column pushdown; Python code runs only in driver to build plans (no per-row Python unless UDFs). |
| `explain()` modes | `simple`, `extended`, `codegen`, `cost`, `formatted`. |

**Interview trap:** `df.count()` after `df.cache()` materializes the cache; `df.cache()` alone does nothing.

---

## 2. Execution plans — what to look for

```python
orders = spark.createDataFrame([(1, 10, 100.0, "2026-10-01"), (2, 11, 50.0, "2026-10-02")],
                               "order_id INT, customer_id INT, amount DOUBLE, dt STRING")
customers = spark.createDataFrame([(10, "gold"), (11, "silver")], "customer_id INT, tier STRING")
q = (orders.filter(F.col("amount") > 20)
           .join(customers, "customer_id")
           .groupBy("tier").agg(F.sum("amount").alias("rev")))
q.explain("formatted")
```

Look for: `BroadcastHashJoin` vs. `SortMergeJoin`; `Exchange hashpartitioning(...)` (shuffles); `PushedFilters` / `PartitionFilters` in file scans; `HashAggregate` partial + final (map-side combine); `AdaptiveSparkPlan`.

---

## 3. Comparisons

### Repartition vs. coalesce

See [10 §3.1](10-amazon-emr-and-apache-spark.md). Rule: `repartition` to increase/rebalance or partition by columns before write; `coalesce` to reduce partitions at the end of a cheap narrow pipeline.

### Broadcast vs. sort-merge

See [10 §4.1](10-amazon-emr-and-apache-spark.md). Rule: broadcast when the small side's **in-memory** size comfortably fits on every executor and driver.

### Cache vs. checkpoint

See [10 §7](10-amazon-emr-and-apache-spark.md). Rule: cache for reuse; checkpoint to cut lineage.

---

## 4. UDFs

| Type | Performance | Use |
|---|---|---|
| Built-in functions | Best (JVM, codegen) | Always first choice |
| SQL higher-order functions (`transform`, `filter`, `aggregate`, `exists`) | JVM | Array/map manipulation |
| Pandas UDF (vectorized, Arrow) | Good | Complex numeric/Python logic in batches |
| Python UDF (row-at-a-time) | Worst (serialization per row; opaque to optimizer) | Last resort |
| Arrow-optimized Python UDFs (`useArrow=True`, Spark 3.5+) | Better than classic | When pandas UDF semantics not suitable |
| Scala/Java UDF | Good | Performance-critical custom logic |

**Interview trap:** UDFs block predicate pushdown and are black boxes to Catalyst (no constant folding, no null propagation optimization).

---

## 5. Exercises with solutions

### X1 — Top-N per group (window)

```python
sales = spark.createDataFrame(
    [("A", "p1", 100), ("A", "p2", 300), ("A", "p3", 200), ("B", "p4", 50), ("B", "p5", 50)],
    "category STRING, product STRING, revenue INT")
w = Window.partitionBy("category").orderBy(F.desc("revenue"), F.asc("product"))
top2 = sales.withColumn("rn", F.row_number().over(w)).filter("rn <= 2").drop("rn")
assert sorted(map(tuple, top2.collect())) == [("A", "p2", 300), ("A", "p3", 200), ("B", "p4", 50), ("B", "p5", 50)]
```

Follow-ups: ties (`dense_rank`), very large groups (window on one huge partition → skew; pre-filter with approx quantile threshold).

### X2 — Deduplicate latest by key

```python
cdc = spark.createDataFrame(
    [(1, "a", "2026-10-01 10:00:00", 1), (1, "b", "2026-10-01 11:00:00", 2), (2, "c", "2026-10-01 09:00:00", 1)],
    "id INT, val STRING, ts STRING, seq INT").withColumn("ts", F.to_timestamp("ts"))
w = Window.partitionBy("id").orderBy(F.col("ts").desc(), F.col("seq").desc())
latest = cdc.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
assert sorted((r.id, r.val) for r in latest.collect()) == [(1, "b"), (2, "c")]
```

Alternative with less shuffle memory: `groupBy("id").agg(F.max_by("val", F.struct("ts", "seq")))` (Spark 3.3+) or `F.max(F.struct("ts","seq","val"))` then unpack.

### X3 — Broadcast join and verifying the plan

```python
big = spark.range(0, 100_000).withColumn("k", (F.col("id") % 100).cast("int"))
small = spark.createDataFrame([(i, f"name{i}") for i in range(100)], "k INT, name STRING")
joined = big.join(F.broadcast(small), "k")
plan = joined._jdf.queryExecution().executedPlan().toString()
assert "BroadcastHashJoin" in plan
```

(`_jdf` is an internal API — fine for a local check; in interviews, cite `explain()`.)

### X4 — Skewed join with salting

```python
skewed = (spark.range(0, 200_000)
          .withColumn("k", F.when(F.col("id") % 10 < 8, F.lit(0)).otherwise(F.col("id") % 50).cast("int")))
dim = spark.createDataFrame([(i, f"d{i}") for i in range(50)], "k INT, attr STRING")

N = 8
salted_fact = skewed.withColumn("salt", (F.rand(seed=42) * N).cast("int"))
salted_dim = dim.crossJoin(spark.range(N).withColumnRenamed("id", "salt").withColumn("salt", F.col("salt").cast("int")))
res = salted_fact.join(salted_dim, ["k", "salt"]).drop("salt")
assert res.count() == skewed.count()
```

Follow-ups: when AQE skew join suffices (`spark.sql.adaptive.skewJoin.enabled`), salting only hot keys to avoid N× dim duplication, aggregation skew → two-phase aggregation.

### X5 — Two-phase aggregation for skewed groupBy

```python
N = 8
partial = (skewed.withColumn("salt", (F.rand(seed=1) * N).cast("int"))
                 .groupBy("k", "salt").agg(F.count("*").alias("c")))
final = partial.groupBy("k").agg(F.sum("c").alias("c"))
assert final.filter("k = 0").first().c == skewed.filter("k = 0").count()
```

Note: Spark already does map-side partial aggregation for `count/sum`; salting matters for non-combinable or heavy aggregations (e.g., `collect_list`, distinct counts) and extreme key skew.

### X6 — Nested JSON: explode and flatten

```python
import json
raw = [json.dumps({"order_id": 1, "customer": {"id": 7, "geo": {"country": "IN"}},
                   "items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}]}),
       json.dumps({"order_id": 2, "customer": {"id": 8, "geo": None}, "items": []})]
schema = ("order_id INT, customer STRUCT<id: INT, geo: STRUCT<country: STRING>>, "
          "items ARRAY<STRUCT<sku: STRING, qty: INT>>")
df = spark.read.schema(schema).json(spark.sparkContext.parallelize(raw))
flat = (df.select("order_id", F.col("customer.id").alias("customer_id"),
                  F.col("customer.geo.country").alias("country"),
                  F.explode_outer("items").alias("item"))
          .select("*", "item.sku", "item.qty").drop("item"))
rows = sorted(map(tuple, flat.collect()), key=lambda r: (r[0], r[3] or ""))
assert rows == [(1, 7, "IN", "A", 2), (1, 7, "IN", "B", 1), (2, 8, None, None, None)]
```

`explode` drops rows with empty/null arrays; `explode_outer` keeps them. Explicit schema avoids an inference pass and type drift. Reading JSON from an RDD of strings is convenient for tests; in production read files (`spark.read.schema(schema).json("s3://...")`).

### X7 — Sessionization in PySpark

```python
ev = spark.createDataFrame(
    [(1, "2026-10-01 10:00:00"), (1, "2026-10-01 10:10:00"), (1, "2026-10-01 11:00:00"), (2, "2026-10-01 09:00:00")],
    "user_id INT, ts STRING").withColumn("ts", F.to_timestamp("ts"))
w = Window.partitionBy("user_id").orderBy("ts")
sess = (ev.withColumn("gap", F.col("ts").cast("long") - F.lag(F.col("ts").cast("long")).over(w))
          .withColumn("new", F.when(F.col("gap").isNull() | (F.col("gap") > 1800), 1).otherwise(0))
          .withColumn("session_id", F.sum("new").over(w.rowsBetween(Window.unboundedPreceding, 0))))
out = sess.groupBy("user_id", "session_id").agg(F.count("*").alias("n")).orderBy("user_id", "session_id")
assert [tuple(r) for r in out.collect()] == [(1, 1, 2), (1, 2, 1), (2, 1, 1)]
```

Spark 3.2+ also offers `F.session_window("ts", "30 minutes")` for streaming/batch aggregations.

### X8 — Incremental processing with a watermark column

```python
def incremental_slice(df, wm_old: str, wm_new: str, lookback_minutes: int = 30):
    lower = F.to_timestamp(F.lit(wm_old)) - F.expr(f"INTERVAL {lookback_minutes} MINUTES")
    return df.filter((F.col("updated_at") > lower) & (F.col("updated_at") <= F.to_timestamp(F.lit(wm_new))))

src = spark.createDataFrame([(1, "2026-10-10 00:50:00"), (2, "2026-10-10 01:20:00"), (3, "2026-10-10 02:10:00")],
                            "id INT, updated_at STRING").withColumn("updated_at", F.to_timestamp("updated_at"))
got = [r.id for r in incremental_slice(src, "2026-10-10 01:00:00", "2026-10-10 02:00:00").orderBy("id").collect()]
assert got == [1, 2]     # id 1 included via lookback overlap; dedupe/merge handles reprocessing
```

### X9 — Dynamic partition overwrite (idempotent reload)

```python
import tempfile, os
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
path = os.path.join(tempfile.mkdtemp(), "tbl")
d1 = spark.createDataFrame([(1, "2026-10-01"), (2, "2026-10-02")], "id INT, dt STRING")
d1.write.mode("overwrite").partitionBy("dt").parquet(path)
fix = spark.createDataFrame([(20, "2026-10-02")], "id INT, dt STRING")
fix.write.mode("overwrite").partitionBy("dt").parquet(path)   # only dt=2026-10-02 replaced
res = sorted((r.id, r.dt) for r in spark.read.parquet(path).collect())
assert res == [(1, "2026-10-01"), (20, "2026-10-02")]
```

With `static` mode (default), the second write would delete `dt=2026-10-01` too. **Interview trap.**

### X10 — Controlling output file count

```python
events = spark.range(0, 100_000).withColumn("dt", F.concat(F.lit("2026-10-0"), (F.col("id") % 3 + 1).cast("string")))
out = os.path.join(tempfile.mkdtemp(), "events")
(events.repartition("dt")                     # one shuffle partition per dt value (hash) → ~1 file per dt
       .write.option("maxRecordsPerFile", 40_000)
       .partitionBy("dt").parquet(out))
n_files = sum(1 for root, _, files in os.walk(out) for f in files if f.endswith(".parquet"))
assert 3 <= n_files <= 6
```

### X11 — Caching vs. checkpointing a reused, long-lineage DataFrame

```python
spark.sparkContext.setCheckpointDir(tempfile.mkdtemp())
base = spark.range(0, 10_000)
for i in range(30):                                      # simulate iterative lineage growth
    base = base.withColumn("id", F.col("id") + 1)
cached = base.cache(); cached.count()                    # materialize once, reuse below
a = cached.filter("id % 2 = 0").count(); b = cached.filter("id % 3 = 0").count()
cp = base.checkpoint()                                   # eager; truncates lineage
assert "Scan ExistingRDD" in cp._jdf.queryExecution().optimizedPlan().toString() or cp.count() == 10_000
cached.unpersist()
```

Use **localCheckpoint** for speed when fault tolerance across executor loss isn't needed.

### X12 — Large-scale aggregation with approximate distinct

```python
clicks = spark.range(0, 500_000).withColumn("user", (F.col("id") % 12_345)).withColumn("page", F.col("id") % 7)
agg = clicks.groupBy("page").agg(F.approx_count_distinct("user", rsd=0.02).alias("users"),
                                 F.count("*").alias("hits"))
exact = clicks.groupBy("page").agg(F.countDistinct("user").alias("u")).orderBy("page").collect()
approx = agg.orderBy("page").collect()
assert all(abs(a.users - e.u) / e.u < 0.06 for a, e in zip(approx, exact))
```

HyperLogLog++ trades exactness for bounded memory and no full distinct shuffle.

---

## 6. Performance diagnosis drills

| Symptom in Spark UI | Interpretation | Action |
|---|---|---|
| One task 50× median duration, large shuffle read | Skew | AQE skew join, salting, isolate hot keys |
| Thousands of tasks of ~1 MB each | Too many partitions / small files | Coalesce (AQE), compact input, raise `maxPartitionBytes` |
| High "Spill (Memory/Disk)" | Partitions too large for execution memory | More partitions, larger executors, reduce row width |
| High GC time (> ~10% of task time) | Object churn, small heaps with many cores | Fewer cores per executor, more memory, avoid UDF objects |
| Stage with 1 task | `coalesce(1)`, window without `partitionBy`, global sort final stage, single gzip file | Remove bottleneck |
| Long scheduler delay | Driver overloaded, too many tasks | Fewer tasks, bigger driver |
| Executors lost repeatedly | OOM (overhead), Spot interruption | Check container exit codes, overhead, Spot handling |
| Job slow before any task runs | Listing millions of S3 files, schema inference, huge plans | Table formats/manifests, explicit schemas, checkpoint plans |

### Memory and shuffle failure interview script

> "First I'd confirm *where* it fails: driver or executor, which stage, and the exact error. `Container killed by YARN for exceeding memory limits` means off-heap overhead — Python workers or Netty buffers — so I raise `memoryOverhead` or reduce cores per executor. `Java heap space` on an executor in a shuffle stage with one giant task means skew — I fix data distribution rather than adding memory. `FetchFailedException` means the executor holding shuffle output died — I look for the earlier root cause (OOM, Spot loss) in the executors tab. Driver OOM is almost always a `collect`, `toPandas`, a big broadcast, or a gigantic plan."

---

## 7. Interview questions and model answers

**Q1. Why might `df.join(broadcast(dim), ...)` OOM even though `dim` is 300 MB on S3?**
Parquet compresses heavily; in memory (deserialized, Java objects/UnsafeRows + hash table) it might be several GB, collected on the driver and copied to each executor.

**Q2. What happens when you call `df.write.partitionBy("dt")` without repartitioning on 2,000 shuffle partitions containing all 30 dates?**
Up to 2,000 × 30 = 60,000 small files.

**Q3. How do you choose `spark.sql.shuffle.partitions`?**
Total shuffle bytes ÷ target 128–200 MB, ≥ 2–3× total cores; or set high and let AQE coalesce to the advisory size.

**Q4. Explain AQE's three main optimizations.**
Coalesce post-shuffle partitions; convert sort-merge to broadcast join at runtime; split skewed partitions in joins.

**Q5. When would you use `mapInPandas` or `applyInPandas`?**
Group-wise or batch-wise Python logic needing pandas/numpy (e.g., per-device model scoring), with Arrow batches; size groups to fit worker memory.

**Q6. Why does `orderBy` trigger an extra job?**
Range partitioning samples the data to compute partition boundaries before the shuffle.

---

## 8. Self-check

1. **Q:** Is `union` narrow or wide? **A:** Narrow (no shuffle; partitions concatenated).
2. **Q:** `explode` vs. `explode_outer`? **A:** `explode` drops null/empty arrays; `explode_outer` emits a row with nulls.
3. **Q:** What config prevents a partition overwrite from deleting other partitions? **A:** `spark.sql.sources.partitionOverwriteMode=dynamic`.
4. **Q:** Why prefer built-in functions to Python UDFs? **A:** They run in the JVM with codegen and are visible to the optimizer; Python UDFs serialize every row and block pushdown.
5. **Q:** What does `approx_count_distinct(rsd=0.02)` trade? **A:** Exactness for bounded memory and speed (~2% relative standard deviation).
6. **Q:** What's the most likely cause of one extremely slow task in a join stage? **A:** A hot key (skew), including null keys.
