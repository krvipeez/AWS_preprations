# 15 — File Formats and Partitioning

> Related: [03 S3](03-amazon-s3.md) · [05 Athena](05-amazon-athena.md) · [10 Spark](10-amazon-emr-and-apache-spark.md) · [02 Lakehouse](02-data-lake-architecture.md)

---

## 1. Row vs. columnar storage

| Dimension | Row-oriented (CSV, JSON, Avro) | Columnar (Parquet, ORC) |
|---|---|---|
| Layout | All columns of a row together | Values of a column stored together in chunks |
| Read few columns | Must read entire rows | Reads only needed column chunks |
| Compression | Lower (mixed types adjacent) | High (similar values adjacent; dictionary, RLE, bit-packing) |
| Write cost | Cheap, streaming-friendly | Buffered (row groups), more CPU |
| Point lookups / whole-record access | Good | Worse |
| Best for | Ingestion/landing, streaming, CDC records, message payloads | Analytics, scans, aggregations |

---

## 2. Format internals

### 2.1 Parquet

```text
File
 ├─ Row group 1 (typically 128 MB–1 GB uncompressed target; parquet.block.size default 128 MB in parquet-mr)
 │    ├─ Column chunk A → pages (data pages ~1 MB, dictionary page)
 │    ├─ Column chunk B → pages
 ├─ Row group 2 ...
 └─ Footer: schema, row group metadata, column chunk offsets, statistics (min/max/null count), optional bloom filters / page indexes
```

- **Predicate pushdown**: readers skip row groups (and pages with column indexes) whose min/max can't match. Effective only if data is **clustered/sorted** on the filter column.
- **Dictionary encoding** for low-cardinality strings; falls back to plain when dictionary grows too large.
- Nested types via Dremel encoding (repetition/definition levels).
- Schema evolution: add columns (readers fill null); rename is by-name problematic; type changes limited.

### 2.2 ORC

- Stripes (default 64 MB), with lightweight indexes every 10,000 rows (row index), file and stripe statistics, optional bloom filters.
- Strong in Hive ecosystems; ACID Hive tables; similar performance to Parquet in Athena/Trino.

### 2.3 Avro

- Row-oriented binary, schema stored in file header (JSON schema), blocks with sync markers → **splittable**.
- Excellent **schema evolution** with reader/writer schema resolution (defaults for new fields).
- Default choice for Kafka/Kinesis payloads with a schema registry, and for CDC landing.

### 2.4 JSON / CSV

- Human-readable, no embedded schema (CSV) or self-describing per record (JSON).
- Text parsing cost is high; types ambiguous; CSV quoting/escaping/multiline issues.
- Splittable only when uncompressed or with splittable codec (bzip2) — **gzip is not splittable**.

---

## 3. Parquet vs. ORC vs. Avro vs. JSON

| Dimension | Parquet | ORC | Avro | JSON |
|---|---|---|---|---|
| Orientation | Columnar | Columnar | Row | Row (text) |
| Compression ratio | High | High | Medium | Low (unless compressed) |
| Predicate pushdown | Row group / page stats, bloom filters | Stripe / row-index stats, bloom filters | No | No |
| Schema evolution | Add columns; by-name mapping | Add columns; positional history in Hive | **Best** (reader/writer schemas, defaults) | Ad hoc |
| Nested data | Yes | Yes | Yes | Yes |
| Splittable | Yes | Yes | Yes | Only uncompressed/line-delimited |
| Write speed | Slower | Slower | Fast | Fast |
| Ecosystem | Universal (Spark, Athena, Redshift, Snowflake, pandas, DuckDB) | Hive/Trino/Spark | Kafka, streaming, CDC | Everywhere |
| Best for | Lake Silver/Gold, Iceberg default | Hive-centric stacks | Streaming, CDC landing, row-level exchange | APIs, logs at landing, debugging |

**Selection rule:** Parquet for analytics tables (Iceberg default). Avro for streaming/CDC transport and Bronze when schema evolution of row records matters. JSON only at the edges.

**Operational consequences:** Columnar writes need memory buffers (row groups) → streaming writers produce small files unless compacted. Avro needs schema management.

**Limitations:** Parquet column renames are fragile without a table format; JSON type inference is unstable.

### 3.1 CSV vs. columnar storage

| Dimension | CSV | Parquet/ORC |
|---|---|---|
| Scan bytes for 3 of 50 columns | 100% of file | ~6% + metadata |
| Types | Strings, inferred | Typed, enforced |
| Nulls vs. empty strings | Ambiguous | Explicit |
| Athena cost | Full scan | Fraction |
| Robustness | Delimiter/quote/newline bugs | Binary, robust |
| Use | Partner exchange, spreadsheets | Everything analytical |

**Selection rule:** Convert CSV to Parquet at the first transformation step; keep original CSV in Bronze only for audit/replay.

---

## 4. Compression codecs

| Codec | Ratio | Speed | Splittable (text files) | Typical use |
|---|---|---|---|---|
| Snappy | Medium | Very fast | No (but Parquet/ORC/Avro compress internally per page/block → files remain splittable) | Default for Parquet in Spark |
| ZSTD | High | Fast (tunable levels) | No (same note) | Better ratio than Snappy at similar speed; increasingly default choice |
| GZIP | High | Slow | **No** for text files | Legacy/landing; avoid for big CSV/JSON |
| LZ4 | Low–medium | Very fast | No | Shuffle/IO in engines |
| BZIP2 | High | Very slow | Yes | Rare |

**Interview trap:** "Parquet with gzip isn't splittable" — false. Compression inside Parquet is per page/column chunk; Parquet files remain splittable by row group. The non-splittable problem is **gzip-compressed text files** (`.csv.gz`, `.json.gz`): one task reads the whole file.

---

## 5. Partitioning

### 5.1 Partition design rules

1. Partition by columns used in **most** query filters (usually date).
2. Keep partitions **large enough**: each partition should hold at least hundreds of MB to GBs (heuristic); avoid > ~10,000s of partitions per table for Hive-style tables unless using projection/Iceberg.
3. Avoid **high-cardinality** keys (user_id, order_id).
4. Prefer **date** (`dt=YYYY-MM-DD`) over `year/month/day` nesting when queries filter by date ranges — nested columns make range predicates awkward (`year=2026 AND month BETWEEN ...`).
5. Use **event time** for analytics partitions; store ingest time as a column.
6. Consider a second, low-cardinality partition column (region, tenant tier) only if it's always filtered.
7. With Iceberg, use **hidden partitioning** (`days(event_ts)`, `bucket(16, user_id)`) and evolve partitions over time.

### 5.2 Partition pruning

| Engine | Mechanism |
|---|---|
| Athena/Trino | Partition predicates on catalog partitions or projection |
| Spark | `PartitionFilters` in scan; **dynamic partition pruning** (DPP) for star joins (filter on dim pushes into fact scan) |
| Redshift Spectrum | Catalog partitions |
| Iceberg | Manifest-level partition stats + file-level column stats |

### 5.3 Bucketing / clustering

- Hive/Spark bucketing: hash into N files per partition; enables bucket joins without shuffle **if both sides bucketed identically and the engine supports it** — brittle across engines.
- Iceberg `bucket(N, col)` partition transform; sort orders (`WRITE ORDERED BY`) for clustering → better min/max skipping.
- Z-ordering style clustering for multi-column filters (engine/format dependent).

---

## 6. File sizing and the small-files problem

**Interview trap: Small files can degrade Spark and Athena performance.**

Why:
- Per-file overhead: S3 GET/LIST requests, open cost (`openCostInBytes`), footer reads.
- Task explosion: one task per small file (or per bin-packed group).
- Catalog/metadata bloat: more manifest entries, partition listing.
- Poor compression and stats effectiveness.

Causes: streaming micro-batches, high `shuffle.partitions` with `partitionBy` (each task writes one file **per partition value it holds** → files = tasks × partition values), Firehose short buffers, Lambda per-event writes, over-partitioning.

Fixes:

| Fix | How |
|---|---|
| Repartition before write | `df.repartition("dt").write.partitionBy("dt")` → ~one file per partition (beware huge partitions; add `maxRecordsPerFile`) |
| Target file size | `spark.sql.files.maxRecordsPerFile`; AQE advisory partition size |
| Compaction job | Periodically rewrite partitions into 256 MB–1 GB files |
| Iceberg maintenance | `rewrite_data_files` (Spark procedure), Athena `OPTIMIZE`, automatic compaction for Glue Catalog Iceberg tables `[Optional integration]` |
| Ingestion buffering | Larger Firehose buffers; micro-batch triggers less frequent |
| Read-side grouping | Glue `groupFiles`, Spark `maxPartitionBytes` bin-packing |

Target: **128 MB–1 GB per file** for analytics (common engineering guidance consistent with AWS Athena tuning guidance). Smaller is OK for small tables; avoid many < 16 MB files.

---

## 7. Schema evolution summary

| Change | Parquet (Hive table) | Avro | Iceberg |
|---|---|---|---|
| Add nullable column | OK (by name) | OK with default | OK |
| Drop column | Readers ignore | OK if reader schema omits | OK (metadata) |
| Rename | Breaks by-name readers | Aliases | OK (column IDs) |
| Widen type (int→long) | Engine-dependent | Promotions allowed (int→long, float→double) | Allowed promotions |
| Reorder | OK by name; positional readers break | OK | OK |

---

## 8. Predicate pushdown — what actually gets pushed

- Comparisons with literals on columns (`=`, `<`, `>`, `IN`, `IS NULL`) on supported types.
- Not pushed: predicates wrapped in UDFs, casts on the column side (`CAST(col AS string) = '5'`), many functions, complex expressions.
- Check `PushedFilters` in Spark plans; in Athena use `EXPLAIN`.
- Pushdown only helps if stats are selective → **sort data on common filters** within partitions.

---

## 9. Interview questions and model answers

**Q1. A Spark job writes 50,000 files per day into a date-partitioned table. Why and how to fix?**
`shuffle.partitions` (e.g., 2,000) × partition values present in each task (e.g., 25 hour or region values) → many files. Fix: `repartition(partition_cols)` or by a computed bucket to control file count, AQE coalescing, `maxRecordsPerFile`, compaction.

**Q2. Should Bronze be Parquet or the original JSON?**
Keep the original for fidelity if contracts/audits require it (cheap with lifecycle tiering); add a typed Parquet/Avro copy (or move directly to Silver). The decision hinges on replay needs and whether parsing can lose information.

**Q3. Why might Parquet predicate pushdown not reduce scan size?**
Data not clustered on the filter column (every row group's min/max spans the full range), predicate not pushable (function/cast), or statistics missing for that type.

**Q4. Partition by `customer_id` for a customer-facing API?**
No for Hive-style lakes (millions of tiny partitions). Use Iceberg `bucket(N, customer_id)` + sort, or serve lookups from a key-value store (DynamoDB) or Redshift.

**Q5. ZSTD vs. Snappy?**
ZSTD gives better compression at comparable decode speed — lower storage and scan cost (Athena bytes). Snappy is slightly faster to write/read and is the historical default. Benchmark on your data.

---

## 10. Self-check

1. **Q:** Is gzip-compressed Parquet splittable? **A:** Yes; compression is internal per page; row groups remain splittable. Gzipped CSV is not.
2. **Q:** What determines how many files Spark writes with `partitionBy`? **A:** Each task writes a file per partition value it contains → tasks × values (bounded by `maxRecordsPerFile`).
3. **Q:** Why does sorting within partitions help Athena? **A:** Tighter min/max statistics per row group allow skipping.
4. **Q:** Best format for CDC transport with schema evolution? **A:** Avro (with a schema registry).
5. **Q:** Two signs of over-partitioning? **A:** Average file size well below ~100 MB; partition counts in the hundreds of thousands with slow planning/listing.
6. **Q:** What is dynamic partition pruning? **A:** Spark pushes filter results from a dimension side into the fact table scan's partition filters at runtime.
