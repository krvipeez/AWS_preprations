# 17 — SQL for Data Engineering Interviews

> Related: [06 Redshift](06-amazon-redshift.md) · [05 Athena](05-amazon-athena.md) · [14 SCD/CDC](14-data-transformation-and-processing.md) · [31 Cookbook (runnable SQLite harness)](31-python-sql-pyspark-cookbook.md)

**Dialect note:** Solutions use ANSI SQL with window functions, runnable on PostgreSQL, Redshift, Athena/Trino, Spark SQL, and Snowflake with minor changes. Dialect-specific functions are flagged. Exercises S1–S10 marked **(SQLite-tested)** were executed against SQLite 3 via the harness in [31](31-python-sql-pyspark-cookbook.md).

---

## 1. Concepts senior interviewers probe

### 1.1 Logical query processing order

`FROM/JOIN → WHERE → GROUP BY → HAVING → window functions → SELECT → DISTINCT → ORDER BY → LIMIT/OFFSET`

Consequences: you can't filter on a window function in `WHERE`. Wrap it in a CTE/subquery (portable), or use `QUALIFY` where supported (Snowflake, Databricks, BigQuery, and Redshift in recent versions; Athena/Trino historically no — verify for your engine version).

### 1.2 Joins

| Join | Note |
|---|---|
| INNER | Matching keys only |
| LEFT | All left rows; **filters on right table in `WHERE` turn it into an inner join** — put them in `ON` |
| FULL OUTER | Reconciliation (source vs. target diffs) |
| CROSS | Cartesian; date spines × entities |
| SEMI (`EXISTS`/`IN`) | Existence without duplicating left rows |
| ANTI (`NOT EXISTS`) | Missing records; **`NOT IN` with a NULL in the subquery returns no rows** |
| Self join | Hierarchies, comparisons within table |

**Interview trap:** Join fan-out. Joining orders to a non-unique dimension duplicates order rows → inflated `SUM(amount)`. Always know the grain of each side.

### 1.3 NULL semantics

- `NULL = NULL` is unknown; use `IS NULL` / `IS NOT DISTINCT FROM`.
- `COUNT(col)` skips nulls; `COUNT(*)` doesn't.
- `AVG` ignores nulls (not zero).
- `NOT IN (subquery with NULL)` → empty.

### 1.4 Window functions

`ROW_NUMBER`, `RANK` (gaps), `DENSE_RANK` (no gaps), `NTILE`, `LAG/LEAD`, `FIRST_VALUE/LAST_VALUE` (watch default frame!), `SUM() OVER`.

**Interview trap:** Default frame with `ORDER BY` is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — ties on the ordering column are summed together in running totals, and `LAST_VALUE` returns the current row's peer, not the partition's last. Use `ROWS BETWEEN ...` explicitly.

---

## 2. Exercise set

Schema used across exercises:

```sql
CREATE TABLE employees (emp_id INT, name VARCHAR(50), dept VARCHAR(20), salary INT, manager_id INT);
CREATE TABLE orders (order_id INT, customer_id INT, order_ts TIMESTAMP, amount DECIMAL(12,2), status VARCHAR(20));
CREATE TABLE events (user_id INT, event_ts TIMESTAMP, event_type VARCHAR(20));
CREATE TABLE logins (user_id INT, login_date DATE);
```

### S1 — Second-highest salary per department (SQLite-tested)

```sql
WITH ranked AS (
  SELECT dept, name, salary,
         DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
  FROM employees
)
SELECT dept, name, salary FROM ranked WHERE rnk = 2;
```

- `DENSE_RANK` → "second distinct salary" (ties at first share rank 1). `ROW_NUMBER` → "second row" (arbitrary among ties). Ask which the interviewer means.
- Nth highest: `rnk = :n`. Departments with < n distinct salaries return nothing — if they must return NULL, left join from distinct departments.
- Without windows: `SELECT MAX(salary) FROM employees WHERE salary < (SELECT MAX(salary) FROM employees)`.

### S2 — Deduplicate keeping the latest record (SQLite-tested)

```sql
WITH r AS (
  SELECT o.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_ts DESC, status DESC) AS rn
  FROM orders o
)
SELECT order_id, customer_id, order_ts, amount, status FROM r WHERE rn = 1;
```

Deterministic tie-breaker required. To physically delete duplicates in Redshift/Postgres: create a deduped table and swap, or `DELETE ... USING` with a ctid/row identifier (engine-specific).

### S3 — Running total and 7-day moving average (SQLite-tested)

```sql
WITH daily AS (
  SELECT DATE(order_ts) AS d, SUM(amount) AS revenue
  FROM orders GROUP BY DATE(order_ts)
)
SELECT d, revenue,
       SUM(revenue) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
       AVG(revenue) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7_rows
FROM daily ORDER BY d;
```

**Trap:** `ROWS BETWEEN 6 PRECEDING` = last 7 **rows**, not 7 calendar days if dates are missing. Use a date spine (S9) or `RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW` where supported (PostgreSQL, Trino; not Redshift — verify).

### S4 — Gaps and islands: consecutive login streaks (SQLite-tested)

```sql
WITH d AS (SELECT DISTINCT user_id, login_date FROM logins),
g AS (
  SELECT user_id, login_date,
         DATE(login_date, '-' || ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) || ' day') AS grp
  FROM d
)
SELECT user_id, MIN(login_date) AS streak_start, MAX(login_date) AS streak_end, COUNT(*) AS days
FROM g GROUP BY user_id, grp
HAVING COUNT(*) >= 3
ORDER BY user_id, streak_start;
```

Portable form of `grp`: `login_date - ROW_NUMBER() * INTERVAL '1 day'` (PostgreSQL), `DATEADD(day, -ROW_NUMBER() OVER(...), login_date)` (Redshift/Snowflake), `date_add('day', -ROW_NUMBER() OVER(...), login_date)` (Athena/Trino). The SQLite version uses `DATE(x, '-N day')`.

Why it works: consecutive dates minus consecutive row numbers yield a constant anchor per island. Dedupe first or duplicates break islands.

### S5 — Sessionization: 30-minute inactivity gap (SQLite-tested)

```sql
WITH ordered AS (
  SELECT user_id, event_ts,
         LAG(event_ts) OVER (PARTITION BY user_id ORDER BY event_ts) AS prev_ts
  FROM events
),
flagged AS (
  SELECT user_id, event_ts,
         CASE WHEN prev_ts IS NULL
                OR (julianday(event_ts) - julianday(prev_ts)) * 86400 > 1800 THEN 1 ELSE 0 END AS new_session
  FROM ordered
),
sessions AS (
  SELECT user_id, event_ts,
         SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_ts ROWS UNBOUNDED PRECEDING) AS session_no
  FROM flagged
)
SELECT user_id, session_no, MIN(event_ts) AS start_ts, MAX(event_ts) AS end_ts, COUNT(*) AS events
FROM sessions GROUP BY user_id, session_no ORDER BY user_id, session_no;
```

Portable gap expression: `event_ts - prev_ts > INTERVAL '30' MINUTE` (PostgreSQL/Trino), `DATEDIFF(second, prev_ts, event_ts) > 1800` (Redshift/Snowflake). At scale (Spark), partition by user and handle sessions crossing batch boundaries (carry last event per user).

### S6 — Customers who ordered in every month of 2026 Q3 (relational division) (SQLite-tested)

```sql
SELECT customer_id
FROM orders
WHERE order_ts >= '2026-07-01' AND order_ts < '2026-10-01'
GROUP BY customer_id
HAVING COUNT(DISTINCT strftime('%Y-%m', order_ts)) = 3;
```

Portable: `COUNT(DISTINCT DATE_TRUNC('month', order_ts)) = 3`. Note the half-open date range (sargable, no function on the column in `WHERE`).

### S7 — Month-over-month growth with LAG (SQLite-tested)

```sql
WITH m AS (
  SELECT strftime('%Y-%m', order_ts) AS month, SUM(amount) AS revenue
  FROM orders WHERE status <> 'CANCELLED' GROUP BY 1
)
SELECT month, revenue,
       LAG(revenue) OVER (ORDER BY month) AS prev,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month)) / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 2) AS pct_change
FROM m ORDER BY month;
```

`NULLIF(...,0)` prevents divide-by-zero. Watch `status <> 'CANCELLED'` excluding NULL statuses (use `COALESCE(status,'') <> 'CANCELLED'` if NULLs should count).

### S8 — Manager hierarchy depth (recursive CTE) (SQLite-tested)

```sql
WITH RECURSIVE chain AS (
  SELECT emp_id, name, manager_id, 0 AS depth, CAST(name AS TEXT) AS path
  FROM employees WHERE manager_id IS NULL
  UNION ALL
  SELECT e.emp_id, e.name, e.manager_id, c.depth + 1, c.path || ' > ' || e.name
  FROM employees e JOIN chain c ON e.manager_id = c.emp_id
  WHERE c.depth < 20                      -- cycle/runaway guard
)
SELECT * FROM chain ORDER BY path;
```

Redshift supports recursive CTEs; Athena (Trino) supports `WITH RECURSIVE` with limits — verify engine version.

### S9 — Date spine to fill missing days with zeros (SQLite-tested)

```sql
WITH RECURSIVE spine(d) AS (
  SELECT DATE('2026-10-01')
  UNION ALL SELECT DATE(d, '+1 day') FROM spine WHERE d < DATE('2026-10-07')
),
daily AS (SELECT DATE(order_ts) AS d, SUM(amount) AS revenue FROM orders GROUP BY 1)
SELECT s.d, COALESCE(daily.revenue, 0) AS revenue
FROM spine s LEFT JOIN daily ON daily.d = s.d ORDER BY s.d;
```

Engines: `generate_series` (PostgreSQL), `sequence()` + `UNNEST` (Athena/Trino), a calendar table (Redshift best practice).

### S10 — Top 3 products per category with ties (SQLite-tested pattern)

```sql
WITH sales AS (
  SELECT p.category, p.product_id, SUM(ol.qty * ol.price) AS revenue
  FROM order_lines ol JOIN products p ON p.product_id = ol.product_id
  GROUP BY p.category, p.product_id
),
r AS (SELECT s.*, RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rk FROM sales s)
SELECT * FROM r WHERE rk <= 3;
```

`RANK` may return more than 3 rows when tied; clarify.

### S11 — SCD Type 2 with plain SQL (Redshift/PostgreSQL style, two statements in one transaction)

```sql
BEGIN;

-- 1) Expire current rows whose tracked attributes changed
UPDATE dim_customer d
SET valid_to = s.effective_ts, is_current = FALSE
FROM stg_customer s
WHERE d.customer_id = s.customer_id
  AND d.is_current = TRUE
  AND (d.name IS DISTINCT FROM s.name OR d.tier IS DISTINCT FROM s.tier OR d.city IS DISTINCT FROM s.city);

-- 2) Insert new current versions for changed + brand-new customers
INSERT INTO dim_customer (customer_id, name, tier, city, valid_from, valid_to, is_current)
SELECT s.customer_id, s.name, s.tier, s.city, s.effective_ts, TIMESTAMP '9999-12-31', TRUE
FROM stg_customer s
LEFT JOIN dim_customer d ON d.customer_id = s.customer_id AND d.is_current = TRUE
WHERE d.customer_id IS NULL;      -- no current row: new key, or just expired in step 1

COMMIT;
```

Why step 2 works: after step 1, changed keys have no current row, so the anti-join selects both changed and new keys; unchanged keys still have a current row and are skipped. `IS DISTINCT FROM` handles nulls (Redshift: verify support; otherwise compare hashes with `COALESCE`). Point-in-time join:

```sql
SELECT f.order_id, d.tier
FROM fact_orders f
JOIN dim_customer d
  ON d.customer_id = f.customer_id
 AND f.order_ts >= d.valid_from AND f.order_ts < d.valid_to;
```

### S12 — Incremental load with watermark + MERGE

```sql
-- :wm_old, :wm_new provided by orchestrator (see chapter 13)
MERGE INTO silver_orders t
USING (
  SELECT * FROM (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM staging_orders s
    WHERE updated_at > :wm_old - INTERVAL '30' MINUTE AND updated_at <= :wm_new
  ) x WHERE rn = 1
) s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at >= t.updated_at THEN
  UPDATE SET status = s.status, amount = s.amount, updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (order_id, customer_id, amount, status, updated_at)
  VALUES (s.order_id, s.customer_id, s.amount, s.status, s.updated_at);
```

The lookback overlap + "only newer wins" guard make reruns idempotent.

### S13 — Reconciliation: source vs. target diff

```sql
SELECT COALESCE(s.order_id, t.order_id) AS order_id,
       CASE WHEN t.order_id IS NULL THEN 'MISSING_IN_TARGET'
            WHEN s.order_id IS NULL THEN 'EXTRA_IN_TARGET'
            ELSE 'MISMATCH' END AS issue
FROM src_snapshot s
FULL OUTER JOIN tgt_snapshot t ON s.order_id = t.order_id
WHERE s.order_id IS NULL OR t.order_id IS NULL
   OR s.amount <> t.amount OR s.status <> t.status;
```

For billions of rows: compare aggregates per partition first (counts, sums, hash sums like `SUM(hash(cols))`), then drill into mismatched partitions.

### S14 — Funnel conversion (view → cart → purchase within 1 day)

```sql
WITH firsts AS (
  SELECT user_id,
         MIN(CASE WHEN event_type = 'view' THEN event_ts END) AS t_view,
         MIN(CASE WHEN event_type = 'cart' THEN event_ts END) AS t_cart,
         MIN(CASE WHEN event_type = 'purchase' THEN event_ts END) AS t_buy
  FROM events GROUP BY user_id
)
SELECT COUNT(t_view) AS viewed,
       COUNT(CASE WHEN t_cart >= t_view AND t_cart < t_view + INTERVAL '1' DAY THEN 1 END) AS carted,
       COUNT(CASE WHEN t_buy >= t_cart AND t_buy < t_view + INTERVAL '1' DAY THEN 1 END) AS purchased
FROM firsts;
```

Simplification: uses first occurrence of each event. A strict funnel requires ordered sequence matching per session (`MATCH_RECOGNIZE` in Trino/Snowflake, or window logic).

### S15 — Median and percentiles

```sql
-- PostgreSQL / Redshift (PERCENTILE_CONT is supported in Redshift; as window in some forms)
SELECT dept, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees GROUP BY dept;
-- Athena/Trino
SELECT dept, approx_percentile(salary, 0.5) FROM employees GROUP BY dept;
```

### S16 — Complex business scenario: active subscriptions per day from start/end events

```sql
-- subscriptions(sub_id, start_date, end_date NULL if active)
WITH deltas AS (
  SELECT start_date AS d, 1 AS delta FROM subscriptions
  UNION ALL
  SELECT end_date AS d, -1 FROM subscriptions WHERE end_date IS NOT NULL
),
per_day AS (SELECT d, SUM(delta) AS net FROM deltas GROUP BY d)
SELECT d, SUM(net) OVER (ORDER BY d ROWS UNBOUNDED PRECEDING) AS active_subs
FROM per_day ORDER BY d;
```

Sweep-line technique: O(n log n) vs. joining every subscription to every day (O(n × days)). Fill missing days with a spine.

---

## 3. Query optimization (engine-aware)

| Technique | Why | Engines |
|---|---|---|
| Sargable predicates (`ts >= x AND ts < y`, not `DATE(ts) = x`) | Enables partition pruning / zone maps / index use | All |
| Filter before join, project only needed columns | Less data shuffled/scanned | All |
| Pre-aggregate before joining | Shrinks join inputs | All |
| `EXISTS` instead of `IN` for large subqueries; `NOT EXISTS` instead of `NOT IN` | Correct null semantics, semi/anti join plans | All |
| `UNION ALL` instead of `UNION` when duplicates impossible | Avoids distinct sort | All |
| Avoid `SELECT *` | Column pruning | Columnar (Athena, Redshift, Spark) |
| Approximate aggregates | `approx_distinct`, `APPROXIMATE COUNT(DISTINCT)` | Athena, Redshift |
| Distribution/sort key alignment | Avoid redistribution, enable merge joins | Redshift |
| Broadcast hints | `/*+ BROADCAST(d) */` | Spark |
| Partition predicates on literal values | Pruning | Athena, Spark, Spectrum |
| Statistics (`ANALYZE`, Glue column stats) | Better join orders | Redshift, Athena CBO, Spark CBO |

**Interview trap:** `WHERE YEAR(order_ts) = 2026` disables pruning; rewrite as a range.

---

## 4. Interview follow-ups (model answers)

**"Your dedup query is slow on 5 B rows in Redshift — why?"** Window partitioned by `order_id` requires redistribution unless DISTKEY = `order_id`; sort spills. Options: DISTKEY alignment, dedupe incrementally only on new batches (staging), or `MERGE` with dedupe at load.

**"How would you test SQL transformations?"** Fixture tables with edge cases (nulls, ties, duplicates, late data), expected-output assertions, dbt tests (unique, not_null, relationships, accepted_values), row-count and checksum reconciliation in CI.

**"Difference between `RANK` and `DENSE_RANK` for 'top 3'?"** `RANK` leaves gaps (1,1,3); `DENSE_RANK` doesn't (1,1,2). Both may return >3 rows; `ROW_NUMBER` returns exactly 3 per partition but arbitrary among ties.

**"Why not `LAST_VALUE` for latest status?"** Default frame ends at current row; use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` or `FIRST_VALUE` with `ORDER BY ... DESC`.

---

## 5. Self-check

1. **Q:** Why does `LEFT JOIN b ... WHERE b.x = 1` behave like an inner join? **A:** Rows with no match have `b.x` NULL and are filtered out; move the predicate into `ON`.
2. **Q:** How does the gaps-and-islands trick identify streaks? **A:** `date - row_number` is constant within consecutive runs.
3. **Q:** What's wrong with `NOT IN (SELECT id FROM t)` if `t.id` has NULL? **A:** The predicate evaluates to unknown for every row → no results.
4. **Q:** Why include a tie-breaker in `ROW_NUMBER` dedupe? **A:** Without it, the chosen row is nondeterministic across runs.
5. **Q:** Why does a 7-row moving average differ from a 7-day one? **A:** Missing dates make 7 rows span more than 7 days; use a date spine or RANGE frames.
6. **Q:** How do you make an incremental MERGE idempotent? **A:** Deduplicate source, overlap lookback window, and only update when the source version is newer or equal.
