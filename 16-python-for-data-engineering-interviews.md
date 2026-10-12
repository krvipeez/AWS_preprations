# 16 — Python for Data Engineering Interviews

> Related: [31 Cookbook](31-python-sql-pyspark-cookbook.md) · [13 API ingestion](13-data-ingestion-patterns.md) · [07 Idempotency](07-aws-lambda-and-event-driven-processing.md)

All code blocks in this chapter are **Runs in: local Python 3.10+**, **Runnable**, standard library only unless noted. They were syntax-checked; the exercise solutions were executed locally with the included asserts.

---

## 1. Fundamentals interviewers probe at senior level

### 1.1 Data structures and complexity

| Structure | Lookup | Insert | Notes |
|---|---|---|---|
| `list` | O(n) search, O(1) index | O(1) amortized append, O(n) insert at front | Dynamic array |
| `dict` | O(1) avg | O(1) avg | Insertion-ordered (3.7+); hash table |
| `set` | O(1) avg membership | O(1) avg | Dedup, set algebra |
| `collections.deque` | O(1) both ends | O(1) | Queues, sliding windows |
| `heapq` | O(1) min peek | O(log n) push/pop | Top-k, merging sorted streams |
| `collections.Counter` | O(1) | O(1) | Frequency counting, `most_common(k)` uses heap |
| `bisect` | O(log n) on sorted list | O(n) insert | Range lookups |

### 1.2 Iterators and generators

- An **iterator** implements `__iter__` and `__next__`; generators are iterators created by functions with `yield`.
- Generators keep **O(1) memory** relative to input size → essential for large files.
- `yield from` delegates; generator expressions `(x for x in ...)` are lazy.

**Interview trap:** A generator is exhausted after one pass. `rows = read_rows(); count = sum(1 for _ in rows); first = next(rows)` → `StopIteration`.

### 1.3 Decorators

```python
import functools, random, time

def retry(exceptions=(Exception,), attempts=5, base=0.2, cap=5.0):
    """Exponential backoff with full jitter."""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for i in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except exceptions:
                    if i == attempts - 1:
                        raise
                    time.sleep(random.uniform(0, min(cap, base * 2 ** i)))
        return wrapper
    return deco
```

`functools.wraps` preserves `__name__`/docstring — interviewers ask why.

### 1.4 Context managers

```python
import contextlib, time

@contextlib.contextmanager
def timed(label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        print(f"{label}: {time.perf_counter() - start:.3f}s")

with timed("parse"):
    total = sum(range(1_000_000))
```

Class-based: `__enter__`/`__exit__(exc_type, exc, tb)`; returning `True` from `__exit__` suppresses the exception (rarely what you want).

### 1.5 Exceptions

- Catch specific exceptions; preserve cause with `raise NewError(...) from e`.
- Distinguish **retryable** (timeouts, throttling, 5xx) from **non-retryable** (validation, 4xx).
- `finally` for cleanup; `else` runs if no exception.
- `ExceptionGroup`/`except*` (3.11+) for concurrent failures.

### 1.6 Typing

```python
from typing import Iterator, Protocol, TypedDict

class Order(TypedDict):
    order_id: str
    amount: float

class Sink(Protocol):
    def write(self, rows: list[Order]) -> None: ...
```

Type hints + `mypy` in CI catch contract drift in pipeline code; `dataclasses(frozen=True, slots=True)` for lightweight records.

### 1.7 Memory efficiency

- Stream (generators) instead of loading lists.
- `__slots__` / `dataclass(slots=True)` to shrink many small objects.
- `array`/`numpy` for numeric data; `sys.getsizeof` is shallow.
- `tracemalloc` to find allocations.
- Read files in chunks (`iter(lambda: f.read(1 << 20), b"")`).

### 1.8 Concurrency

| Tool | Parallelism | Best for |
|---|---|---|
| `threading` / `ThreadPoolExecutor` | Concurrency; GIL limits CPU parallelism (in standard CPython builds) | I/O-bound: S3 downloads, API calls |
| `asyncio` | Single-threaded cooperative | Many concurrent network calls (aiohttp, aiobotocore) |
| `multiprocessing` / `ProcessPoolExecutor` | True CPU parallelism | CPU-bound parsing, compression |
| Distributed (Spark, Ray, Batch arrays) | Multi-machine | Beyond one node |

Note: CPython 3.13+ offers an experimental free-threaded build (no GIL) — not the default.

**Interview trap:** boto3 **clients** are generally thread-safe; **resources** and **sessions** are not — create a session per thread/process if you use them. Don't share clients across processes (fork) — create per process.

---

## 2. Practical exercises (with solutions)

### Exercise P1 — Frequency counting: top-k error codes from a log stream

```python
import heapq
from collections import Counter
from typing import Iterable

def top_k_errors(lines: Iterable[str], k: int) -> list[tuple[str, int]]:
    counts = Counter()
    for line in lines:
        parts = line.split()
        if len(parts) >= 3 and parts[1] == "ERROR":
            counts[parts[2]] += 1
    return heapq.nlargest(k, counts.items(), key=lambda kv: (kv[1], kv[0]))

logs = ["t1 ERROR E42 x", "t2 INFO ok", "t3 ERROR E42 y", "t4 ERROR E7 z", "t5 ERROR E9 q", "t6 ERROR E9 w"]
assert top_k_errors(logs, 2) == [("E9", 2), ("E42", 2)]
```

Complexity: O(n + u log k) time, O(u) space (u = unique codes). Follow-ups: ties ordering (deterministic key), data too big for memory of `u` → approximate (Count-Min Sketch) or distributed (Spark `groupBy`).

### Exercise P2 — Generator-based large-file processing (CSV → aggregated JSON lines)

```python
import csv, io, json
from collections import defaultdict
from typing import Iterator

def read_rows(fh) -> Iterator[dict]:
    for row in csv.DictReader(fh):
        yield row

def valid_rows(rows: Iterator[dict]) -> Iterator[dict]:
    for r in rows:
        try:
            r["amount"] = float(r["amount"])
        except (KeyError, ValueError):
            continue
        if r.get("customer_id"):
            yield r

def aggregate(rows: Iterator[dict]) -> dict[str, float]:
    totals: dict[str, float] = defaultdict(float)
    for r in rows:
        totals[r["customer_id"]] += r["amount"]
    return dict(totals)

data = io.StringIO("customer_id,amount\nc1,10\nc2,5.5\nc1,abc\n,3\nc1,2\n")
result = aggregate(valid_rows(read_rows(data)))
assert result == {"c1": 12.0, "c2": 5.5}
print(json.dumps(result))
```

Complexity: O(n) time, O(c) memory (c = customers), independent of file size. Follow-ups: what if `c` doesn't fit memory → external sort by key / spill to disk partitions (hash partition into N temp files) / use DuckDB or Spark. Using floats for money → use `decimal.Decimal`.

### Exercise P3 — Deduplicate records keeping the latest version

```python
from typing import Iterable

def dedupe_latest(records: Iterable[dict], key: str, version: str) -> list[dict]:
    best: dict = {}
    for r in records:
        k = r[key]
        if k not in best or r[version] > best[k][version]:
            best[k] = r
    return sorted(best.values(), key=lambda r: r[key])

recs = [
    {"id": 1, "v": 1, "s": "a"}, {"id": 2, "v": 1, "s": "b"},
    {"id": 1, "v": 3, "s": "c"}, {"id": 1, "v": 2, "s": "d"},
]
assert dedupe_latest(recs, "id", "v") == [{"id": 1, "v": 3, "s": "c"}, {"id": 2, "v": 1, "s": "b"}]
```

O(n) time, O(u) space. Follow-up: ties on version → secondary tie-breaker (sequence/ingest time).

### Exercise P4 — Merge k sorted files (streams) without loading them

```python
import heapq
from typing import Iterable, Iterator

def merge_sorted(streams: list[Iterable[int]]) -> Iterator[int]:
    return heapq.merge(*streams)

assert list(merge_sorted([[1, 4, 9], [2, 3, 10], [0]])) == [0, 1, 2, 3, 4, 9, 10]
```

O(N log k). Used in external sorting and merging sorted partition outputs.

### Exercise P5 — Sliding-window rate (events per user in last 60 seconds)

```python
from collections import defaultdict, deque

class RateTracker:
    def __init__(self, window_s: int):
        self.window = window_s
        self.events: dict[str, deque] = defaultdict(deque)

    def hit(self, user: str, ts: int) -> int:
        q = self.events[user]
        q.append(ts)
        while q and q[0] <= ts - self.window:
            q.popleft()
        return len(q)

rt = RateTracker(60)
assert [rt.hit("u", t) for t in (0, 10, 59, 60, 61, 130)] == [1, 2, 3, 3, 4, 1]   # window is (ts-60, ts]
```

Amortized O(1) per hit. Follow-ups: memory bound for many users (evict idle users), out-of-order timestamps (use sorted structure or bucketing).

### Exercise P6 — Flatten nested JSON

```python
def flatten(obj, parent: str = "", sep: str = ".") -> dict:
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(flatten(v, f"{parent}{sep}{k}" if parent else k, sep))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.update(flatten(v, f"{parent}{sep}{i}" if parent else str(i), sep))
    else:
        out[parent] = obj
    return out

assert flatten({"a": {"b": 1, "c": [10, {"d": 2}]}, "e": None}) == {"a.b": 1, "a.c.0": 10, "a.c.1.d": 2, "e": None}
```

Follow-ups: deep recursion limits (use an explicit stack), arrays should often become child tables (relationalize) rather than indexed columns.

### Exercise P7 — Data validation with error collection

```python
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class ValidationResult:
    valid: list = field(default_factory=list)
    errors: list = field(default_factory=list)

ALLOWED = {"PLACED", "SHIPPED", "CANCELLED"}

def validate(rows) -> ValidationResult:
    res = ValidationResult()
    seen = set()
    for i, r in enumerate(rows):
        errs = []
        if not r.get("order_id"):
            errs.append("missing order_id")
        elif r["order_id"] in seen:
            errs.append("duplicate order_id")
        if r.get("status") not in ALLOWED:
            errs.append(f"bad status {r.get('status')!r}")
        try:
            datetime.fromisoformat(r.get("ts", ""))
        except ValueError:
            errs.append("bad ts")
        if errs:
            res.errors.append({"row": i, "errors": errs, "record": r})
        else:
            seen.add(r["order_id"])
            res.valid.append(r)
    return res

out = validate([
    {"order_id": "1", "status": "PLACED", "ts": "2026-10-10T01:00:00"},
    {"order_id": "1", "status": "PLACED", "ts": "2026-10-10T01:00:00"},
    {"order_id": "", "status": "X", "ts": "bad"},
])
assert len(out.valid) == 1 and len(out.errors) == 2
assert out.errors[1]["errors"] == ["missing order_id", "bad status 'X'", "bad ts"]
```

### Exercise P8 — Idempotent processing with a ledger

```python
import hashlib, json, os, tempfile

class Ledger:
    """File-backed processed-items ledger (stand-in for DynamoDB conditional writes)."""
    def __init__(self, path: str):
        self.path = path
        self.done = set()
        if os.path.exists(path):
            with open(path) as f:
                self.done = set(json.load(f))

    def claim(self, item_id: str) -> bool:
        if item_id in self.done:
            return False
        self.done.add(item_id)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(sorted(self.done), f)
        os.replace(tmp, self.path)          # atomic rename on same filesystem
        return True

def item_id(record: dict) -> str:
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()

with tempfile.TemporaryDirectory() as d:
    led = Ledger(os.path.join(d, "ledger.json"))
    batch = [{"a": 1}, {"a": 2}, {"a": 1}]
    processed = [r for r in batch if led.claim(item_id(r))]
    assert processed == [{"a": 1}, {"a": 2}]
    led2 = Ledger(os.path.join(d, "ledger.json"))     # "restart"
    assert not led2.claim(item_id({"a": 2}))
```

Follow-up: claiming before processing risks losing items if processing fails after claim → two-phase status (IN_PROGRESS with expiry → DONE), as in [07](07-aws-lambda-and-event-driven-processing.md).

### Exercise P9 — Paginated API client with retries (mocked transport)

```python
import random, time
from typing import Callable, Iterator

class RetryableError(Exception):
    pass

def paginate(fetch: Callable[[str | None], dict], max_retries: int = 4, sleep=time.sleep) -> Iterator[dict]:
    cursor = None
    while True:
        for attempt in range(max_retries):
            try:
                page = fetch(cursor)
                break
            except RetryableError:
                if attempt == max_retries - 1:
                    raise
                sleep(min(2 ** attempt, 8) * random.uniform(0.5, 1.0))
        yield from page["items"]
        cursor = page.get("next")
        if not cursor:
            return

pages = {None: {"items": [1, 2], "next": "p2"}, "p2": {"items": [3], "next": None}}
calls = {"n": 0}
def flaky_fetch(cursor):
    calls["n"] += 1
    if calls["n"] == 2:
        raise RetryableError("503")
    return pages[cursor]

assert list(paginate(flaky_fetch, sleep=lambda s: None)) == [1, 2, 3]
```

Injecting `fetch` and `sleep` makes it unit-testable without network.

### Exercise P10 — Parallel I/O with bounded concurrency

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def download(key: str) -> tuple[str, int]:
    return key, len(key)                      # stand-in for s3.download_file / get_object

keys = [f"data/part-{i:04d}.parquet" for i in range(50)]
results = {}
errors = {}
with ThreadPoolExecutor(max_workers=8) as pool:
    futures = {pool.submit(download, k): k for k in keys}
    for fut in as_completed(futures):
        k = futures[fut]
        try:
            results[k] = fut.result()[1]
        except Exception as e:
            errors[k] = repr(e)
assert len(results) == 50 and not errors
```

Follow-ups: why threads not processes (I/O bound), choosing `max_workers` (S3 per-prefix rate, connection pool size `botocore.config.Config(max_pool_connections=...)`).

### Exercise P11 — String/array classics framed as DE

1. **Parse S3 URI**

```python
def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(uri)
    bucket, _, key = uri[5:].partition("/")
    return bucket, key

assert parse_s3_uri("s3://b/k1/k2.parquet") == ("b", "k1/k2.parquet")
```

2. **Group files into ~target-size batches (bin packing, first-fit decreasing)**

```python
def batch_files(sizes: dict[str, int], target: int) -> list[list[str]]:
    batches: list[tuple[int, list[str]]] = []
    for name, size in sorted(sizes.items(), key=lambda kv: -kv[1]):
        for i, (total, names) in enumerate(batches):
            if total + size <= target:
                batches[i] = (total + size, names + [name])
                break
        else:
            batches.append((size, [name]))
    return [names for _, names in batches]

assert batch_files({"a": 70, "b": 50, "c": 40, "d": 30, "e": 10}, 100) == [["a", "d"], ["b", "c", "e"]]
```

O(n·b) — fine for compaction planning; heap-based variants for large n.

3. **Find missing dates in partitions**

```python
from datetime import date, timedelta

def missing_dates(present: set[date], start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if d not in present:
            out.append(d)
        d += timedelta(days=1)
    return out

assert missing_dates({date(2026, 10, 1), date(2026, 10, 3)}, date(2026, 10, 1), date(2026, 10, 4)) == [date(2026, 10, 2), date(2026, 10, 4)]
```

### Exercise P12 — Unit testing a transformation (pytest style)

```python
# test_transform.py — run with: python -m pytest -q test_transform.py
import pytest

def normalize_amount(value: str) -> int:
    """Convert '1,234.50' to cents. Raises ValueError on bad input."""
    cleaned = value.replace(",", "").strip()
    if not cleaned:
        raise ValueError("empty")
    whole, _, frac = cleaned.partition(".")
    frac = (frac + "00")[:2]
    return int(whole) * 100 + (int(frac) if not whole.startswith("-") else -int(frac))

@pytest.mark.parametrize("raw,cents", [("1,234.50", 123450), ("0.5", 50), ("7", 700), ("-2.25", -225)])
def test_normalize(raw, cents):
    assert normalize_amount(raw) == cents

def test_empty_raises():
    with pytest.raises(ValueError):
        normalize_amount("  ")
```

Testing principles: pure functions for business logic, injected I/O, fixtures for sample data, property-based tests (Hypothesis) for parsers, contract tests for schemas, moto (`[Optional integration]`) for boto3 mocks.

---

## 3. Time and space complexity cheat sheet for DE problems

| Problem | Naive | Better |
|---|---|---|
| Dedupe n records | O(n²) pairwise | O(n) with hash set/dict |
| Top-k of n | O(n log n) sort | O(n log k) heap |
| Join two lists by key | O(n·m) | O(n + m) hash join |
| Merge k sorted | O(N log N) | O(N log k) heap merge |
| Range lookup (IP → country) | O(n) scan | O(log n) bisect on sorted starts |
| Sessionize sorted events | — | O(n) single pass |

---

## 4. Interview questions and model answers

**Q1. How do you process a 50 GB file on a 4 GB machine in Python?**
Stream it line-by-line or in chunks with generators; keep only aggregates; if aggregation state is too big, hash-partition to temp files on disk and aggregate per partition; or use DuckDB/Polars streaming engines; or distribute via Spark/Batch arrays.

**Q2. Threads or processes to download 10,000 S3 objects?**
Threads (I/O-bound; GIL released during network I/O), bounded pool, botocore max pool connections set accordingly, retries with backoff. Processes only if per-object CPU parsing dominates.

**Q3. How do you make a Python ETL step testable?**
Separate pure transformation functions from I/O; inject clients; parametrize tests; use small fixtures; mock AWS with moto or stubs (`botocore.stub.Stubber`).

**Q4. What's wrong with `except Exception: pass` in a pipeline?**
Silent data loss; no visibility. Catch specific exceptions, log with context, count failures, quarantine bad records, and fail the run past an error threshold.

**Q5. `float` for money?**
Binary floating point can't represent decimal cents exactly; use `Decimal` or integer cents.

---

## 5. Self-check

1. **Q:** Why is `heapq.nlargest(k, ...)` better than sorting for top-k? **A:** O(n log k) vs. O(n log n) and O(k) extra memory.
2. **Q:** What happens on a second iteration over a generator? **A:** It yields nothing (already exhausted).
3. **Q:** Are boto3 clients thread-safe? **A:** Generally yes; sessions/resources are not — and don't share across forked processes.
4. **Q:** How do you preserve original exception context when re-raising? **A:** `raise NewError(...) from e`.
5. **Q:** What makes `os.replace` useful for ledgers/checkpoints? **A:** Atomic rename on the same filesystem — readers never see partial files.
6. **Q:** Why inject `sleep` into a retry function? **A:** To unit test retry logic deterministically and fast.
