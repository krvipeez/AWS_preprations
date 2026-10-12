# 03 — Amazon S3 for Data Engineering

> Related: [02 Data Lake](02-data-lake-architecture.md) · [15 File Formats](15-file-formats-and-partitioning.md) · [07 Lambda/Events](07-aws-lambda-and-event-driven-processing.md) · [21 Security](21-aws-security-and-governance.md) · [27 Limits](27-numbers-limits-and-defaults.md)

---

## 1. Core model

| Concept | Reality |
|---|---|
| **Bucket** | Regional container; globally unique name; general purpose buckets (flat namespace) vs. **directory buckets** (S3 Express One Zone, hierarchical, single-AZ) vs. **table buckets** (S3 Tables, Iceberg). |
| **Object** | Key + data + metadata + version ID. Immutable — "update" means replace. |
| **Key** | Up to 1,024 bytes UTF-8. The full name, including slashes. |
| **Prefix** | Any leading substring of a key. Used for listing, permissions, lifecycle, request scaling. |
| **Delimiter** | Character (usually `/`) used by `ListObjectsV2` to roll keys up into `CommonPrefixes`. |

**Interview trap: S3 prefixes are not directories.** There is no directory object, no rename, no atomic directory move. "Renaming" a 10 TB "folder" means copying and deleting every object (Spark's `FileOutputCommitter` v1 rename semantics are slow and non-atomic on S3 — this is why EMRFS S3-optimized committer and table formats exist). An empty "folder" in the console is a zero-byte object ending with `/`.

### 1.1 Object size facts (verified 2026-10-10)

| Fact | Value |
|---|---|
| Single `PUT` max | 5 GB |
| Multipart upload object max | **50 TB** (raised from the long-standing 5 TB; for > 5 TB use SDK Transfer Manager/CRT) |
| Multipart part size | 5 MiB to 5 GiB (last part can be smaller); up to 10,000 parts |
| Console upload max | 160 GB |
| User-defined metadata | 2 KB |
| Tags per object | 10 |

---

## 2. Consistency

**Official behavior:** Since December 2020, S3 provides **strong read-after-write consistency** for PUTs and DELETEs of objects, and strongly consistent LIST after writes, in all Regions, at no cost.

What this means for pipelines:
- A Spark job listing a prefix immediately after another job wrote it will see all objects.
- EMRFS consistent view (DynamoDB-backed) and S3Guard are no longer needed `[Legacy/deprecated]`.

**Interview trap: Strong consistency does not eliminate the need for idempotent processing.** Consistency is per-request. It does not give you:
- **Atomic multi-object commits** — readers can see 40 of 100 files a job is writing.
- **Exactly-once events** — event notifications are at-least-once.
- **Concurrency control between writers** — two jobs writing the same key: last writer wins. Use **conditional writes** (`If-None-Match: *` to prevent overwrite; `If-Match` with ETag for compare-and-swap) `[IAM permission required]`, a lock (DynamoDB), or a table format.

---

## 3. Storage classes

| Class | Availability design | Min storage duration | Min billable object size | Retrieval | Use case |
|---|---|---|---|---|---|
| S3 Standard | Multi-AZ | None | None | Immediate, no retrieval fee | Hot data, Silver/Gold |
| S3 Intelligent-Tiering | Multi-AZ | None | Objects < 128 KB not monitored/auto-tiered (always billed at frequent tier) | Immediate for frequent/infrequent/archive instant tiers | Unknown/changing access patterns. Monitoring fee per object `[Additional cost]` |
| S3 Standard-IA | Multi-AZ | 30 days | 128 KB | Immediate, per-GB retrieval fee | Older Bronze, backups |
| S3 One Zone-IA | Single AZ | 30 days | 128 KB | Immediate, retrieval fee | Re-creatable data |
| S3 Express One Zone | Single AZ, directory bucket | None (check) | None | Single-digit ms, very high TPS | Spark scratch/shuffle-adjacent, ML training hot data `[Region-dependent]` |
| S3 Glacier Instant Retrieval | Multi-AZ | 90 days | 128 KB | Milliseconds, higher retrieval fee | Quarterly-accessed archives |
| S3 Glacier Flexible Retrieval | Multi-AZ | 90 days | 40 KB overhead per object | Minutes–hours (expedited/standard/bulk), restore required | Compliance archives |
| S3 Glacier Deep Archive | Multi-AZ | 180 days | 40 KB overhead | ~12–48 hours | Long-term retention |

**Interview trap:** Transitioning millions of tiny objects to IA/Glacier can **increase** cost: per-object transition request charges, minimum billable sizes, and per-object overhead. Compact before archiving.

**Interview trap:** Athena/Spark cannot read objects in Glacier Flexible/Deep Archive without a restore. A lifecycle rule moving Bronze to Deep Archive breaks backfills that assumed Bronze was readable.

---

## 4. Lifecycle policies

Rule components: filter (prefix, tags, object size), actions (transition, expiration, noncurrent version transition/expiration, abort incomplete multipart uploads, expired delete marker cleanup).

```json
{
  "Rules": [
    {
      "ID": "bronze-tiering",
      "Filter": {"Prefix": "bronze/"},
      "Status": "Enabled",
      "Transitions": [
        {"Days": 30, "StorageClass": "STANDARD_IA"},
        {"Days": 180, "StorageClass": "GLACIER_IR"}
      ],
      "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}
    },
    {
      "ID": "athena-results-expiry",
      "Filter": {"Prefix": "athena-results/"},
      "Status": "Enabled",
      "Expiration": {"Days": 7}
    }
  ]
}
```

**Best practice:** Always add `AbortIncompleteMultipartUpload`. Failed Spark/Glue writes leave incomplete multipart parts that are **billed but invisible** in normal listings.

Lifecycle actions are asynchronous; they can take time after the rule's due date (objects are queued for action). Billing changes when the object is eligible.

---

## 5. Versioning, Object Lock, replication

| Feature | Behavior | DE use | Prerequisites |
|---|---|---|---|
| Versioning | Keeps every version; delete inserts a delete marker | Protect against bad overwrites; required for replication and MWAA buckets | Once enabled, can only be suspended, not disabled |
| Object Lock | WORM retention (governance/compliance modes), legal hold | Regulatory retention of Bronze/audit data | Versioning; set on bucket |
| SRR/CRR | Asynchronous replication of new objects | DR, residency, account isolation | Versioning on both buckets; replication role `[IAM permission required]`; KMS permissions for encrypted objects `[KMS configuration required]`; cross-Region transfer `[Additional cost]` |
| S3 Replication Time Control | 99.99% of objects replicated within 15 minutes (SLA-backed) | Strict RPO | `[Additional cost]` |
| Batch Replication | Replicate existing objects | Backfill a new replica | S3 Batch Operations job `[Additional cost]` |

**Interview trap:** Replication does not copy existing objects when you create a rule (use Batch Replication), does not replicate deletes by default (delete marker replication is opt-in), and **is not a backup** against logical corruption (bad writes replicate too). Use versioning + Object Lock or AWS Backup for S3.

**Cost trap:** Versioning on a bucket where Spark overwrites partitions daily multiplies storage. Add `NoncurrentVersionExpiration`.

---

## 6. Encryption

| Option | Key management | Notes |
|---|---|---|
| SSE-S3 | S3-managed keys | **Default for all new objects since January 2023.** No KMS calls. |
| SSE-KMS | KMS key (AWS managed `aws/s3` or customer managed) | Audit via CloudTrail; key policies for access control; KMS request quotas and cost `[KMS configuration required]` `[Additional cost]` |
| DSSE-KMS | Dual-layer server-side encryption with KMS | Compliance requirements for two layers `[Additional cost]` |
| SSE-C | Customer-provided key per request | Rare in DE; you manage keys |
| Client-side | Encrypt before upload | Strongest control, hardest for engines to read |

**S3 Bucket Keys** reduce SSE-KMS request traffic to KMS by using a bucket-level key to generate data keys; AWS documents reductions of up to 99% in KMS request costs. Enable for any high-volume SSE-KMS bucket.

**Interview trap:** A Spark job writing 2 million small objects to an SSE-KMS bucket without Bucket Keys can hit KMS `ThrottlingException` (shared account-level request quota per Region) — failures surface as S3 errors.

---

## 7. Access control

Layers that can all affect a request:

1. IAM identity policy
2. Bucket policy (resource)
3. S3 Access Point policy (if accessed via access point)
4. VPC endpoint policy (if via gateway/interface endpoint)
5. KMS key policy (for SSE-KMS objects)
6. Block Public Access (account and bucket level)
7. Object Ownership / ACLs (`[Legacy/deprecated]` — new buckets default to **Bucket owner enforced**, ACLs disabled)
8. SCPs/RCPs
9. Lake Formation (when access is via catalog-integrated engines on registered locations)

**Interview trap:** An S3 bucket policy and an IAM identity policy can both affect access. Same-account: either can allow (no explicit deny). Cross-account: both must allow. Explicit deny in either wins.

Useful bucket policy guardrails:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyInsecureTransport",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": ["arn:aws:s3:::<bucket>", "arn:aws:s3:::<bucket>/*"],
      "Condition": {"Bool": {"aws:SecureTransport": "false"}}
    },
    {
      "Sid": "DenyOutsideOrg",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": ["arn:aws:s3:::<bucket>", "arn:aws:s3:::<bucket>/*"],
      "Condition": {
        "StringNotEquals": {"aws:PrincipalOrgID": "<o-orgid>"},
        "Bool": {"aws:PrincipalIsAWSService": "false"}
      }
    }
  ]
}
```

---

## 8. Event notifications

| Mechanism | Destinations | Filtering | Delivery |
|---|---|---|---|
| **S3 Event Notifications** (native) | SNS, SQS, Lambda | Prefix/suffix only; no overlapping configurations for the same event type with overlapping prefixes | At-least-once; usually seconds, occasionally a minute or longer; ordering not guaranteed |
| **EventBridge integration** | Any EventBridge target (Step Functions, SQS, Lambda, API destinations, other buses/accounts) | Rich content filtering (key prefix/suffix/wildcard, size, metadata), multiple rules | At-least-once; archive and replay; slightly higher latency; per-event cost `[Additional cost]` `[Optional integration]` (enable on bucket) |

### 8.1 Comparison: S3 event notifications vs. EventBridge-based processing

| Dimension | S3 → SQS/SNS/Lambda | S3 → EventBridge |
|---|---|---|
| Setup | Bucket notification config | Enable "Send to EventBridge" on bucket + rules |
| Fan-out | SNS for fan-out, or one destination per non-overlapping filter | Up to 5 targets per rule, many rules |
| Filtering | Prefix/suffix | Content-based patterns |
| Replay | No | Archive + replay |
| Cross-account | Via SNS/SQS resource policies | Native event bus to event bus |
| Cost | No S3 charge for notifications; downstream charges | EventBridge event charges |
| Best for | High-volume simple triggers into SQS buffer | Multi-consumer routing, orchestration triggers, auditability |

**Selection rule:** For high-volume file ingestion, use **S3 → SQS → consumer** (buffering, DLQ, batching). Use **EventBridge** when several independent consumers need different filters, replay, or cross-account routing.

**Interview trap: An S3 event notification is not a guaranteed exactly-once event.** Duplicates can occur; rare delays happen; events are unordered across keys; a misconfigured notification or a permissions issue can drop delivery. Production designs add **idempotent consumers** (dedupe on `bucket/key/versionId` or `sequencer`) and a **periodic reconciliation** (S3 Inventory or a listing of expected prefixes vs. a processed-files ledger).

The `sequencer` field orders events **for the same key** only.

---

## 9. Performance

**Official behavior:** S3 supports at least **3,500 PUT/COPY/POST/DELETE** and **5,500 GET/HEAD** requests per second **per partitioned prefix**, and scales automatically by partitioning the key space as request rates grow. There's no limit to the number of prefixes.

Implications:

- Sudden spikes on a single new prefix can return `503 Slow Down` while S3 scales. Use exponential backoff (SDKs do this) and ramp up.
- Date-first keys (`2026/10/10/...`) concentrate all writes for "today" on one key range; at extreme rates, add high-cardinality components early (e.g., `event_type=x/dt=...`) or rely on S3's auto-partitioning with gradual ramp.
- Use **byte-range GETs** for parallel reads of large objects (Parquet readers do this for column chunks).
- Use **multipart upload** for objects > ~100 MB.
- **S3 Transfer Acceleration** for long-distance uploads `[Additional cost]`.
- Co-locate compute and bucket in the same Region.
- S3 Express One Zone for latency-critical, high-TPS scratch data `[Region-dependent]` `[Additional cost]`.

### 9.1 Listing

`ListObjectsV2` returns up to **1,000 keys per call**, lexicographically ordered. Listing 10 million objects = 10,000 sequential calls unless parallelized by prefix. This is why:

- Engines that list partitions are slow on tables with millions of small files.
- Athena partition projection and Iceberg manifests avoid listings.
- **S3 Inventory** (daily/weekly CSV/ORC/Parquet manifest) is the right tool for auditing billions of objects `[Additional cost]`.

**Interview trap:** Small files hurt twice: more requests (GET/LIST charges and latency) and more task overhead in Spark/Athena.

---

## 10. Partitioned data layouts

```text
s3://bucket/silver/events/event_date=2026-10-10/event_type=purchase/part-00012-<uuid>.c000.snappy.parquet
```

Rules (details in [15](15-file-formats-and-partitioning.md)):
- Hive-style `key=value` names so engines infer partition columns.
- Low/moderate cardinality partition keys; target files 128 MB–1 GB.
- Write file names with a run/attempt identifier to support idempotent cleanup (`part-<batch_id>-<task>.parquet`).
- Don't partition by high-cardinality IDs (`customer_id=...`).

### Comparison: S3 partitioning vs. Redshift distribution and sort keys

| Dimension | S3/Hive partitioning | Redshift distribution key | Redshift sort key |
|---|---|---|---|
| Problem solved | Skip whole files/prefixes (pruning) | Co-locate rows on slices to avoid redistribution in joins/aggregations | Skip blocks within a table via zone maps; speed range filters and merge joins |
| Granularity | Directory/prefix | Slice | 1 MB block |
| Wrong choice causes | Full scans or small-file explosion | Data skew across slices; broadcast/redistribution steps | Full table scans for range predicates |
| Changeable? | Rewrite data (or Iceberg partition evolution) | `ALTER TABLE ... ALTER DISTSTYLE/DISTKEY` (rewrites) | `ALTER TABLE ... ALTER SORTKEY` |
| Selection rule | Most common, bounded-cardinality filter (date) | Most frequent large-table join key with even distribution | Most common range filter (timestamp) |

**Limitation:** Neither concept transfers directly: a Redshift sort key on `event_date` is closer to S3 partitioning + in-file sorting than a distribution key is.

---

## 11. Cost optimization

| Cost driver | Lever |
|---|---|
| Storage GB-month | Lifecycle tiering, compression, Parquet, delete temp/staging, expire noncurrent versions, Iceberg snapshot expiry |
| Requests | Fewer, larger files; avoid listing loops; batch deletes (`DeleteObjects` up to 1,000 keys) |
| Data transfer | Same-Region compute; gateway endpoints (no NAT data-processing charge); avoid cross-AZ readers where applicable |
| Retrieval | Avoid IA for frequently read data |
| Monitoring | Intelligent-Tiering per-object fee; Inventory; Storage Lens advanced metrics `[Additional cost]` |
| KMS | Bucket Keys |

Tools: **S3 Storage Lens** (org-wide usage and activity), **Storage Class Analysis**, Cost Explorer by usage type, S3 Inventory + Athena.

**Interview trap:** Access via NAT Gateway to S3 incurs NAT data processing charges per GB. A gateway endpoint for S3 is free and keeps traffic on the AWS network.

---

## 12. Common failures and troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `403 AccessDenied` | Any policy layer (see §7) | CloudTrail; `get-caller-identity`; check KMS and endpoint policies |
| `503 SlowDown` | Request rate on a prefix during scaling | Backoff with jitter; spread prefixes; fewer smaller requests |
| `NoSuchKey` right after an event | Object was overwritten/deleted after event; wrong key decoding (URL-encoded keys in events: spaces become `+`) | `urllib.parse.unquote_plus(key)`; check versionId |
| Missing events | Overlapping notification configs rejected; Lambda permission missing; SQS policy missing `s3.amazonaws.com` with `aws:SourceArn` | Verify config; reconciliation job |
| `EntityTooLarge` | Single PUT > 5 GB | Multipart |
| Partial data visible | Readers during writes | Write-then-commit pattern or Iceberg |
| Bill spike | Versioning + overwrites; incomplete MPUs; replication; requests from small files | Storage Lens; lifecycle rules |

---

## 13. Interview questions and model answers

**Q1. A team reports Athena returning duplicate rows after a Glue job retry. Why, given S3 is strongly consistent?**
The first attempt wrote some files before failing; the retry wrote a full new set; both sets are under the partition. Consistency guarantees visibility, not atomicity or cleanup. Fix: write to a run-specific staging prefix and atomically switch (Catalog partition location update), or overwrite the partition deterministically (dynamic partition overwrite), or use Iceberg (failed commits leave orphan files that readers don't see).

**Q2. How would you ingest 50,000 files/hour arriving in S3?**
S3 → SQS (event notifications) → consumers that batch messages (Lambda with batch size/window, or a Glue/EMR/Batch micro-batch reading the queue). DLQ, idempotent processing keyed by `key+versionId` or ETag, and an hourly reconciliation against a processed-files ledger. Compact into larger Parquet files downstream.

**Q3. How do you reduce S3 cost for a 2 PB lake by 30%?**
Storage Lens + Inventory to find cold prefixes; lifecycle Bronze to IA/Glacier IR where replay is rare; compact small files before transitions; expire noncurrent versions and Iceberg snapshots; delete orphaned temp/staging data; convert remaining JSON/CSV to compressed Parquet; Intelligent-Tiering for unpredictable datasets with large objects.

**Q4. What's the difference between `ListObjects` delimiter behavior and directories?**
Delimiter-based listing groups keys into `CommonPrefixes` client-visible "folders" by string matching — there's no directory object or metadata; permissions and lifecycle apply to string prefixes.

**Q5. How do you prevent two concurrent jobs from overwriting the same output object?**
S3 conditional writes (`If-None-Match: *` fails if the object exists; `If-Match` compare-and-swap on ETag), or a DynamoDB lock/lease, or a table format with optimistic concurrency.

---

## 14. Self-check

1. **Q:** What request rates does S3 support per partitioned prefix? **A:** At least 3,500 write-class and 5,500 read-class requests per second, scaling as S3 partitions the key space.
2. **Q:** Why add `AbortIncompleteMultipartUpload` to every lake bucket? **A:** Failed multipart uploads leave parts that are billed but not visible as objects.
3. **Q:** Can Athena read Glacier Deep Archive objects directly? **A:** No; they must be restored first (Glacier Instant Retrieval is readable directly).
4. **Q:** Which S3 event field helps order events for the same key? **A:** `sequencer` (hex string comparable for the same key only).
5. **Q:** Why might SSE-KMS writes fail at high volume, and the fix? **A:** KMS request throttling; enable S3 Bucket Keys and reduce object count.
6. **Q:** Does enabling CRR copy existing objects? **A:** No; use S3 Batch Replication for existing objects.
