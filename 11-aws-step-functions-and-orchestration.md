# 11 — AWS Step Functions and Orchestration

> Related: [19 Airflow & MWAA](19-apache-airflow-and-mwaa.md) · [12 Batch](12-aws-batch-and-containerized-data-jobs.md) · [04 Glue Workflows](04-aws-glue.md) · [32 ADR-006](32-architecture-decision-records.md)

---

## 1. Orchestration options on AWS

| Option | Model | Strength |
|---|---|---|
| **Step Functions** | Serverless state machines (Amazon States Language — JSON/YAML; JSONPath or JSONata) | Deep AWS integrations, durable execution, visual debugging |
| **Glue Workflows + triggers** | Glue-native DAG of jobs/crawlers | Simple Glue-only pipelines |
| **EventBridge Scheduler** | Managed cron/rate/one-time schedules invoking targets | Scheduling at scale; flexible windows |
| **EventBridge rules** | Event-driven triggers | Reactive pipelines |
| **AWS Batch** | Job queue with dependencies, array jobs | Container job scheduling (not a general orchestrator) |
| **Apache Airflow / MWAA** | Python DAGs, scheduler, rich operators | Complex dependency graphs, backfills, cross-system orchestration |

---

## 2. Step Functions internals

### 2.1 Workflow types

| Dimension | Standard | Express |
|---|---|---|
| Max duration | **1 year** | **5 minutes** |
| Execution semantics | **Exactly-once** workflow execution (each state transition persisted) | Async: **at-least-once**; Sync: **at-most-once** |
| History | Execution history in service (25,000-event limit; retained 90 days after close) | CloudWatch Logs only (must enable) |
| Pricing | Per **state transition** | Per request + duration × memory |
| Throughput | StartExecution and state transition quotas (token bucket; e.g., 5,000 transitions/s refill in major Regions) | Very high (state transitions effectively unlimited) |
| Integration patterns | Request-response, `.sync` (Run a Job), `.waitForTaskToken` | Request-response only (no `.sync` or callback) |
| Redrive | Yes (Standard executions within 14 days) | No |
| Best for | Long-running ETL orchestration, human approval, Glue/EMR/Batch job control | High-volume, short event processing (per-record transforms, IoT, streaming microflows) |

(Quotas verified 2026-10-10 — see [27](27-numbers-limits-and-defaults.md).)

**Selection rule:** Data pipelines that run jobs for minutes–hours → **Standard**. Per-event processing at thousands/second with < 5 minutes → **Express**. A common pattern: a Standard parent orchestrating Express children (e.g., Distributed Map with Express child workflows).

**Limitations:** Standard history 25,000 events per execution — long loops need **continue-as-new** (start a new execution) or Distributed Map. Payload 256 KiB per state input/output for both.

### 2.2 Service integration patterns

| Pattern | Syntax | Behavior |
|---|---|---|
| Request-response | `arn:aws:states:::glue:startJobRun` | Returns as soon as API call returns |
| **Run a job (.sync)** | `arn:aws:states:::glue:startJobRun.sync` | Waits for job completion (Step Functions polls/subscribes); fails state if job fails |
| **Callback (.waitForTaskToken)** | `arn:aws:states:::sqs:sendMessage.waitForTaskToken` | Pauses until `SendTaskSuccess/Failure(taskToken)`; heartbeat timeouts |

Supported `.sync` targets include Glue, EMR (add step), EMR Serverless, EMR on EKS, Batch, ECS/Fargate, Athena (`startQueryExecution.sync`), Step Functions (nested), SageMaker, CodeBuild, and others (verify list).

### 2.3 States

`Task`, `Choice`, `Parallel`, `Map` (Inline / **Distributed**), `Wait`, `Pass`, `Succeed`, `Fail`.

**Distributed Map**: iterates over large datasets (S3 objects, S3 inventory, JSON/CSV files in S3) with up to **10,000 parallel child executions**, writing results to S3; tolerated failure thresholds; child executions can be Standard or Express.

### 2.4 Retries, catches, timeouts

```json
{
  "Comment": "Daily orders pipeline (Standard)",
  "StartAt": "IngestRaw",
  "States": {
    "IngestRaw": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "orders-bronze-ingest",
        "Arguments": {"--run_date.$": "$.run_date"}
      },
      "TimeoutSeconds": 3600,
      "Retry": [
        {
          "ErrorEquals": ["Glue.ConcurrentRunsExceededException", "Glue.ThrottlingException", "States.Timeout"],
          "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2.0, "MaxDelaySeconds": 600, "JitterStrategy": "FULL"
        },
        {"ErrorEquals": ["States.TaskFailed"], "IntervalSeconds": 120, "MaxAttempts": 1}
      ],
      "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "NotifyFailure"}],
      "ResultPath": "$.ingest",
      "Next": "QualityGate"
    },
    "QualityGate": {
      "Type": "Task",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {
        "QueryString.$": "States.Format('SELECT count(*) AS bad FROM silver.orders WHERE order_date = DATE \\'{}\\' AND amount < 0', $.run_date)",
        "WorkGroup": "etl"
      },
      "ResultPath": "$.dq",
      "Next": "FanOutMarts"
    },
    "FanOutMarts": {
      "Type": "Parallel",
      "Branches": [
        {"StartAt": "MartRevenue", "States": {"MartRevenue": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "gold-revenue", "Arguments": {"--run_date.$": "$.run_date"}}, "End": true}}},
        {"StartAt": "MartCustomer", "States": {"MartCustomer": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": "gold-customer", "Arguments": {"--run_date.$": "$.run_date"}}, "End": true}}}
      ],
      "ResultPath": null,
      "Next": "Done"
    },
    "NotifyFailure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {"TopicArn": "arn:aws:sns:<region>:<account-id>:data-alerts", "Message.$": "States.JsonToString($.error)"},
      "Next": "Failed"
    },
    "Failed": {"Type": "Fail"},
    "Done": {"Type": "Succeed"}
  }
}
```

Notes:
- Retry defaults: `IntervalSeconds` 1, `MaxAttempts` 3, `BackoffRate` 2.0.
- The quality gate above only runs the query; a real gate fetches results (`athena:getQueryResults`) and uses a `Choice` state on the count, or runs DQ in Glue and fails the job.
- `ResultPath` preserves input while attaching outputs; `ResultPath: null` discards output.
- Timeouts: `TimeoutSeconds` (state), `HeartbeatSeconds` (callbacks), and execution-level `TimeoutSeconds`.

**Interview trap:** Retrying a `.sync` Glue task after `States.Timeout` may start a **second Glue run while the first is still running** (Step Functions timeout doesn't necessarily stop the job). Stop the job in a Catch path or make the job idempotent and MaxConcurrentRuns = 1.

---

## 3. Glue Workflows vs. Step Functions vs. Airflow/MWAA

| Dimension | Glue Workflows | Step Functions | Airflow / MWAA |
|---|---|---|---|
| Scope | Glue jobs, crawlers | Any AWS service (200+ integrations, SDK integrations), HTTP tasks | Any system (operators/providers), Python code |
| Authoring | Console/API/IaC triggers | ASL JSON/YAML, Workflow Studio, CDK | Python DAGs |
| Branching/error handling | Basic (conditional triggers) | Rich (Choice, Catch, Retry, Parallel, Map) | Rich (trigger rules, branching, callbacks) |
| Scheduling | Cron triggers | Via EventBridge Scheduler/rules | Built-in scheduler, data-aware scheduling (Datasets/Assets) |
| **Backfill** | Manual | Manual (loop over dates via Map) | First-class (`airflow dags backfill`, catchup) |
| Cost | Included (pay for jobs) | Per transition (Standard) | MWAA environment-hours (always on) + workers `[Additional cost]` |
| Ops | None | None | MWAA: managed but environment sizing, dependency management, Airflow upgrades |
| Observability | Basic run graph | Excellent per-execution visual history | Rich UI, task logs, Gantt |
| Testing | Hard | Step Functions Local / TestState API | DAG unit tests, `dag.test()` |
| Best for | Simple Glue chains | AWS-native, event-driven, serverless pipelines; high reliability | Large DAG estates, cross-platform dependencies (Snowflake, dbt, Databricks, on-prem), backfill-heavy analytics |

**Selection rule:** Glue Workflows only for small Glue-only pipelines. Step Functions for AWS-native, event-driven, or per-entity orchestration with strong reliability and zero infra. Airflow/MWAA when you need schedule-centric data pipelines with many dependencies, backfills, and non-AWS systems — or when the team already has Airflow expertise.

**Operational consequences:** MWAA has fixed baseline cost and requires dependency management (`requirements.txt`, constraints); Step Functions requires ASL fluency and careful payload management (256 KiB); Glue Workflows have limited visibility and error handling.

**Hybrid:** Airflow for scheduling and cross-system dependencies, triggering Step Functions for AWS-heavy sub-pipelines (`StepFunctionStartExecutionOperator`).

---

## 4. EventBridge Scheduler

- One-time, rate, and cron schedules with time zones; **flexible time windows** to spread load; retry policy and DLQ per schedule; universal targets (any AWS API via templated targets).
- Scales to very large numbers of schedules (quota-dependent) — useful for per-tenant schedules.
- Replaces scheduled EventBridge rules for most new scheduling use cases.

---

## 5. DAG design principles (engine-agnostic)

1. **Idempotent tasks**: every task can be re-run for a logical date without duplicates (overwrite partitions, MERGE).
2. **Parameterize by logical date/window**, not `now()`.
3. **Atomic publish**: write to staging, validate, then swap/commit (Iceberg snapshot, partition location update, Redshift transaction).
4. **Small, well-bounded tasks**: retry granularity matches failure granularity.
5. **Explicit dependencies on data**, not just on time (sensors/events/Datasets on success markers).
6. **Retries with exponential backoff + jitter** for transient errors only; fail fast on deterministic errors (schema mismatch).
7. **Timeouts everywhere** (task, job, execution).
8. **Fan-out/fan-in** bounded by downstream capacity (Map `MaxConcurrency`, Airflow pools).
9. **Separate orchestration from compute**: orchestrator triggers jobs; it doesn't process data itself.
10. **Observability**: emit run metadata (rows in/out, duration, watermark) to a control table.

### 5.1 Fan-out/fan-in with Distributed Map (per-file processing)

```json
{
  "Type": "Map",
  "ItemReader": {
    "Resource": "arn:aws:states:::s3:listObjectsV2",
    "Parameters": {"Bucket": "<bucket>", "Prefix.$": "States.Format('landing/{}/', $.run_date)"}
  },
  "ItemProcessor": {
    "ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "EXPRESS"},
    "StartAt": "ProcessFile",
    "States": {
      "ProcessFile": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {"FunctionName": "validate-file", "Payload.$": "$"},
        "Retry": [{"ErrorEquals": ["Lambda.TooManyRequestsException"], "IntervalSeconds": 2, "MaxAttempts": 6, "BackoffRate": 2}],
        "End": true
      }
    }
  },
  "MaxConcurrency": 200,
  "ToleratedFailurePercentage": 1,
  "ResultWriter": {"Resource": "arn:aws:states:::s3:putObject", "Parameters": {"Bucket": "<bucket>", "Prefix": "sfn-results/"}},
  "End": true
}
```

(Illustrative; validate with `ValidateStateMachineDefinition` / Workflow Studio.)

---

## 6. Failure handling patterns

| Failure | Pattern |
|---|---|
| Transient API throttling | Retry with backoff + jitter |
| Downstream partial outage | Circuit breaker: Choice on health check; pause via callback |
| Deterministic data error | Fail fast; route to quarantine; alert owner |
| Partial fan-out failure | Distributed Map tolerated failure %, then reprocess failures from results manifest |
| Long-running job hang | Task timeout + Catch → stop job API → alert |
| Manual intervention | `.waitForTaskToken` approval step |
| Redo from failure point | Standard workflow **redrive** (from failed state, within 14 days) |

---

## 7. Cost considerations

- Standard: per state transition — a loop of 1M items as Inline Map with 5 states = 5M transitions. Use Distributed Map with Express children for high item counts.
- Express: per request + GB-seconds.
- Polling loops (`Wait` + `GetJobRun`) consume transitions; `.sync` integration is cheaper and simpler.

---

## 8. Interview questions and model answers

**Q1. Orchestrate: wait for 30 partner files, process each, then build a daily mart — with an SLA at 06:00.**
EventBridge rule on S3 object created (or S3 → SQS) records arrivals in DynamoDB; when the count reaches 30 (or at 05:00 deadline via Scheduler), start a Standard workflow: Distributed Map over the day's files (Express children validate/convert), then Glue `.sync` mart build, DQ gate, publish. If deadline hits with missing files, Choice branch: alert + process available + mark partial.

**Q2. Standard vs. Express for processing each Kinesis record with 3 enrichment steps?**
Express (high volume, short) — or simply Lambda/Flink without Step Functions. Accept at-least-once and design idempotent steps.

**Q3. How do you backfill 365 days with Step Functions?**
Parent workflow with a Map over dates (`MaxConcurrency` respecting Glue DPU quota and source capacity), each child running the same idempotent daily logic; or Airflow backfill if Airflow is the scheduler. Separate backfill compute from production (different queue/workgroup) to avoid competition.

**Q4. Why not use Airflow for everything?**
Always-on cost, scheduler latency for event-driven work, Python dependency management, and it's not ideal for high-frequency per-event orchestration. Step Functions excels at serverless, event-driven, durable executions.

**Q5. A Step Functions execution failed at step 7 of 10 after 3 hours. How to resume?**
Redrive the execution (Standard) from the failed state after fixing the cause — provided earlier steps' outputs are still valid and steps are idempotent.

---

## 9. Self-check

1. **Q:** Express workflow max duration? **A:** 5 minutes.
2. **Q:** What does `.sync` do? **A:** Waits for the integrated job to complete and reflects its success/failure in the state.
3. **Q:** Standard execution history limit? **A:** 25,000 events; use child executions/continue-as-new for long loops.
4. **Q:** Max state payload? **A:** 256 KiB — pass S3 pointers for larger data.
5. **Q:** Which is cheaper for 1M short items: Inline Map in Standard or Distributed Map with Express children? **A:** Usually Distributed Map with Express children (avoids per-transition Standard pricing at high volume).
6. **Q:** Why might a Step Functions timeout lead to duplicate Glue runs? **A:** The state times out but the Glue job keeps running; a retry starts another run unless you stop the first or enforce single concurrency/idempotency.
