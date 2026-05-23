# EventBridge Datacenter Monitoring Telemetry

[![Architecture](https://raw.githubusercontent.com/BoldFellow/eventbridge-datacenter-monitoring/main/architecture.png)](https://github.com/BoldFellow/eventbridge-datacenter-monitoring)

A datacenter monitoring telemetry pipeline built on Amazon EventBridge. Sensors publish temperature, motion, and energy events to a custom event bus. Four EventBridge rules with progressively richer JSON patterns route each event to different downstream consumers — demonstrating the pattern-matching capabilities that make EventBridge the right choice over SNS for complex event routing.

## What you will build

```
EventBridge Scheduler
  ├── every 2 min  →  simulator Lambda  →  dc-telemetry-bus (custom event bus)
  └── daily 09:00  →  daily-summary Lambda

dc-telemetry-bus rules:
  ├── all events           →  recorder Lambda   →  DynamoDB dc-readings
  ├── temperature > 80C    →  SNS email alert   →  your inbox
  ├── motion in restricted-* zones  →  SQS      →  dc-security-review-queue
  └── energy spike > 50 kWh  →  anomaly Lambda  →  DynamoDB dc-anomalies

CloudWatch:
  ├── Alarm on SQS queue depth ≥ 1  →  SNS email  →  your inbox (human-in-the-loop)
  └── Dashboard: rule invocations, Lambda counts, DynamoDB writes, queue depth, alarm state
```

## What you will learn

- **Custom event buses** — why you never publish app events to the default bus
- **EventBridge JSON pattern matching** — exact, numeric, prefix, nested, multi-field
- **Numeric thresholds** — `temperature > 80`, `kwh > 50` — impossible in SNS filter policies
- **Multi-target fan-out** — one event matching multiple rules simultaneously
- **Native AWS service targets** — Lambda, SNS, SQS wired directly from EventBridge rules
- **EventBridge Scheduler** — the current best-practice replacement for legacy scheduled rules
- **IAM least-privilege** — `events:PutEvents` scoped to one bus; separate role per Lambda
- **Resource-based policies** — how EventBridge gets permission to invoke Lambda, SNS, SQS
- **CloudWatch Alarms** — the "human in the loop" pattern: queue for retention, alarm for visibility
- **CloudWatch Dashboards** — visualizing an event-driven pipeline in real time

## Files

| File | Description |
|---|---|
| `guide.md` | Full console/clickops walkthrough — start here |
| `template.yaml` | CloudFormation template — deploys everything in one command |
| `dashboard.json` | CloudWatch dashboard body — paste into the dashboard source editor |
| `lambdas/simulator.py` | Publishes 3–5 random sensor events every 2 minutes |
| `lambdas/recorder.py` | Persists all events to DynamoDB (catch-all rule target) |
| `lambdas/anomaly_handler.py` | Writes energy spike anomalies to DynamoDB |
| `lambdas/daily_summary.py` | Aggregates yesterday's data, emails via SNS |
| `test-events/reading-normal.json` | Normal temperature — only catch-all fires |
| `test-events/reading-high-temp.json` | 87C — triggers SNS email alert |
| `test-events/reading-restricted-motion.json` | Motion in CEO office — triggers SQS + alarm |
| `test-events/reading-energy-spike.json` | 67.3 kWh — triggers anomaly handler |

## Quick start (CloudFormation)

```bash
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name eventbridge-datacenter-monitoring \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides AlertEmailAddress=you@example.com
```

Confirm the SNS subscription email that arrives in your inbox. Wait 2 minutes for the first simulator run, then check DynamoDB `dc-readings` for incoming records. The CloudWatch dashboard deploys automatically — open it in CloudWatch → Dashboards.

> Use `CAPABILITY_NAMED_IAM` — the template uses named IAM roles. `CAPABILITY_IAM` alone will fail.

## Test manually

After deploying, publish a test event from the command line:

```bash
# Triggers SNS email alert
aws events put-events --entries file://test-events/reading-high-temp.json

# Triggers SQS message + CloudWatch alarm (leave message unread to watch alarm fire)
aws events put-events --entries file://test-events/reading-restricted-motion.json

# Triggers anomaly handler Lambda
aws events put-events --entries file://test-events/reading-energy-spike.json
```

## Cleanup

```bash
aws cloudformation delete-stack --stack-name eventbridge-datacenter-monitoring
```

Or follow the manual cleanup steps at the end of `guide.md`.

## Cost

$0.01–$0.05/day while running. All services stay within AWS free tier at lab-level event volumes except the CloudWatch Dashboard (~$0.10/month prorated). Runs for near-free during a single lab session.
