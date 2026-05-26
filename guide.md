# EventBridge Datacenter Monitoring Telemetry — Step-by-Step Guide

You will build a real-time telemetry pipeline for a datacenter. Sensors emit temperature, motion, and energy readings. Amazon EventBridge routes each event by type and value to multiple downstream consumers — a DynamoDB recorder, an SNS email alert, an SQS security queue, and an anomaly-tracking Lambda.

The teaching focus is **EventBridge event pattern matching**: exact values, numeric thresholds, prefix matching on nested fields, and multi-field conditions. These features are impossible in SNS filter policies and are what make EventBridge the right choice for routing complex events.

---

## Contents

1. [Prerequisites and cost](#1-prerequisites-and-cost)
2. [Architecture overview](#2-architecture-overview)
3. [Create the DynamoDB tables](#3-create-the-dynamodb-tables)
4. [Create the SNS topics](#4-create-the-sns-topics)
5. [Create the SQS queue](#5-create-the-sqs-queue)
6. [Create the EventBridge custom bus](#6-create-the-eventbridge-custom-bus)
7. [Create IAM roles for each Lambda](#7-create-iam-roles-for-each-lambda)
8. [Create the Lambda functions](#8-create-the-lambda-functions)
9. [Create the four EventBridge rules](#9-create-the-four-eventbridge-rules)
10. [Create the two Scheduler entries](#10-create-the-two-scheduler-entries)
11. [Create the security review alarm](#11-create-the-security-review-alarm)
12. [Create the CloudWatch dashboard](#12-create-the-cloudwatch-dashboard)
13. [Test end-to-end](#13-test-end-to-end)
14. [Cleanup](#14-cleanup)
15. [Appendix A — CloudFormation shortcut](#appendix-a--cloudformation-shortcut)
16. [Stretch exercise — AWS service events](#stretch-exercise--aws-service-events-on-the-default-bus)

---

## 1. Prerequisites and cost

**What you need:**
- An AWS account with permissions to create: Lambda, DynamoDB, SNS, SQS, EventBridge, IAM roles, EventBridge Scheduler, CloudWatch Alarms, and CloudWatch Dashboards.
- Three email addresses you can access during the lab (or one mailbox used three times for testing) — you will confirm an SNS subscription for each to receive alerts.
- AWS CLI installed and configured (used for the test step; console-only steps are also shown).

**What you will deploy (and what it costs):**
- 2 DynamoDB tables (on-demand pricing — free tier covers 25 GB and 200 million requests/month)
- 3 SNS topics with 1 email subscription each (first 1,000 emails/month free)
- 1 SQS queue (first 1 million requests/month free)
- 1 EventBridge custom bus + 4 rules (custom bus events: $1/million; free tier covers 1 million/month)
- 4 Lambda functions (first 1 million invocations/month free)
- 2 EventBridge Scheduler entries (first 14 million invocations/month free)
- 1 CloudWatch Alarm (first 10 alarms free)
- 1 CloudWatch Dashboard ($3/month — negligible at lab scale; free tier does not cover dashboards)

**Estimated cost while running:** $0.01–$0.05/day at the event volumes this lab generates. Tear it down after the lab to pay nothing.

**Region:** Use **us-east-1** (N. Virginia) for all resources. EventBridge Scheduler is available in all major regions; us-east-1 keeps the lab consistent.

---

## 2. Architecture overview

```
   EventBridge Scheduler
   ┌────────────────────────────────────────┐
   │  every 2 minutes → simulator Lambda   │
   │  daily 09:00 UTC → daily-summary      │
   └────────────────────────────────────────┘
                  │
                  ▼
         simulator Lambda
         (publishes 3-5 random sensor events per run)
                  │
                  │ PutEvents (source = sensors.datacenter)
                  ▼
   ┌────────────────────────────────────────┐
   │   Custom event bus: dc-telemetry-bus   │
   └────────────────────────────────────────┘
        │              │           │              │
   Rule 1         Rule 2       Rule 3         Rule 4
   all events     temp > 80C   motion in      energy
   (catch-all)    (numeric)    restricted-*   spike > 50 kWh
        │              │       zones (prefix)      │
        ▼              ▼          ├──────┐          ▼
   recorder       SNS            ▼      ▼      anomaly-handler
   Lambda         dc-infra-    SNS        SQS      Lambda
        │          team        dc-sec-    dc-sec-      │
        ▼          (email:     team       review-      ▼
   DynamoDB       temp alerts) (email:    queue    DynamoDB
   dc-readings                 sec team)     │    dc-anomalies
                                             ▼
                                        CloudWatch
                                        Alarm (≥5)
                                             │ escalation
                                             ▼
                              SNS dc-management ◄── daily-summary Lambda
                                   (manager)         (09:00 UTC digest)
```

Each audience receives only what is relevant to them:
- **Infra team** (`dc-infra-team`): temperature alerts from EventBridge Rule 2.
- **Security team** (`dc-sec-team`): immediate per-event alerts from EventBridge Rule 3 (within seconds of detection).
- **Manager** (`dc-management`): daily digest at 09:00 UTC, plus CloudWatch escalation if 5 or more security events accumulate unprocessed.

All metrics are visible in the **CloudWatch dashboard** — rule invocations, Lambda counts, DynamoDB writes, queue depth, and alarm state on one screen.

**What each component teaches:**

| Component | EventBridge concept |
|---|---|
| `dc-telemetry-bus` custom bus | Custom buses segregate application events from AWS service events. The default bus receives AWS service events; never mix production app events into it. |
| Rule 1 — catch-all | Match by `source` only — every event from this source lands in DynamoDB |
| Rule 2 — temp > 80C | **Numeric pattern** — only EventBridge (not SNS) can filter by `> 80` on a field |
| Rule 3 — restricted zone motion | **Prefix pattern** on a nested field — `"zone": [{"prefix": "restricted-"}]` |
| Rule 4 — energy spike | Type filter + numeric combined — two conditions must both match |
| Scheduler entries | EventBridge Scheduler (new service) replaces legacy scheduled CloudWatch Events rules |
| CloudWatch Alarm | How ops teams get notified when a triage queue has pending work — the "human in the loop" |

---

## 3. Create the DynamoDB tables

You need two tables: one for all sensor readings, one for energy anomalies only.

### Table 1: dc-readings

1. Open the **DynamoDB** console → **Tables** → **Create table**.
2. **Table name:** `dc-readings`
3. **Partition key:** `eventId` (String)
4. Leave sort key blank.
5. **Table settings:** select **Customize settings**.
6. **Table class:** DynamoDB Standard.
7. **Read/write capacity settings:** select **On-demand**.
8. Leave everything else as default → **Create table**.

### Table 2: dc-anomalies

1. **Create table** again.
2. **Table name:** `dc-anomalies`
3. **Partition key:** `anomalyId` (String)
4. **Table settings → Customize settings → On-demand** → **Create table**.

Wait for both tables to reach **Active** status (usually 15–30 seconds).

---

## 4. Create the SNS topics

You need three topics — one per audience. All are Standard type (FIFO is not supported as an EventBridge or CloudWatch Alarm target).

### Topic 1: dc-infra-team (infra team — temperature alerts)

1. Open the **SNS** console → **Topics** → **Create topic**.
2. **Type:** Standard.
3. **Name:** `dc-infra-team`
4. Leave all other settings as default → **Create topic**.
5. Note the **Topic ARN** — it looks like `arn:aws:sns:us-east-1:123456789012:dc-infra-team`.
6. On the topic page → **Subscriptions** tab → **Create subscription**.
7. **Protocol:** Email, **Endpoint:** infra team email address → **Create subscription**.
8. Check that inbox and click **Confirm subscription** in the AWS confirmation email.

### Topic 2: dc-sec-team (security team — queue-depth alarm)

1. **Create topic** → **Standard** → **Name:** `dc-sec-team` → **Create topic**.
2. Note the **Topic ARN**.
3. **Subscriptions** → **Create subscription** → **Protocol:** Email, **Endpoint:** security team email address → **Create subscription**.
4. Confirm the subscription from that inbox.

### Topic 3: dc-management (manager — daily digest)

1. **Create topic** → **Standard** → **Name:** `dc-management` → **Create topic**.
2. Note the **Topic ARN** — you will paste it into the `dc-daily-summary` Lambda environment variable later.
3. **Subscriptions** → **Create subscription** → **Protocol:** Email, **Endpoint:** manager email address → **Create subscription**.
4. Confirm the subscription from that inbox.

> All three subscriptions must be confirmed before the respective publishers can deliver messages. If you skip a confirmation, the publisher will fire but emails will not arrive.

---

## 5. Create the SQS queue

1. Open the **SQS** console → **Create queue**.
2. **Type:** Standard.
3. **Name:** `dc-security-review-queue`
4. **Message retention period:** 1 day (86400 seconds) — security events are time-sensitive.
5. Leave all other settings as default → **Create queue**.

Note the **Queue URL** and **Queue ARN** — you will use the ARN when wiring the EventBridge rule target and queue policy in section 9.

> **Why not add the queue policy here?** The queue policy that allows EventBridge to send messages needs the rule ARN in its `Condition` block — and that rule does not exist until section 9. You will add the queue policy immediately after creating Rule 3.

> **Why resource-based, not IAM role?** For SQS and SNS targets, EventBridge uses resource-based policies on the target (queue policy / topic policy). You do not create an IAM execution role for EventBridge in these cases. IAM execution roles are only needed for certain target types like Step Functions and EventBridge Scheduler.

---

## 6. Create the EventBridge custom bus

1. Open the **EventBridge** console → **Event buses** (left sidebar) → **Create event bus**.
2. **Name:** `dc-telemetry-bus`
3. Leave all other settings as default → **Create**.

> **Custom bus vs. default bus:** The default event bus receives events from AWS services (EC2 state changes, S3 events, etc.). Never publish application events to the default bus — it is shared infrastructure. A custom bus isolates your application events, gives you independent quotas, and lets you set cross-account publishing policies.

---

## 7. Create IAM roles for each Lambda

Each Lambda function gets its own minimal IAM role — no shared roles, no wildcard permissions. You will create four roles.

### Role 1: dc-simulator-role

1. Open **IAM** → **Roles** → **Create role**.
2. **Trusted entity type:** AWS service → **Lambda** → **Next**.
3. **Permissions:** search for and attach `AWSLambdaBasicExecutionRole` (managed policy for CloudWatch Logs). → **Next**.
4. **Role name:** `dc-simulator-role` → **Create role**.
5. Open the new role → **Add permissions** → **Create inline policy**.
6. Switch to the **JSON** editor, paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "events:PutEvents",
      "Resource": "arn:aws:events:us-east-1:YOUR_ACCOUNT_ID:event-bus/dc-telemetry-bus"
    }
  ]
}
```

7. **Policy name:** `PutTelemetryEvents` → **Create policy**.

> `events:PutEvents` is scoped to a single bus ARN — not `*`. This is the correct least-privilege pattern for event publishers.

### Role 2: dc-recorder-role

1. **Create role** → **Lambda** → attach `AWSLambdaBasicExecutionRole` → name it `dc-recorder-role` → **Create role**.
2. Add inline policy — **Policy name:** `WriteReadings`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:PutItem",
      "Resource": "arn:aws:dynamodb:us-east-1:YOUR_ACCOUNT_ID:table/dc-readings"
    }
  ]
}
```

### Role 3: dc-anomaly-handler-role

1. **Create role** → **Lambda** → attach `AWSLambdaBasicExecutionRole` → name it `dc-anomaly-handler-role` → **Create role**.
2. Add inline policy — **Policy name:** `WriteAnomalies`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:PutItem",
      "Resource": "arn:aws:dynamodb:us-east-1:YOUR_ACCOUNT_ID:table/dc-anomalies"
    }
  ]
}
```

### Role 4: dc-daily-summary-role

1. **Create role** → **Lambda** → attach `AWSLambdaBasicExecutionRole` → name it `dc-daily-summary-role` → **Create role**.
2. Add inline policy — **Policy name:** `SummaryPermissions`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:Scan",
      "Resource": [
        "arn:aws:dynamodb:us-east-1:YOUR_ACCOUNT_ID:table/dc-readings",
        "arn:aws:dynamodb:us-east-1:YOUR_ACCOUNT_ID:table/dc-anomalies"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:dc-management"
    }
  ]
}
```

---

## 8. Create the Lambda functions

You will create four functions. For each: runtime is **Python 3.12**, architecture is **x86_64**, and you paste code directly into the inline editor.

### Function 1: dc-simulator

1. **Lambda** console → **Create function** → **Author from scratch**.
2. **Function name:** `dc-simulator`
3. **Runtime:** Python 3.12
4. **Execution role:** Use an existing role → `dc-simulator-role`
5. **Create function**.
6. In the **Code source** editor, replace the default code with:

```python
import json
import random
import boto3
import os

BUILDING = "HQ"
ZONES = [
    "lobby",
    "office-1",
    "cafeteria",
    "restricted-server-room",
    "restricted-ceo-office",
]
BUS = os.environ["BUS_NAME"]
events_client = boto3.client("events")


def temp_event(zone):
    temp = random.randint(82, 95) if random.random() < 0.2 else random.randint(18, 75)
    return {
        "Source": "sensors.datacenter",
        "DetailType": "TemperatureReading",
        "EventBusName": BUS,
        "Detail": json.dumps({
            "sensorId": random.choice(["T-001", "T-002"]),
            "building": BUILDING,
            "zone": zone,
            "temperature": temp,
            "unit": "C",
        }),
    }


def motion_event(zone):
    return {
        "Source": "sensors.datacenter",
        "DetailType": "MotionDetected",
        "EventBusName": BUS,
        "Detail": json.dumps({
            "sensorId": random.choice(["M-003", "M-004"]),
            "building": BUILDING,
            "zone": zone,
            "confidence": random.randint(70, 100),
        }),
    }


def energy_event(zone):
    kwh = round(random.uniform(52, 80), 1) if random.random() < 0.15 else round(random.uniform(5, 45), 1)
    return {
        "Source": "sensors.datacenter",
        "DetailType": "EnergyUsage",
        "EventBusName": BUS,
        "Detail": json.dumps({
            "sensorId": random.choice(["E-005", "E-006"]),
            "building": BUILDING,
            "zone": zone,
            "kwh": kwh,
            "period": "15min",
        }),
    }


MAKERS = [temp_event, motion_event, energy_event]


def lambda_handler(event, context):
    zones = random.sample(ZONES, k=min(5, len(ZONES)))
    entries = [random.choice(MAKERS)(z) for z in zones]
    resp = events_client.put_events(Entries=entries)
    print(f"Published {len(entries)} events, failed={resp['FailedEntryCount']}")
    return {"published": len(entries), "failed": resp["FailedEntryCount"]}
```

7. **Deploy** (orange button).
8. Go to **Configuration** → **Environment variables** → **Edit** → **Add environment variable**:
   - Key: `BUS_NAME`, Value: `dc-telemetry-bus`
9. **Save**.
10. **Configuration** → **General configuration** → **Edit** → set **Timeout** to **30 seconds** → **Save**.

> **Why 30 seconds?** The simulator publishes 3–5 events per invocation. The default 3-second Lambda timeout can expire before all PutEvents calls complete. 30 seconds gives comfortable headroom.

### Function 2: dc-recorder

1. **Create function** → `dc-recorder` → Python 3.12 → `dc-recorder-role`.
2. Paste this code:

```python
import json
import os
import boto3

dynamodb = boto3.resource("dynamodb")
TABLE = os.environ["READINGS_TABLE"]


def lambda_handler(event, context):
    table = dynamodb.Table(TABLE)
    table.put_item(Item={
        "eventId": event["id"],
        "eventType": event["detail-type"],
        "sensorId": event["detail"].get("sensorId", ""),
        "building": event["detail"].get("building", ""),
        "zone": event["detail"].get("zone", ""),
        "detail": json.dumps(event["detail"]),
        "timestamp": event["time"],
    })
    print(f"Recorded {event['id']} type={event['detail-type']} zone={event['detail'].get('zone')}")
    return {"recorded": event["id"]}
```

3. **Deploy**.
4. Environment variable: `READINGS_TABLE` = `dc-readings`.

### Function 3: dc-anomaly-handler

1. **Create function** → `dc-anomaly-handler` → Python 3.12 → `dc-anomaly-handler-role`.
2. Paste this code:

```python
import os
import uuid
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb")
TABLE = os.environ["ANOMALIES_TABLE"]


def lambda_handler(event, context):
    d = event["detail"]
    table = dynamodb.Table(TABLE)
    aid = str(uuid.uuid4())
    table.put_item(Item={
        "anomalyId": aid,
        "originalEventId": event["id"],
        "sensorId": d.get("sensorId", ""),
        "building": d.get("building", ""),
        "zone": d.get("zone", ""),
        "kwh": str(d.get("kwh", 0)),
        "period": d.get("period", ""),
        "detectedAt": datetime.now(timezone.utc).isoformat(),
        "eventTimestamp": event["time"],
        "status": "new",
    })
    print(f"Anomaly {aid}: zone={d.get('zone')}, kwh={d.get('kwh')}")
    return {"anomalyId": aid}
```

3. **Deploy**.
4. Environment variable: `ANOMALIES_TABLE` = `dc-anomalies`.

### Function 4: dc-daily-summary

1. **Create function** → `dc-daily-summary` → Python 3.12 → `dc-daily-summary-role`.
2. Paste this code:

```python
import os
import boto3
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
READINGS_TABLE = os.environ["READINGS_TABLE"]
ANOMALIES_TABLE = os.environ["ANOMALIES_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    readings = dynamodb.Table(READINGS_TABLE).scan(
        FilterExpression=Attr("timestamp").begins_with(yesterday)
    ).get("Items", [])

    anomalies = dynamodb.Table(ANOMALIES_TABLE).scan(
        FilterExpression=Attr("detectedAt").begins_with(yesterday)
    ).get("Items", [])

    by_type = defaultdict(int)
    zones = set()
    for r in readings:
        by_type[r.get("eventType", "Unknown")] += 1
        zones.add(r.get("zone", ""))

    lines = [
        f"Datacenter Daily Summary — {yesterday}",
        "=" * 48,
        f"Total readings : {len(readings)}",
        f"Energy anomalies: {len(anomalies)}",
        f"Active zones   : {len(zones)}",
        "",
        "Events by type:",
    ] + [f"  {t}: {c}" for t, c in sorted(by_type.items())]

    if anomalies:
        lines += ["", "Energy anomalies:"]
        for a in anomalies[:10]:
            lines.append(
                f"  {a.get('zone')}: {a.get('kwh')} kWh "
                f"at {str(a.get('eventTimestamp', ''))[:19]}"
            )

    msg = "\n".join(lines)
    print(msg)
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"Datacenter Daily Summary — {yesterday}",
        Message=msg,
    )
    return {"date": yesterday, "events": len(readings), "anomalies": len(anomalies)}
```

3. **Deploy**.
4. Add three environment variables:
   - `READINGS_TABLE` = `dc-readings`
   - `ANOMALIES_TABLE` = `dc-anomalies`
   - `SNS_TOPIC_ARN` = the `dc-management` topic ARN from step 4

---

## 9. Create the four EventBridge rules

All four rules live on the `dc-telemetry-bus` custom bus. Navigate to **EventBridge** → **Rules** — make sure the bus selector at the top says **dc-telemetry-bus** before creating each rule.

### Rule 1 — All readings (catch-all → recorder Lambda)

**What it teaches:** The simplest pattern — match every event from a given source. This is your audit log rule.

1. **Create rule** on `dc-telemetry-bus`.
2. **Name:** `dc-all-readings`
3. **Description:** Catch all sensor events and persist to DynamoDB
4. **Rule type:** Rule with an event pattern → **Next**.
5. **Event source:** Other → **Next**.
6. **Event pattern:** select **Custom pattern (JSON editor)**, paste:

```json
{
  "source": ["sensors.datacenter"]
}
```

7. **Next**.
8. **Target 1:** AWS service → Lambda function → `dc-recorder` → **Next**.
9. Review → **Create rule**.

**Grant EventBridge permission to invoke the Lambda:**

After saving the rule, go to **Lambda** → `dc-recorder` → **Configuration** → **Resource-based policy statements** → **Add permissions**:
- **AWS service:** Other
- **Statement ID:** `allow-eventbridge-recorder`
- **Principal:** `events.amazonaws.com`
- **Source ARN:** `arn:aws:events:us-east-1:YOUR_ACCOUNT_ID:rule/dc-telemetry-bus/dc-all-readings`
- **Action:** `lambda:InvokeFunction`
- **Save**.

> **Why resource-based policy, not IAM?** Lambda uses resource-based policies for cross-service invocation (EventBridge, SNS, S3 triggers). EventBridge does not need an execution role to call Lambda — it calls it using the Lambda resource-based policy. Compare this to SNS/SQS targets which use queue/topic policies — same idea, different surface.

### Rule 2 — High temperature alert (numeric → SNS)

**What it teaches:** Numeric pattern matching — filtering events where a detail field value exceeds a threshold. This is impossible with SNS filter policies; it is a key differentiator for EventBridge.

1. **Create rule** on `dc-telemetry-bus`.
2. **Name:** `dc-high-temperature`
3. **Rule type:** Rule with an event pattern → **Next**.
4. **Event source:** Other → **Next**.
5. **Event pattern:**

```json
{
  "source": ["sensors.datacenter"],
  "detail-type": ["TemperatureReading"],
  "detail": {
    "temperature": [{"numeric": [">", 80]}]
  }
}
```

6. **Target:** SNS topic → `dc-infra-team`.
7. Expand **Additional settings** → **Configure input** → select **Input transformer**.

**Input paths map:**
```json
{
  "zone": "$.detail.zone",
  "temp": "$.detail.temperature",
  "sensor": "$.detail.sensorId"
}
```

**Input template:**
```
"TEMPERATURE ALERT: <temp>C detected at <zone> by sensor <sensor>. Threshold is 80C."
```

> **Important:** Do not use special characters like the degree symbol (°) in the Input template. The EventBridge API rejects templates containing non-ASCII characters with a `ValidationException`. Use `C` instead of `°C`.

> The input transformer extracts specific fields from the event and formats a human-readable message. Without it, SNS delivers the entire raw EventBridge event envelope as the email body.

8. **Next** → **Create rule**.

**Grant EventBridge permission to publish to SNS** (resource-based on the SNS topic):

1. Open **SNS** → `dc-infra-team` → **Access policy** tab → **Edit**.
2. Add this statement inside the `"Statement"` array (alongside any existing statements):

```json
{
  "Sid": "AllowEventBridgePublish",
  "Effect": "Allow",
  "Principal": {
    "Service": "events.amazonaws.com"
  },
  "Action": "sns:Publish",
  "Resource": "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:dc-infra-team",
  "Condition": {
    "ArnLike": {
      "aws:SourceArn": "arn:aws:events:us-east-1:YOUR_ACCOUNT_ID:rule/dc-telemetry-bus/dc-high-temperature"
    }
  }
}
```

3. **Save changes**.

### Rule 3 — Restricted zone motion (prefix pattern → SNS + SQS fan-out)

**What it teaches:** Prefix matching on a nested detail field, plus **multi-target fan-out** — a single rule delivering to two targets simultaneously. The zone names `restricted-server-room` and `restricted-ceo-office` both start with `restricted-`. One pattern catches both.

The security alert goes to two places at once:
- **SNS** (`dc-sec-team`) — immediate notification to the security team within seconds of the event
- **SQS** (`dc-security-review-queue`) — durable audit log; a human must receive and delete the message to acknowledge review

The CloudWatch alarm (§11) fires when 5 or more messages accumulate unprocessed — an escalation signal, not the primary alert.

1. **Create rule** on `dc-telemetry-bus`.
2. **Name:** `dc-restricted-motion`
3. **Event pattern:**

```json
{
  "source": ["sensors.datacenter"],
  "detail-type": ["MotionDetected"],
  "detail": {
    "zone": [{"prefix": "restricted-"}]
  }
}
```

4. **Add Target 1:** SNS topic → `dc-sec-team`.
   - Expand **Additional settings** → **Configure input** → select **Input transformer**.
   - **Input paths map:**
     ```json
     {
       "zone": "$.detail.zone",
       "sensor": "$.detail.sensorId",
       "confidence": "$.detail.confidence"
     }
     ```
   - **Input template:**
     ```
     "SECURITY ALERT: Motion detected in <zone> by sensor <sensor> (confidence <confidence>%). Immediate review required."
     ```
5. **Add Target 2:** SQS queue → `dc-security-review-queue`.
6. **Create rule**.

**Grant EventBridge permission to publish to `dc-sec-team`:**

1. Open **SNS** → `dc-sec-team` → **Access policy** tab → **Edit**.
2. Add this statement inside the `"Statement"` array:

```json
{
  "Sid": "AllowEventBridgePublish",
  "Effect": "Allow",
  "Principal": {
    "Service": "events.amazonaws.com"
  },
  "Action": "sns:Publish",
  "Resource": "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:dc-sec-team"
}
```

3. **Save changes**.

**Add the SQS queue policy** (now that the rule ARN exists):

EventBridge needs permission to send messages to the queue. You grant this via a **resource-based queue policy** — and you need the rule ARN to lock it down to least privilege, which is why this step comes after creating the rule.

1. On the `dc-security-review-queue` page → **Access policy** tab → **Edit**.
2. Replace the existing policy with:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowEventBridgeSend",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:us-east-1:YOUR_ACCOUNT_ID:dc-security-review-queue",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:us-east-1:YOUR_ACCOUNT_ID:rule/dc-telemetry-bus/dc-restricted-motion"
        }
      }
    }
  ]
}
```

Replace `YOUR_ACCOUNT_ID` with your 12-digit AWS account ID. The `Condition` restricts delivery to only this specific rule — this is the least-privilege pattern.

3. **Save** the policy.

### Rule 4 — Energy spike anomaly (multi-condition → Lambda)

**What it teaches:** Combining a detail-type filter with a numeric threshold in a single pattern. Both conditions must match — this is an AND condition. EventBridge does not currently support OR across conditions in a single pattern; you would need two rules for OR logic.

1. **Create rule** on `dc-telemetry-bus`.
2. **Name:** `dc-energy-spike`
3. **Event pattern:**

```json
{
  "source": ["sensors.datacenter"],
  "detail-type": ["EnergyUsage"],
  "detail": {
    "kwh": [{"numeric": [">", 50]}]
  }
}
```

4. **Target:** Lambda function → `dc-anomaly-handler`.
5. **Create rule**.

**Grant Lambda permission:**

Lambda → `dc-anomaly-handler` → **Configuration** → **Resource-based policy statements** → **Add permissions**:
- **Statement ID:** `allow-eventbridge-anomaly`
- **Principal:** `events.amazonaws.com`
- **Source ARN:** `arn:aws:events:us-east-1:YOUR_ACCOUNT_ID:rule/dc-telemetry-bus/dc-energy-spike`
- **Action:** `lambda:InvokeFunction`
- **Save**.

---

## 10. Create the two Scheduler entries

**EventBridge Scheduler** is a separate service from EventBridge Rules — it appears in the EventBridge console under **Scheduler** in the left sidebar. Unlike legacy scheduled rules on the default bus, Scheduler entries have their own flexible time windows, retry policies, and do not consume event bus capacity.

### Create the Scheduler execution role

Scheduler needs an IAM role to invoke your Lambda functions. Unlike EventBridge Rules (which use resource-based policies), Scheduler uses an IAM execution role.

1. **IAM** → **Roles** → **Create role**.
2. **Trusted entity type:** **Custom trust policy** — paste the policy below, then **Next**.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "scheduler.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

3. **Permissions:** do not attach any managed policy — click **Next**.
4. **Role name:** `dc-scheduler-role` → **Create role**.
5. Open the new role → **Add permissions** → **Create inline policy** → switch to **JSON** editor:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": [
        "arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function:dc-simulator",
        "arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function:dc-daily-summary"
      ]
    }
  ]
}
```

6. **Policy name:** `InvokeScheduledLambdas` → **Create policy**.

> **Why inline and not `AWSLambdaRole`?** The managed `AWSLambdaRole` policy grants `lambda:InvokeFunction` on every function in your account. For a lab this works, but it is overly broad — Scheduler only needs to call two specific functions. The inline policy above is least-privilege. If you prefer a quicker setup, attaching `AWSLambdaRole` instead also works.

> **`scheduler.amazonaws.com` as the trust principal:** Scheduler is a distinct service from EventBridge Rules. Its IAM trust policy uses `scheduler.amazonaws.com` — not `events.amazonaws.com` and not `lambda.amazonaws.com`. Getting this wrong produces `AccessDenied` when Scheduler tries to invoke the Lambda.

### Schedule 1: Simulator (every 2 minutes)

1. **EventBridge** → **Scheduler** (left sidebar) → **Create schedule**.
2. **Schedule name:** `dc-simulator-schedule`
3. **Schedule group:** default
4. **Occurrence:** Recurring schedule
5. **Schedule type:** Rate-based schedule
6. **Rate expression:** `2 minutes`
7. **Flexible time window:** Off (for predictable 2-minute cadence in the lab)
8. **Next**.
9. **Target:** Templated targets → **Lambda** → **Invoke** → select `dc-simulator`.
10. **Payload:** leave empty (the simulator ignores the input event).
11. **Next**.
12. **Action after schedule completion:** None (this runs indefinitely).
13. **Execution role:** Use existing role → `dc-scheduler-role`.
14. **Retry policy:** Max 1 retry, max age 1 minute (events are ephemeral — stale retries add noise).
15. **Create schedule**.

### Schedule 2: Daily summary (09:00 UTC)

1. **Create schedule** again.
2. **Schedule name:** `dc-daily-summary-schedule`
3. **Occurrence:** Recurring schedule
4. **Schedule type:** Cron-based schedule
5. **Cron expression:** `0 9 * * ? *` (09:00 UTC every day)
6. **Timezone:** UTC
7. **Flexible time window:** 15 minutes (slight jitter is fine for a daily report)
8. **Target:** Lambda → `dc-daily-summary`
9. **Execution role:** `dc-scheduler-role`
10. **Create schedule**.

> After creating the simulator schedule, the first run fires within 2 minutes. You will see records appearing in `dc-readings` before you have published any test events — this is expected. It confirms the Scheduler integration works.

---

## 11. Create the security review alarm

The security team already receives an immediate SNS alert when motion is detected (Rule 3, §9). The SQS queue holds every event as an audit log — but if events accumulate and nobody is processing them, that is a signal worth escalating. A CloudWatch Alarm on queue depth provides this escalation.

**What it teaches:** Layered notification — fast path (EventBridge → SNS direct) for real-time alerting, and a slow-path escalation (CloudWatch Alarm) for unacknowledged work. A queue depth of 5 or more suggests a sustained breach or a team that has stopped processing alerts — both warrant escalation. A threshold of 1 would fire on the first event, duplicating the already-immediate SNS alert and adding noise.

### Create the alarm

1. Open **CloudWatch** → **Alarms** → **All alarms** → **Create alarm**.
2. Click **Select metric**.
3. In the metric browser, search for **SQS** → **Queue metrics**.
4. Find the row for `dc-security-review-queue` with metric name `ApproximateNumberOfMessagesVisible` → **Select metric**.
5. Under **Metric**:
   - **Statistic:** Maximum
   - **Period:** 1 minute
6. Under **Conditions**:
   - **Threshold type:** Static
   - **Whenever ApproximateNumberOfMessagesVisible is:** Greater/Equal `>=`
   - **than:** `5`
7. Under **Additional configuration**:
   - **Treat missing data as:** `notBreaching` — a queue with no data means it is empty, not alarming.
8. **Next**.
9. Under **Notification**:
   - **Alarm state trigger:** In alarm
   - **Select an existing SNS topic:** `dc-management`
10. Click **Add notification** again:
    - **Alarm state trigger:** OK
    - **Select an existing SNS topic:** `dc-management`
    > The alarm goes to the manager, not the security team. The security team already received immediate per-event alerts via SNS. This escalation tells the manager that 5 or more alerts are sitting unprocessed — a sign the security team may be overwhelmed or unresponsive.
11. **Next**.
12. **Alarm name:** `dc-security-review-pending`
13. **Create alarm**.

> **Expected initial state:** The alarm will show **Insufficient data** immediately after creation. SQS queue-depth metrics take 3–5 minutes to start appearing in CloudWatch for a new queue. Once metric data arrives, the alarm evaluates and transitions to **OK** or **IN_ALARM**. This delay is acceptable because the immediate security alert arrives via SNS within seconds — the alarm is an escalation signal, not the primary notification path.

### Grant CloudWatch permission to publish to SNS

CloudWatch Alarms publish to SNS using the `cloudwatch.amazonaws.com` service principal. The escalation alarm targets the manager topic, so you need to add this permission to `dc-management`.

1. Open **SNS** → `dc-management` → **Access policy** tab → **Edit**.
2. Add this statement inside the `"Statement"` array:

```json
{
  "Sid": "AllowCloudWatchAlarmPublish",
  "Effect": "Allow",
  "Principal": {
    "Service": "cloudwatch.amazonaws.com"
  },
  "Action": "sns:Publish",
  "Resource": "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:dc-management"
}
```

3. **Save changes**.

> After saving, the alarm can escalate to the manager. If 5 or more messages accumulate unprocessed in `dc-security-review-queue`, the alarm transitions to **IN_ALARM** within 1–2 evaluation periods and the manager receives a notification. Processing and deleting messages until fewer than 5 remain causes the alarm to return to **OK**, also notifying the manager that the backlog cleared.

---

## 12. Create the CloudWatch dashboard

A dashboard turns the abstract event pipeline into something students can watch pulse in real time. Every 2 minutes, the simulator fires — and within seconds the rule invocation metrics tick up, the recorder Lambda increments, and DynamoDB write capacity moves.

### Create the dashboard

1. Open **CloudWatch** → **Dashboards** → **Create dashboard**.
2. **Dashboard name:** `eventbridge-datacenter-monitoring` → **Create dashboard**.
3. In the **Add widget** dialog that appears, click **Cancel** — you will add all widgets at once by pasting the source JSON.
4. Click the **Actions** menu (top right of the dashboard canvas) → **View/edit source**.
5. Delete the existing content and paste the full JSON from `dashboard.json` (provided in this repository).
6. **Update** → **Save dashboard**.

> The dashboard JSON uses hard-coded resource names (`dc-telemetry-bus`, `dc-security-review-queue`, etc.) which match what you created in this guide. No substitutions needed.

### What each widget shows

| Widget | What to watch |
|---|---|
| **EventBridge Rule Invocations** | Four lines — one per rule. All-readings climbs every 2 min. The others spike only when matching events arrive. |
| **Lambda Invocations** | Mirrors the rule chart. Recorder should track all-readings exactly. |
| **Lambda Errors** | Should stay flat at zero. Any spike means a function is throwing exceptions — check CloudWatch Logs. |
| **DynamoDB Writes** | dc-readings follows every event; dc-anomalies only spikes on energy events above 50 kWh. |
| **Security Review Queue** | Shows messages waiting for human acknowledgment. Each receive-and-delete confirms someone reviewed that event. |
| **Security Review Alarm** | Escalation indicator — turns red (IN_ALARM) when 5 or more messages sit unprocessed, green (OK) when the backlog clears. The immediate alert arrives via SNS; this alarm fires when the backlog is large enough to indicate a systemic problem. |
| **SNS Published** | Counts messages to `dc-infra-team` (temperature alerts only — alarm and daily summary use their own topics). |

> **CloudWatch metrics lag:** Metric data for Lambda, DynamoDB, and EventBridge typically appears within 1 minute of the event. SQS queue-depth metrics have a 3–5 minute delay. Do not be alarmed if the queue widget is blank immediately after publishing a test event — it will populate shortly.

---

## 13. Test end-to-end

### Step 1: Wait for the first simulator run

After creating the Scheduler entry, wait up to 2 minutes. Then:

1. **Lambda** → `dc-simulator` → **Monitor** tab → **View CloudWatch Logs**.
2. Open the most recent log stream. You should see a line like:
   ```
   Published 5 events, failed=0
   ```
3. Open **DynamoDB** → `dc-readings` → **Explore table items**. You should see records appearing automatically — the catch-all rule is routing every simulator event to the recorder Lambda.

If no records appear after 3 minutes:
- Check the `dc-all-readings` rule is **Enabled** on `dc-telemetry-bus`.
- Check the `dc-recorder` Lambda resource-based policy includes the rule ARN.
- Check the `dc-recorder` CloudWatch Logs for any error output.

### Step 2: Test each event type manually

Use the CLI to publish each test event and verify the expected target fires. The test event files are in `test-events/`.

**Test A — Normal reading (only recorder fires)**
```bash
aws events put-events \
  --entries file://test-events/reading-normal.json \
  --region us-east-1
```

Expected: `dc-readings` gets a new record. No email. No SQS message. No anomaly record.

**Test B — High temperature (recorder + SNS alert)**
```bash
aws events put-events \
  --entries file://test-events/reading-high-temp.json \
  --region us-east-1
```

Expected: Email arrives within ~30 seconds: *"TEMPERATURE ALERT: 87C detected at restricted-server-room by sensor T-002. Threshold is 80C."*
Also: `dc-readings` gets a new record (catch-all fires too).

**Test C — Restricted zone motion (recorder + SQS + alarm)**
```bash
aws events put-events \
  --entries file://test-events/reading-restricted-motion.json \
  --region us-east-1
```

Expected:
- **Immediate (within ~30 seconds):** Email arrives in the **security team** mailbox: *"SECURITY ALERT: Motion detected in restricted-server-room by sensor M-003 (confidence 92%). Immediate review required."*
- SQS console → `dc-security-review-queue` → **Send and receive messages** → **Poll for messages** shows a new message (the audit record).
- Also: `dc-readings` gets a new record (catch-all rule also fires).
- **Escalation (only if 5+ messages accumulate unprocessed):** Within 1–5 minutes of the queue depth reaching 5, `dc-security-review-pending` transitions to **IN_ALARM** and an escalation email arrives in the **manager** mailbox via `dc-management`.

> To test the escalation alarm, publish the test event 5 or more times and leave the SQS messages unprocessed. After the queue depth reaches 5 and CloudWatch reflects it (~3–5 minutes), the alarm fires. Process and delete messages until fewer than 5 remain to watch it return to OK.

**Test D — Energy spike (recorder + anomaly handler)**
```bash
aws events put-events \
  --entries file://test-events/reading-energy-spike.json \
  --region us-east-1
```

Expected: `dc-anomalies` gets a new record with `zone=cafeteria`, `kwh=67.3`, `status=new`.
Also: `dc-readings` gets a new record.

**Console-only alternative:** In the EventBridge console → **Event buses** → `dc-telemetry-bus` → **Send events**. Paste the contents of the `Detail` field and fill in Source and Detail type manually. This works for testing individual rules without the CLI.

### Step 3: Verify rule metrics

1. **EventBridge** → **Rules** on `dc-telemetry-bus`.
2. Click each rule → **Monitoring** tab.
3. Check **Invocations** — this graph shows how many times the rule matched an event and invoked its target.

This view is the fastest way to confirm a rule is firing without digging through Lambda logs.

### Step 4: Check the CloudWatch dashboard

Open the `eventbridge-datacenter-monitoring` dashboard. Within 5 minutes of your test events, all widgets should show data:
- Rule invocation lines should show spikes
- Lambda invocation counts should match
- DynamoDB write capacity should show activity
- SQS queue depth widget shows 1 (if you left the message unprocessed)
- Alarm widget shows red/IN_ALARM if the SQS message is still waiting

### Step 5: Manually invoke the daily summary

> **Why the first run shows zeros — and how to test with real data**
>
> The function always reports on *yesterday's* date (`datetime.now(UTC) - 1 day`). Every record in `dc-readings` has a `timestamp` equal to the EventBridge event time — which is today. So on the day you deploy, the scan finds nothing and all counts are 0. This is correct behaviour, not a bug. The scheduled 09:00 UTC run tomorrow morning will pick up today's records and show real counts.
>
> To see non-zero output **today**: go to **Lambda** → `dc-daily-summary` → **Code** → change `timedelta(days=1)` to `timedelta(days=0)` → **Deploy**. Run the test below, then change it back.

1. **Lambda** → `dc-daily-summary` → **Test** tab.
2. **Event name:** `manual-test`, leave the default payload `{}`.
3. **Test**.
4. The function returns `{"date": "...", "events": ..., "anomalies": ...}`.
5. The function publishes a **Datacenter Daily Summary** email to the `dc-management` SNS topic. Check the **manager** mailbox — the email contains total readings, anomaly count, active zones, and an energy anomaly list.

---

## 14. Cleanup

Delete resources in this order to avoid dependency errors.

### Delete the Scheduler entries

1. **EventBridge** → **Scheduler** → select `dc-simulator-schedule` → **Delete**.
2. Select `dc-daily-summary-schedule` → **Delete**.

### Delete the EventBridge rules

1. **EventBridge** → **Rules** on `dc-telemetry-bus` → select all four rules → **Delete**.

### Delete the custom bus

1. **EventBridge** → **Event buses** → select `dc-telemetry-bus` → **Delete**.

### Delete the Lambda functions

1. **Lambda** → select `dc-simulator`, `dc-recorder`, `dc-anomaly-handler`, `dc-daily-summary` → **Actions** → **Delete**.

### Delete the IAM roles

1. **IAM** → **Roles** → delete: `dc-simulator-role`, `dc-recorder-role`, `dc-anomaly-handler-role`, `dc-daily-summary-role`, `dc-scheduler-role`.

### Delete the CloudWatch alarm

1. **CloudWatch** → **Alarms** → select `dc-security-review-pending` → **Actions** → **Delete**.

### Delete the CloudWatch dashboard

1. **CloudWatch** → **Dashboards** → select `eventbridge-datacenter-monitoring` → **Delete**.

### Delete the SNS topics

1. **SNS** → **Topics** → select `dc-infra-team` → **Delete**.
2. Select `dc-sec-team` → **Delete**.
3. Select `dc-management` → **Delete**.

### Delete the SQS queue

1. **SQS** → select `dc-security-review-queue` → **Delete**.

### Delete the DynamoDB tables

1. **DynamoDB** → **Tables** → select `dc-readings` → **Delete** → confirm by typing `delete`.
2. Repeat for `dc-anomalies`.

> **Verify cost:** After cleanup, open **AWS Cost Explorer** or **Billing** dashboard. At this event volume, the total charge is typically $0.00 — everything stays within free tier limits during a single lab session (except the CloudWatch Dashboard which charges ~$0.10/month prorated).

---

## Appendix A — CloudFormation shortcut

If you want to deploy the entire stack in one command instead of clicking through the console, the `template.yaml` file in this repository creates all resources automatically — including the CloudWatch Alarm and Dashboard.

**Requirements:** AWS CLI configured, `jq` optional but helpful.

```bash
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name eventbridge-datacenter-monitoring \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    AlertEmailAddress=infra@example.com \
    SecurityEmailAddress=security@example.com \
    ManagerEmailAddress=manager@example.com
```

> **Important:** Use `CAPABILITY_NAMED_IAM`, not `CAPABILITY_IAM`. The template uses named IAM roles (the `RoleName` property is set). CloudFormation requires explicit acknowledgment that named roles are being created — `CAPABILITY_IAM` only covers anonymous roles. This is a common deployment error worth knowing.

**Confirm the SNS subscription** from your inbox after the stack deploys — CFN creates the subscription but AWS still sends the confirmation email.

**Check outputs:**
```bash
aws cloudformation describe-stacks \
  --stack-name eventbridge-datacenter-monitoring \
  --query "Stacks[0].Outputs"
```

**Tear down:**
```bash
aws cloudformation delete-stack --stack-name eventbridge-datacenter-monitoring
```

Note: CFN will delete the DynamoDB tables even if they contain items (because `DeletionPolicy` is not set to `Retain`). Export any data you want to keep before running delete.

---

## Stretch exercise — AWS service events on the default bus

The default event bus receives events from AWS services automatically — no publisher code needed. Add a rule to catch EC2 instance state changes:

1. **EventBridge** → **Rules** — change the bus selector to **default**.
2. **Create rule** → **Name:** `catch-ec2-state-changes`.
3. **Event source:** AWS events or EventBridge partner events.
4. **Sample event:** choose `EC2 Instance State-change Notification`.
5. **Event pattern** (auto-populated, but review it):

```json
{
  "source": ["aws.ec2"],
  "detail-type": ["EC2 Instance State-change Notification"]
}
```

6. **Target:** CloudWatch Log group → create a new group `/aws/events/ec2-state-changes`.
7. **Create rule**.

Now stop or start any EC2 instance. Within seconds, a log entry appears in the CloudWatch log group containing the full event — instance ID, previous state, new state, timestamp.

**The key insight:** You did not write any publisher code. AWS published the event on your behalf. This is the other half of EventBridge's value — not just routing your own events, but reacting to AWS service events with zero integration code.

**Clean up:** Delete the rule and the CloudWatch log group when done.
