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
