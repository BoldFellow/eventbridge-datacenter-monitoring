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
