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
