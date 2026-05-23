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
