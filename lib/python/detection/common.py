import os
import base64
import boto3
import jsonlines
from uuid import uuid4
from io import BytesIO
from datetime import datetime, timezone

s3 = boto3.resource("s3")


def decode_event(event):
    decoded_records = []
    raw_records = []
    for records in event["records"].values():
        for record in records:
            raw_records.append(record)
            decoded_record = base64.b64decode(record["value"]).decode("utf-8")
            decoded_records.append(decoded_record)
    return raw_records, decoded_records


def process_responses(raw_records, alert_responses, topic):
    alerts_upload_obj = BytesIO()
    json_writer = jsonlines.Writer(alerts_upload_obj)

    for idx, response in enumerate(alert_responses):
        if not response["alert"]:
            continue
        raw_record = raw_records[idx]
        topic, partition, offset = (
            raw_record["topic"],
            str(raw_record["partition"]),
            str(raw_record["offset"]),
        )
        alert_obj = {
            "id": str(uuid4()),
            "title": response["title"],
            "detection": os.environ["MATANO_DETECTION_NAME"],
            "log_source": topic,
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "record_reference": base64.b64encode(
                f"{topic}#{partition}#{offset}".encode("utf-8")
            ).decode("utf-8"),
        }
        json_writer.write(alert_obj)

    raw_events_bucket = s3.Bucket(os.environ["MATANO_RAW_EVENTS_BUCKET"])
    # s3.Object(raw_events_bucket, "alerts").Put(alerts_upload_obj.getvalue())
