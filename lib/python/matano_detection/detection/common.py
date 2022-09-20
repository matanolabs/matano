from typing import Any
from dataclasses import dataclass
import os
import base64
import json
import logging
import importlib
import boto3
from uuid import uuid4
from datetime import datetime, timezone
import fastavro

from detection.util import Timer, Timers, timing

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource("s3")
sns = boto3.client("sns")
DETECTION_CONFIGS = None
ALERTING_SNS_TOPIC_ARN = None
timers = Timers()

@dataclass
class RecordData:
    record: Any
    record_idx: int
    s3_bucket: str
    s3_key: str
    log_source: str

    def record_reference(self):
        ref = f"{self.s3_bucket}#{self.s3_key}#{self.record_idx}"
        return base64.b64encode(ref.encode("utf-8")).decode("utf-8"),

@timing(timers)
def handler(event, context):

    global ALERTING_SNS_TOPIC_ARN
    ALERTING_SNS_TOPIC_ARN = os.environ["ALERTING_SNS_TOPIC_ARN"]

    global DETECTION_CONFIGS
    if DETECTION_CONFIGS is None:
        DETECTION_CONFIGS = json.loads(os.environ["DETECTION_CONFIGS"])
        for detection_configs in DETECTION_CONFIGS.values():
            for detection_config in detection_configs:
                detection_config["module"] = importlib.import_module(".", detection_config["import_path"])

    alert_responses = []
    detection_run_count = 0
    with timers.get_timer("process"):
        for record_data in get_records(event):
            for response in run_detections(record_data):
                detection_run_count += 1
                if response:
                    alert_responses.append(response)

    process_responses(alert_responses)

    debug_metrics(record_data.record_idx, detection_run_count)


def debug_metrics(record_count, detection_run_count):
    processing_time = timers.get_timer("process").elapsed
    dl_time = timers.get_timer("data_download").elapsed

    avg_detection_run_time = 0 if record_count == 0 else processing_time/detection_run_count
    logger.info(f"Took {processing_time} seconds to process {record_count} records for an average time of {avg_detection_run_time} seconds per detection run")
    logger.info(f"Downloading took: {dl_time} seconds")


# { bucket: ddd, key: "" }
def get_records(event):
    # Actually batch size: 1 currrently
    for sqs_record in event['Records']:
        sqs_record_body = json.loads(sqs_record['body'])
        log_source = sqs_record_body["log_source"]
        s3_bucket, s3_key = sqs_record_body["bucket"], sqs_record_body["key"]

        logger.info(f"START: Downloading from s3://{s3_bucket}/{s3_key}")
        with timers.get_timer("process").pause():
            with timers.get_timer("data_download"):
                obj_body = s3.Object(s3_bucket, s3_key).get()["Body"]
        logger.info(f"END: Downloading from s3://{s3_bucket}/{s3_key}")

        reader = fastavro.reader(obj_body)

        for record_idx, record in enumerate(reader):
            yield RecordData(record, record_idx, s3_bucket, s3_key, log_source)

def run_detections(record_data: RecordData):
    configs = DETECTION_CONFIGS[record_data.log_source]

    for detection_config in configs:
        detection_name, detection_module = detection_config['name'], detection_config["module"]

        alert_title = detection_name # TODO: fix
        alert_response = detection_module.detect(record_data.record)

        if not alert_response:
            yield False
        else:
            yield {
                "alert": alert_response,
                "title": alert_title,
                "detection": detection_name,
                "record_data": record_data,
            }

def process_responses(alert_responses):
    if not alert_responses:
        return

    alert_objs = []
    for idx, response in enumerate(alert_responses):
        record_data: RecordData = response["record_data"]
        alert_obj = {
            "id": str(uuid4()),
            "title": response["title"],
            "detection": record_data.detection,
            "log_source": record_data.log_source,
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "record_reference": record_data.record_reference(),
        }
        alert_objs.append(alert_obj)

    alert_objs_chunks = chunks(alert_objs, 10)
    for alert_objs_chunk in alert_objs_chunks:
        sns.publish_batch(
            TopicArn=ALERTING_SNS_TOPIC_ARN,
            PublishBatchRequestEntries=[
                { 'Id': obj["id"], 'Message': json.dumps(obj) }
                for obj in alert_objs_chunk
            ]
        )
        # TODO: ingest sqs for archival


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
