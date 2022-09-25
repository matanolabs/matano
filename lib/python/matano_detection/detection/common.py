from io import BytesIO
from typing import Any
from dataclasses import dataclass
import os
import base64
import asyncio
import json
import jsonlines
import logging
import importlib
import botocore
import botocore.session
import aiobotocore
import aiobotocore.client
import aiobotocore.session
from uuid import uuid4
from datetime import datetime, timezone
import fastavro

from detection.util import Timer, Timers, timing

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# TODO: consolidate this
botocore_session = botocore.session.get_session()
botocore_session.set_credentials(
    os.environ["AWS_ACCESS_KEY_ID"],
    os.environ["AWS_SECRET_ACCESS_KEY"],
    os.environ["AWS_SESSION_TOKEN"],
)

aiobotocore_session = aiobotocore.session.get_session()
aiobotocore_session.set_credentials(
    os.environ["AWS_ACCESS_KEY_ID"],
    os.environ["AWS_SECRET_ACCESS_KEY"],
    os.environ["AWS_SESSION_TOKEN"],
)

botocore_client_kwargs = dict(
    region_name=os.environ["AWS_REGION"],
)

s3 = botocore_session.create_client("s3", **botocore_client_kwargs)
s3_async = aiobotocore_session.create_client("s3", **botocore_client_kwargs)
sns = aiobotocore_session.create_client("sns", **botocore_client_kwargs)

event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(event_loop)

async def ensure_clients():
    global s3_async
    global sns
    if not isinstance(s3_async, aiobotocore.client.BaseClient):
        s3_async = await s3_async.__aenter__()
        sns = await sns.__aenter__()

DETECTION_CONFIGS = None
ALERTING_SNS_TOPIC_ARN = None
SOURCES_S3_BUCKET = None
MATANO_ALERTS_TABLE_NAME = "matano_alerts"
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
        return base64.b64encode(ref.encode("utf-8")).decode("utf-8")

@timing(timers)
def handler(event, context):

    global ALERTING_SNS_TOPIC_ARN
    ALERTING_SNS_TOPIC_ARN = os.environ["ALERTING_SNS_TOPIC_ARN"]

    global SOURCES_S3_BUCKET
    SOURCES_S3_BUCKET = os.environ["SOURCES_S3_BUCKET"]

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

    event_loop.run_until_complete(process_responses(alert_responses))

    debug_metrics(record_data.record_idx, detection_run_count)


def debug_metrics(record_count, detection_run_count):
    processing_time = timers.get_timer("process").elapsed
    dl_time = timers.get_timer("data_download").elapsed

    avg_detection_run_time = 0 if record_count == 0 else processing_time/detection_run_count
    logger.info(f"Took {processing_time} seconds, processed {record_count} records, ran {detection_run_count} detections, an average time of {avg_detection_run_time} seconds per detection run")
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
                obj_body = s3.get_object(
                    Bucket=s3_bucket,
                    Key=s3_key
                )["Body"]
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

async def process_responses(alert_responses):
    if not alert_responses:
        return

    alert_objs = []
    for idx, response in enumerate(alert_responses):
        record_data: RecordData = response["record_data"]
        alert_obj = {
            "id": str(uuid4()),
            "title": response["title"],
            "detection": response["detection"],
            "log_source": record_data.log_source,
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "record_reference": record_data.record_reference(),
        }
        alert_objs.append(alert_obj)

    futures = []
    alert_objs_chunks = chunks(alert_objs, 10)
    await ensure_clients()

    for alert_objs_chunk in alert_objs_chunks:
        sns_fut = publish_sns_batch(
            topic_arn=ALERTING_SNS_TOPIC_ARN,
            entries=[
                { 'Id': obj["id"], 'Message': json.dumps(obj) }
                for obj in alert_objs_chunk
            ]
        )
        futures.append(sns_fut)

    s3_future = write_alerts(alert_objs)
    futures.append(s3_future)

    await asyncio.gather(*futures)


async def write_alerts(alerts: list):
    # TODO: S3 ingest for now, prolly convert to SQS when added.
    obj = BytesIO()
    json_writer = jsonlines.Writer(obj)
    json_writer.write_all(alerts)
    await s3_async.put_object(
        Body=obj.getvalue(),
        Bucket=SOURCES_S3_BUCKET,
        Key=f"{MATANO_ALERTS_TABLE_NAME}/{str(uuid4())}.json",
    )

async def publish_sns_batch(topic_arn, entries):
    # TODO: handle errors (partial)
    return await sns.publish_batch(
        TopicArn=topic_arn,
        PublishBatchRequestEntries=entries,
    )


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
