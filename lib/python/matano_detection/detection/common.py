import asyncio
import base64
import concurrent.futures
import importlib
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Iterable
from uuid import uuid4

import aiobotocore
import aiobotocore.client
import aiobotocore.session
import botocore
import botocore.session
import detection.enrichment
import fastavro
import jsonlines
import nest_asyncio
import pyston_lite
from detection.cache import RemoteCache
from detection.enrichment import _load_enrichment_tables
from detection.util import ALERT_ECS_FIELDS, DeepDict, Timers, json_dumps_dt, timing

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

ddb = botocore_session.create_client("dynamodb", **botocore_client_kwargs)
s3 = botocore_session.create_client("s3", **botocore_client_kwargs)
s3_async = aiobotocore_session.create_client("s3", **botocore_client_kwargs)
sns = aiobotocore_session.create_client("sns", **botocore_client_kwargs)

event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(event_loop)
nest_asyncio.apply(event_loop)


async def ensure_clients():
    global s3_async
    global sns
    if not isinstance(s3_async, aiobotocore.client.BaseClient):
        s3_async = await s3_async.__aenter__()
        sns = await sns.__aenter__()


THREAD_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=12)

_load_enrichment_tables(detection.enrichment)


DETECTION_CONFIGS = None
TABLE_DETECTION_CONFIG = None
SOURCES_S3_BUCKET = None
MATANO_ALERTS_TABLE_NAME = "matano_alerts"
LOADED_PYSTON = False
SEVERITY_TO_DESTINATION_DEFAULTS = {
    "info": [],
    "notice": ["slack"],
    "low": ["slack"],
    "medium": ["slack"],  # TODO...
    "high": ["slack"],
    "critical": ["slack"],
}

timers = Timers()


def remotecache(namespace: str, ttl: int = 3600):
    return RemoteCache(ddb, os.environ["REMOTE_CACHE_TABLE_NAME"], namespace, ttl)


def _load_detection_configs():
    global DETECTION_CONFIGS
    config_path = os.path.join("/opt/config/detections_config.json")
    with open(config_path) as f:
        DETECTION_CONFIGS = json.load(f)


def _load_table_detection_config():
    global TABLE_DETECTION_CONFIG
    ret = defaultdict(list)
    for detection_path, detection_config in DETECTION_CONFIGS.items():
        for table_name in detection_config["tables"]:
            ret[table_name].append(
                {
                    "detection": detection_config,
                    "module": importlib.import_module(".", f"{detection_path}.detect"),
                }
            )
    TABLE_DETECTION_CONFIG = ret


class RecordData:
    __slots__ = "record", "record_idx", "s3_bucket", "s3_key", "table_name"
    record: Any
    record_idx: int
    s3_bucket: str
    s3_key: str
    table_name: str

    def __init__(self, record, record_idx, s3_bucket, s3_key, table_name):
        self.record = DeepDict(record)
        self.record_idx = record_idx
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_name = table_name

    def record_reference(self):
        ref = f"{self.s3_bucket}#{self.s3_key}#{self.record_idx}"
        return base64.b64encode(ref.encode("utf-8")).decode("utf-8")


@timing(timers)
def handler(event, context):
    global LOADED_PYSTON
    if not LOADED_PYSTON:
        pyston_lite.enable()
        LOADED_PYSTON = True

    global SOURCES_S3_BUCKET
    SOURCES_S3_BUCKET = os.environ["SOURCES_S3_BUCKET"]

    if DETECTION_CONFIGS is None:
        _load_detection_configs()
        _load_table_detection_config()

    alert_responses = []
    future_map = {}
    errors = []
    detection_run_count = 0
    with timers.get_timer("process"):
        for record_data in get_records(event):
            detection_configs = TABLE_DETECTION_CONFIG[record_data.table_name]
            for detection_config in detection_configs:
                # Using for IO, not at all perfect but can't know if IO or CPU, at least give it some parallelism.
                future = THREAD_EXECUTOR.submit(
                    run_detection, record_data, detection_config
                )
                future_map[future] = {
                    "record_data": record_data,
                    "detection_name": detection_config["detection"]["name"],
                }
                detection_run_count += 1

        for fut in concurrent.futures.as_completed(future_map):
            try:
                response = fut.result()
                if response:
                    alert_responses.append(response)
            except Exception as e:
                context = future_map[fut]
                errors.append(
                    {
                        **context,
                        "error": e,
                    }
                )

    event_loop.run_until_complete(process_responses(alert_responses))
    if errors:
        # TODO: send errors
        logger.warn(f"Resulted in {len(errors)} errors from user detections")
        for error in errors:
            logger.error(error)

    debug_metrics(record_data.record_idx + 1, detection_run_count, len(alert_responses))


def debug_metrics(record_count, detection_run_count, alert_count):
    processing_time = timers.get_timer("process").elapsed
    dl_time = timers.get_timer("data_download").elapsed

    avg_detection_run_time = (
        0 if record_count == 0 else processing_time / detection_run_count
    )
    logger.info(
        f"Took {processing_time} seconds, processed {record_count} records, ran {detection_run_count} detections, an average time of {avg_detection_run_time} seconds per detection run, generating {alert_count} alerts"
    )
    logger.info(f"Downloading took: {dl_time} seconds")


# { bucket: ddd, key: "" }
def get_records(event):
    # Actually batch size: 1 currrently
    for sqs_record in event["Records"]:
        sqs_record_body = json.loads(sqs_record["body"])
        table_name = sqs_record_body["resolved_table_name"]
        s3_bucket, s3_key = sqs_record_body["bucket"], sqs_record_body["key"]

        with timers.get_timer("process").pause():
            with timers.get_timer("data_download"):
                obj_body = s3.get_object(Bucket=s3_bucket, Key=s3_key)["Body"]
        logger.info(f"Downloaded from s3://{s3_bucket}/{s3_key}")

        reader = fastavro.reader(obj_body)

        for record_idx, record in enumerate(reader):
            yield RecordData(record, record_idx, s3_bucket, s3_key, table_name)


def run_detection(record_data: RecordData, detection_config):
    detection, detection_module = (
        detection_config["detection"],
        detection_config["module"],
    )
    alert_response = detection_module.detect(record_data.record)

    if not alert_response:
        return False
    else:
        alert_title = safe_call(detection_module, "title", record_data.record)
        alert_dedupe = safe_call(detection_module, "dedupe", record_data.record)
        alert_severity = safe_call(detection_module, "severity", record_data.record)
        alert_description = safe_call(
            detection_module, "description", record_data.record
        )
        alert_runbook = safe_call(detection_module, "runbook", record_data.record)
        alert_reference = safe_call(detection_module, "reference", record_data.record)
        alert_context = safe_call(detection_module, "alert_context", record_data.record)
        alert_destinations = safe_call(
            detection_module, "destinations", record_data.record
        )

        return {
            "title": alert_title,
            "dedupe": alert_dedupe,
            "severity": alert_severity,
            "description": alert_description,
            "runbook": alert_runbook,
            "reference": alert_reference,
            "context": alert_context,
            "destinations": alert_destinations,
            "detection": detection,
            "record_data": record_data,
        }


def create_alert(alert_response):
    record_data: RecordData = alert_response["record_data"]
    record: dict = record_data.record
    detection_config = alert_response["detection"]

    rule_name = detection_config["name"]
    rule_display_name = detection_config.get("display_name")
    rule_threshold = detection_config.get("alert", {}).get("threshold", 1)
    rule_deduplication_window = (
        detection_config.get("alert", {}).get("deduplication_window_minutes", 60) * 60
    )
    rule_severity = detection_config.get("alert", {}).get("severity")
    rule_description = detection_config.get("description")
    rule_runbook = detection_config.get("runbook")
    rule_reference = detection_config.get("reference")
    rule_destinations = detection_config.get("alert", {}).get("destinations")
    rule_false_positives = detection_config.get("false_positives")
    alert_title = alert_response["title"] or rule_display_name or rule_name
    alert_dedupe = alert_response["dedupe"] or alert_title
    alert_severity = (
        alert_response["severity"]
        or rule_severity
        or ("notice" if rule_destinations else "info")
    )
    alert_description = alert_response["description"] or rule_description
    alert_runbook = alert_response["runbook"] or rule_runbook
    alert_reference = alert_response["reference"] or rule_reference
    alert_destinations = (
        next(
            (
                r
                for r in (
                    alert_response["destinations"],
                    rule_destinations,
                    # SEVERITY_TO_DESTINATION_DEFAULTS.get(alert_severity), TODO(shaeq)
                    [],
                )
                if r is not None
            ),
            None,
        )
        if alert_severity != "info"
        else []
    )
    alert_context = json.dumps({})
    # TODO think about what the API for accepting additional user-defined context fields should be like
    # alert_context = alert_response["context"]

    ret = {
        **{k: v for k, v in record.items() if k in ALERT_ECS_FIELDS},
        "ts": time.time(),
        "event": {
            **(record.get("event") or {}),
            "kind": "signal",
        },
        "matano": {
            "table": record_data.table_name,
            "alert": {
                "title": alert_title,
                "severity": alert_severity,
                "dedupe": alert_dedupe,
                "description": alert_description,
                "runbook": alert_runbook,
                "reference": alert_reference,
                "destinations": alert_destinations,
                "context": alert_context,
                "original_timestamp": record["ts"],
                "original_event": json_dumps_dt(record_data.record),
                "original_event_id": record_data.record_reference(),  # TODO: replace w/ real ID when added
                "rule": {
                    "name": rule_name,
                    "threshold": rule_threshold,
                    "false_positives": rule_false_positives,
                    "deduplication_window": rule_deduplication_window,
                    "match": {
                        "id": str(uuid4()),
                    },
                    "severity": rule_severity,
                    "destinations": rule_destinations,
                },
            },
        },
    }
    return ret


async def process_responses(alert_responses):
    if not alert_responses:
        return

    alert_objs = []
    for idx, response in enumerate(alert_responses):
        alert_obj = create_alert(response)
        alert_objs.append(alert_obj)

    futures = []
    await ensure_clients()

    s3_future = write_alerts(alert_objs)
    futures.append(s3_future)

    await asyncio.gather(*futures)


async def write_alerts(alerts: list):
    # TODO: S3 ingest for now, prolly convert to SQS when added.
    obj = BytesIO()
    json_writer = jsonlines.Writer(obj, dumps=json_dumps_dt)
    json_writer.write_all(alerts)
    await s3_async.put_object(
        Body=obj.getvalue(),
        Bucket=SOURCES_S3_BUCKET,
        Key=f"{MATANO_ALERTS_TABLE_NAME}/{str(uuid4())}.json",
    )


def safe_call(module: Any, func_name: str, *args):
    maybe_func = module.__dict__.get(func_name)
    return maybe_func(*args) if maybe_func else None
