import os
import base64
import json, time
import importlib
import boto3
import jsonlines
from uuid import uuid4
from io import BytesIO
from datetime import datetime, timezone
import fastavro

s3 = boto3.resource("s3")
sns = boto3.client("s3")
DETECTION_CONFIGS = None
ALERTING_SNS_TOPIC_ARN = None


def handler(event, context):
    processtime = {"t": 0}

    global ALERTING_SNS_TOPIC_ARN
    ALERTING_SNS_TOPIC_ARN = os.environ["ALERTING_SNS_TOPIC_ARN"]

    global DETECTION_CONFIGS
    if DETECTION_CONFIGS is None:
        DETECTION_CONFIGS = json.loads(os.environ["DETECTION_CONFIGS"])
        for detection_configs in DETECTION_CONFIGS.values():
            for detection_config in detection_configs:
                detection_config["module"] = importlib.import_module(".", detection_config["import_path"])

    alert_responses = []
    i1 = 0
    st = time.time()
    for record in get_records(event, processtime):
        i1 += 1
        for response in run_detections(record, processtime):
            alert_responses.append(response)
    elt = time.time() - st
    processtime["t"] += elt
    print(f"DET: I took {processtime['t']} seconds to process {i1} records for an average time of {processtime['t']/i1} seconds per record")
    process_responses(alert_responses)

# { bucket: ddd, key: "" }
def get_records(event, processtime):
    # Actually batch size: 1 currrently
    for sqs_record in event['Records']:
        sqs_record_body = json.loads(sqs_record['body'])
        s3_bucket, s3_key = sqs_record_body["bucket"], sqs_record_body["key"]

        st = time.time()
        print(f"START: Downloading from s3://{s3_bucket}/{s3_key}")
        obj_body = s3.Object(s3_bucket, s3_key).get()["Body"].read()
        print(f"END: Downloading from s3://{s3_bucket}/{s3_key}")
        print("Time taken: ", time.time() - st)
        processtime["t"] -= time.time() - st

        reader = fastavro.reader(obj_body)

        for record in reader:
            yield record

def run_detections(record, processtime):
    log_source = record["log_source"]
    configs = DETECTION_CONFIGS[log_source]

    st3 = time.time()
    for detection_config in configs:
        detection_name, detection_module = detection_config['name'], detection_config["module"]
        # print(f"Running detection: {detection_name} for log_source: {log_source}")

        alert_title = log_source
        alert_response = detection_module.detect(record["data"])

        yield {
            "alert": alert_response,
            "title": alert_title,
            "detection": detection_name,
            "log_source": log_source,
        }
    et3 = time.time() - st3
    processtime["t"] += et3

def process_responses(alert_responses):
    alert_objs = []
    for idx, response in enumerate(alert_responses):
        if not response["alert"]:
            continue
        alert_obj = {
            "id": str(uuid4()),
            "title": response["title"],
            "detection": response["detection"],
            "log_source": response["log_source"],
            "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            # "record_reference": base64.b64encode(
            #     f"{topic}#{partition}#{offset}".encode("utf-8")
            # ).decode("utf-8"),
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
