import os
import json
from base64 import b64decode
from dateutil.parser import parse

import boto3

GROK_EXPR=""
INPUT_TOPIC=""
REGION = os.environ.get("AWS_REGION", "us-west-2")
s3 = boto3.resource("s3")


def lambda_handler(event, context):
    # event_time = parse(event["time"]) 

    # TODO implement
    ret = {
        'statusCode': 200,
        'body': json.dumps([str(b64decode(msg["value"])) for msgs in event["records"].values() for msg in msgs])
    }
    print(ret)
    return ret
