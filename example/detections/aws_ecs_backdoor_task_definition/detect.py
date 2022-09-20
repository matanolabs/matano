import re, ipaddress
from fnmatch import fnmatch


def detect(record):
    return (
        record.get("event", {}).get("provider") == "ecs.amazonaws.com"
        and (
            record.get("event", {}).get("action")
            in ("DescribeTaskDefinition", "RegisterTaskDefinition", "RunTask")
        )
        and "169.254"
        in record.get("aws", {})
        .get("cloudtrail", {})
        .get("request_parameters", {})
        .get("containerDefinitions", {})
        .get("command")
        and "$AWS_CONTAINER_CREDENTIALS"
        in record.get("aws", {})
        .get("cloudtrail", {})
        .get("request_parameters", {})
        .get("containerDefinitions", {})
        .get("command")
    )
