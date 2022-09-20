import re, ipaddress
from fnmatch import fnmatch


def detect(record):
    return (
        record.get("aws", {}).get("cloudtrail", {}).get("user_identity", {}).get("type") == "Root"
        and not record.get("aws", {}).get("cloudtrail", {}).get("event_type") == "AwsServiceEvent"
    )
