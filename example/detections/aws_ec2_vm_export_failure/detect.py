import re, ipaddress
from fnmatch import fnmatch


def detect(record):
    return (
        record.get("event", {}).get("action") == "CreateInstanceExportTask"
        and record.get("event", {}).get("provider") == "ec2.amazonaws.com"
        and (
            (
                record.get("aws", {}).get("cloudtrail", {}).get("error_message")
                and record.get("aws", {}).get("cloudtrail", {}).get("error_message").startswith("")
            )
            or (
                record.get("aws", {}).get("cloudtrail", {}).get("error_code")
                and record.get("aws", {}).get("cloudtrail", {}).get("error_code").startswith("")
            )
            or "Failure" in record.get("aws", {}).get("cloudtrail", {}).get("response_elements")
        )
    )
