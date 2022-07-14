from detection.common import decode_event, process_responses
from detect import detect

try:
    from detect import title
except:
    pass


def handler(event, context):
    if not len(event["records"]):
        return
    topic = next(iter(event["records"].values()))[0]["topic"]
    raw_records, decoded_records = decode_event(event)
    alert_responses = []

    for record in decoded_records:
        alert_title = topic
        if title is not None:
            alert_title = title(record)

        alert_response = detect(record)
        alert_responses.append({"alert": alert_response, "title": alert_title})

    process_responses(raw_records, alert_responses, topic)
