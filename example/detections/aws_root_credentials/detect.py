def detect(event):
    return (
        event.deepget("aws.cloudtrail.user_identity.type") == "Root"
        and not event.deepget("aws.cloudtrail.event_type") == "AwsServiceEvent"
    )
