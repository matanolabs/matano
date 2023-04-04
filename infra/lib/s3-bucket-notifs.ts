import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";

interface S3BucketWithNotificationsProps {
  bucketProps?: s3.BucketProps;
  maxReceiveCount?: number;
  eventType?: s3.EventType;
  s3Filters?: s3.NotificationKeyFilter[];
  queueProps?: Partial<sqs.QueueProps>;
}
export class S3BucketWithNotifications extends Construct {
  bucket: s3.Bucket;
  queue: sqs.Queue;
  dlq: sqs.Queue;
  topic: sns.Topic;
  constructor(scope: Construct, id: string, props: S3BucketWithNotificationsProps) {
    super(scope, id);

    this.bucket = new s3.Bucket(this, "Bucket", {
      ...props.bucketProps,
    });

    const bucketName = props?.bucketProps?.bucketName;

    this.dlq = new sqs.Queue(this, "DLQ", {
      queueName: bucketName ? `${bucketName}-dlq` : undefined,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    this.queue = new sqs.Queue(this, "Queue", {
      ...props.queueProps,
      queueName: bucketName ? `${bucketName}-queue` : undefined,
      deadLetterQueue: {
        queue: this.dlq,
        maxReceiveCount: props.maxReceiveCount ?? 3,
      },
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    this.topic = new sns.Topic(this, "Topic", {
      topicName: bucketName ? `${bucketName}-topic` : undefined,
    });

    this.topic.addSubscription(
      new SqsSubscription(this.queue, {
        rawMessageDelivery: true,
      })
    );

    this.bucket.addEventNotification(
      props.eventType ?? s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(this.topic),
      ...(props.s3Filters ?? [])
    );
  }
}
