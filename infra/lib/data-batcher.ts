import * as fs from "fs";
import * as fse from "fs-extra";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { S3BucketWithNotifications } from "./s3-bucket-notifs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { getLocalAsset, getLocalAssetPath } from "./utils";
import { RustFunctionCode } from "./rust-function-layer";

interface DataBatcherProps {
  s3Bucket: S3BucketWithNotifications;
  transformerFunction: lambda.Function;
  duplicatesTable: dynamodb.ITable;
}

export class DataBatcher extends Construct {
  outputDLQ: sqs.Queue;
  outputQueue: sqs.Queue;
  batcherFunction: lambda.Function;
  constructor(scope: Construct, id: string, props: DataBatcherProps) {
    super(scope, id);

    this.outputDLQ = new sqs.Queue(this, "OutputDLQ");
    this.outputQueue = new sqs.Queue(this, "OutputQueue", {
      visibilityTimeout: cdk.Duration.seconds(Math.max(props.transformerFunction.timeout!.toSeconds(), 30)), // same as transformer
      deadLetterQueue: { queue: this.outputDLQ, maxReceiveCount: 3 },
    });

    this.batcherFunction = new lambda.Function(this, "Function", {
      code: RustFunctionCode.assetCode({ package: "data_batcher" }),
      handler: "main",
      runtime: lambda.Runtime.PROVIDED_AL2,
      timeout: cdk.Duration.seconds(10),
      environment: {
        RUST_LOG: "warn,data_batcher=info",
        MATANO_SOURCES_BUCKET: props.s3Bucket.bucket.bucketName,
        OUTPUT_QUEUE_URL: this.outputQueue.queueUrl,
        DUPLICATES_TABLE_NAME: props.duplicatesTable.tableName,
      },
    });
    props.duplicatesTable.grantReadWriteData(this.batcherFunction);

    const sqsEventSource = new SqsEventSource(props.s3Bucket.queue, {
      batchSize: 10000,
      maxBatchingWindow: cdk.Duration.seconds(5),
      reportBatchItemFailures: true,
    });
    this.batcherFunction.addEventSource(sqsEventSource);
    this.outputQueue.grantSendMessages(this.batcherFunction);
    this.outputQueue.grantSendMessages(props.transformerFunction);
    this.outputDLQ.grantSendMessages(props.transformerFunction);

    props.transformerFunction.addEventSource(
      new SqsEventSource(this.outputQueue, {
        batchSize: 1,
      })
    );
    props.transformerFunction.addEnvironment("MATANO_BATCHER_QUEUE_URL", this.outputQueue.queueUrl);
    props.transformerFunction.addEnvironment("MATANO_BATCHER_DLQ_URL", this.outputDLQ.queueUrl);
  }
}
