import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { S3BucketWithNotifications } from "./s3-bucket-notifs";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";

interface DataBatcherProps {
    s3Bucket: S3BucketWithNotifications;
}

export class DataBatcher extends Construct {
    outputQueue: sqs.Queue;
    constructor(scope: Construct, id: string, props: DataBatcherProps) {
        super(scope, id);

        this.outputQueue = new sqs.Queue(this, "DataBatcherOutputQueue")

        const lambdaFunc = new NodejsFunction(this, "DataBatcherProcessorFunction", {
            entry: "../lib/nodejs/data-batcher-processor/index.ts",
            depsLockFilePath: "../lib/nodejs/package-lock.json",
            runtime: lambda.Runtime.NODEJS_16_X,
            timeout: cdk.Duration.seconds(10),
            environment: {
                OUTPUT_QUEUE_URL: this.outputQueue.queueUrl,
            },
        });

        const sqsEventSource = new SqsEventSource(props.s3Bucket.queue, {
            batchSize: 10000,
            maxBatchingWindow: cdk.Duration.seconds(1),
            reportBatchItemFailures: true,
        });
        lambdaFunc.addEventSource(sqsEventSource);
        this.outputQueue.grantSendMessages(lambdaFunc);

    }
}
