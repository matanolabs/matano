import * as cdk from "aws-cdk-lib/core";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { S3BucketWithNotifications } from "./s3-bucket-notifs";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";

interface DataBatcherProps {
    s3Bucket: S3BucketWithNotifications;
    outputQueue: sqs.Queue;
}

export class DataBatcher extends Construct {
    constructor(scope: Construct, id: string, props: DataBatcherProps) {
        super(scope, id);

        const lambdaFunc = new NodejsFunction(this, "DataBatcherProcessorFunction", {
            entry: "../lambdas/data-batcher/index.ts",
            depsLockFilePath: "../lambdas/package-lock.json",
            runtime: lambda.Runtime.NODEJS_16_X,
            timeout: cdk.Duration.seconds(10),
            environment: {
                OUTPUT_QUEUE_URL: props.outputQueue.queueUrl,
            }
        });

        const sqsEventSource = new SqsEventSource(props.s3Bucket.queue, {
            batchSize: 10000,
            maxBatchingWindow: cdk.Duration.seconds(1),
            reportBatchItemFailures: true,
        });
        lambdaFunc.addEventSource(sqsEventSource);

    }
}
