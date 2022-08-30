import * as fs from "fs";
import * as fse from "fs-extra";
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

        this.outputQueue = new sqs.Queue(this, "DataBatcherOutputQueue");

        // TODO: extract better, deep imports blocked tho..
        const lambdaFunc = new NodejsFunction(this, "DataBatcherProcessorFunction", {
            entry: "../lib/nodejs/data-batcher-processor/index.ts",
            depsLockFilePath: "../lib/nodejs/package-lock.json",
            runtime: lambda.Runtime.NODEJS_16_X,
            timeout: cdk.Duration.seconds(10),
            bundling: {
                commandHooks: {
                    beforeInstall: (_1,_2) => [],
                    beforeBundling: (_1,_2) => [],
                    afterBundling(inputDir, outputDir) {
                        return [`mkdir -p ${inputDir}/build/DataBatcherProcessorFunction && cp -a ${outputDir}/* ${inputDir}/build/DataBatcherProcessorFunction`]
                    },
                }
            },
            environment: {
                OUTPUT_QUEUE_URL: this.outputQueue.queueUrl,
            },
        });

        if (!(process as any).pkg) {
            fse.copySync("../lib/nodejs/build", "../local-assets")
        }

        const sqsEventSource = new SqsEventSource(props.s3Bucket.queue, {
            batchSize: 10000,
            maxBatchingWindow: cdk.Duration.seconds(1),
            reportBatchItemFailures: true,
        });
        lambdaFunc.addEventSource(sqsEventSource);
        this.outputQueue.grantSendMessages(lambdaFunc);

    }
}
