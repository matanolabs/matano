import * as fs from "fs";
import * as path from "path";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sns from "aws-cdk-lib/aws-sns";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";

import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { getDirectories, getLocalAsset } from "../lib/utils";
import { readDetectionConfig } from "./utils";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";


export interface MatanoDetectionsProps {
  realtimeTopic: sns.Topic;
  alertingSnsTopic: sns.Topic;
  matanoSourcesBucketName: string;
}

export class MatanoDetections extends Construct {
  detectionsQueue: sqs.Queue;
  detectionConfigs: any;
  detectionFunction: lambda.Function;
  constructor(scope: Construct, id: string, props: MatanoDetectionsProps) {
    super(scope, id);

    const matanoUserDirectory = (cdk.Stack.of(this) as MatanoStack).matanoUserDirectory;
    const detectionsCommonCodeDir = path.resolve("..", "lib/python/matano_detection");
    const detectionsCommonAssetName = "MatanoDetectionsCommonLayer"

    const detectionsLayer = new lambda.LayerVersion(this, detectionsCommonAssetName, {
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
      code: getLocalAsset(detectionsCommonAssetName),
    });

    const detectionsDirectory = path.join(matanoUserDirectory, "detections");
    const detectionNames = getDirectories(detectionsDirectory);

    const detectionsFuncAssetName = "MatanoDetectionFunction";

    this.detectionFunction = new lambda.Function(this, detectionsFuncAssetName, {
      handler: "detection.common.handler",
      description: `Matano managed detections function.`,
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset(detectionsDirectory, {
        exclude: ["*.pyc"],
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          command: ["bash", "-c", `cp -rT /asset-input/ /asset-output`],
        },
      }),
      layers: [detectionsLayer],
      memorySize: 1800,
      environment: {
        ALERTING_SNS_TOPIC_ARN: props.alertingSnsTopic.topicArn,
        SOURCES_S3_BUCKET: props.matanoSourcesBucketName,
      },
      initialPolicy: [
        new iam.PolicyStatement({actions: ["s3:*"], resources: ["*"]}),
      ],
    });

    props.alertingSnsTopic.grantPublish(this.detectionFunction);

    this.detectionsQueue = new sqs.Queue(this, "DetectionsQueue");
    const sqsEventSource = new SqsEventSource(this.detectionsQueue, {
      batchSize: 1,
    });
    this.detectionFunction.addEventSource(sqsEventSource);

    this.detectionConfigs = {}
    let logSourcesWithDetections: string[] = [];
    for (const detectionName of detectionNames) {
      const detectionDirectory = path.resolve(matanoUserDirectory, "detections", detectionName);
      const config = readDetectionConfig(detectionDirectory);
      this.detectionConfigs[detectionName] = config;
      logSourcesWithDetections = [...logSourcesWithDetections, ...config["log_sources"]]
    }
    logSourcesWithDetections = [...new Set(logSourcesWithDetections)];

    props.realtimeTopic.addSubscription(
      new SqsSubscription(this.detectionsQueue, {
        rawMessageDelivery: true,
        filterPolicy: {
          "log_source": sns.SubscriptionFilter.stringFilter({ allowlist: logSourcesWithDetections })
        },
      }),
    );
  }
}
