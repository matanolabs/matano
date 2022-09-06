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
import { dualAsset, getDirectories } from "../lib/utils";
import { readDetectionConfig } from "./utils";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";

interface DetectionProps {
  matanoUserDirectory: string;
  detectionName: string;
  detectionsLayer: lambda.LayerVersion;
}

class Detection extends Construct {
  constructor(scope: Construct, id: string, props: DetectionProps) {
    super(scope, id);
    const { detectionName, detectionsLayer } = props;
    const detectionDirectory = path.resolve(props.matanoUserDirectory, "detections", detectionName);
    const config = readDetectionConfig(detectionDirectory);

    const logSources = config["log_sources"];
    if (logSources.length == 0) {
      throw "Must have at least one log source configured for a detection.";
    }
    for (const logSource of logSources) {
    }
  }
}

export interface MatanoDetectionsProps {
  alertingSnsTopic: sns.Topic;
}

export class MatanoDetections extends Construct {
  detectionsQueue: sqs.Queue;
  constructor(scope: Construct, id: string, props: MatanoDetectionsProps) {
    super(scope, id);

    const matanoUserDirectory = (cdk.Stack.of(this) as MatanoStack).matanoUserDirectory;
    const detectionsCommonCodeDir = path.resolve("..", "lib/python/matano_detection");
    const detectionsCommonAssetName = "MatanoDetectionsCommonLayer"

    const detectionsLayer = new lambda.LayerVersion(this, detectionsCommonAssetName, {
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
      code: dualAsset(detectionsCommonAssetName, () => lambda.Code.fromAsset(detectionsCommonCodeDir, {
        exclude: ['*.pyc'],
        bundling: {
          volumes: [{hostPath: path.resolve("../local-assets"), containerPath: "/local-assets"}],
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          command: ["bash", "-c", `python -m pip install -r requirements.txt -t /asset-output/python && cp -rT /asset-input/ /asset-output/python && mkdir -p /local-assets/${detectionsCommonAssetName} && cp -a /asset-output/* /local-assets/${detectionsCommonAssetName}`],
        },
      })),
    });

    const detectionsDirectory = path.join(matanoUserDirectory, "detections");
    const detectionNames = getDirectories(detectionsDirectory);

    const detectionsFuncAssetName = "MatanoDetectionFunction";

    const detectionFunction = new lambda.Function(this, detectionsFuncAssetName, {
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
      },
      initialPolicy: [
        new iam.PolicyStatement({actions: ["s3:*"], resources: ["*"]}),
      ],
    });

    props.alertingSnsTopic.grantPublish(detectionFunction);

    this.detectionsQueue = new sqs.Queue(this, "DetectionsQueue");
    const sqsEventSource = new SqsEventSource(this.detectionsQueue, {
      batchSize: 1,
    });
    detectionFunction.addEventSource(sqsEventSource);

    const detectionConfigs: any = {}
    for (const detectionName of detectionNames) {
      const detectionDirectory = path.resolve(matanoUserDirectory, "detections", detectionName);
      const config = readDetectionConfig(detectionDirectory);
      const logSources = config["log_sources"];
      for (const logSource of logSources) {
        if (!detectionConfigs[logSource]) {
          detectionConfigs[logSource] = []
        }
        detectionConfigs[logSource].push({ 
          name: detectionName,
          import_path: `${detectionName}.detect`,
        });
      }
    }
    detectionFunction.addEnvironment("DETECTION_CONFIGS", JSON.stringify(detectionConfigs));
  }
}
