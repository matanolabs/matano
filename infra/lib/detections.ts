import * as fs from "fs";
import * as path from "path";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { PythonFunction, PythonLayerVersion } from "@aws-cdk/aws-lambda-python-alpha";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { getDirectories } from "../lib/utils";
import { readDetectionConfig } from "./utils";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";

interface DetectionProps {
  matanoUserDirectory: string;
  detectionName: string;
  detectionsLayer: lambda.LayerVersion;
  rawEventsBucket: s3.Bucket;
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
  rawEventsBucket: s3.Bucket;
}

export class MatanoDetections extends Construct {
  constructor(scope: Construct, id: string, props: MatanoDetectionsProps) {
    super(scope, id);

    const matanoUserDirectory = (cdk.Stack.of(this) as MatanoStack).matanoUserDirectory;

    const detectionsLayer = new PythonLayerVersion(this, "MatanoDetectionsCommonLayer", {
      entry: path.resolve("..", "lib/python/matano_detection"),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
    });

    const detectionsDirectory = path.join(matanoUserDirectory, "detections");
    fs.writeFileSync(path.resolve(detectionsDirectory, "index.py"), "");
    const detectionNames = getDirectories(detectionsDirectory);

    const detectionFunction = new PythonFunction(this, `MatanoDetectionFunction`, {
      // functionName: `matano-detections`,
      description: `Matano managed detections function.`,
      entry: detectionsDirectory,
      runtime: lambda.Runtime.PYTHON_3_9,
      layers: [detectionsLayer],
      memorySize: 1800,
      environment: {
        MATANO_RAW_EVENTS_BUCKET: props.rawEventsBucket.bucketName,
      },
      initialPolicy: [
        new iam.PolicyStatement({actions: ["s3:*"], resources: ["*"]}),
      ],
    });
    (detectionFunction.node.defaultChild as lambda.CfnFunction).handler = "detection.common.handler";
    fs.unlinkSync(path.resolve(detectionsDirectory, "index.py"));

    const detectionsQueue = new sqs.Queue(this, "DetectionsQueue");
    const sqsEventSource = new SqsEventSource(detectionsQueue, {
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
      // new Detection(this, `Detection-${detectionName}`, {
      //   detectionName,
      //   detectionsLayer,
      //   matanoUserDirectory: matanoUserDirectory,
      //   rawEventsBucket: props.rawEventsBucket,
      // });
    }
    detectionFunction.addEnvironment("DETECTION_CONFIGS", JSON.stringify(detectionConfigs));
  }
}
