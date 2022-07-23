import * as fs from "fs";
import * as path from "path";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { PythonFunction, PythonLayerVersion } from "@aws-cdk/aws-lambda-python-alpha";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { getDirectories } from "../lib/utils";
import {
  AuthenticationMethod,
  ManagedKafkaEventSource,
  SelfManagedKafkaEventSource,
} from "aws-cdk-lib/aws-lambda-event-sources";
import { readDetectionConfig } from "./utils";
import { IKafkaCluster } from "./KafkaCluster";

interface DetectionProps {
  matanoUserDirectory: string;
  detectionName: string;
  detectionsLayer: lambda.LayerVersion;
  rawEventsBucket: s3.Bucket;
  kafkaCluster: IKafkaCluster;
}

class Detection extends Construct {
  constructor(scope: Construct, id: string, props: DetectionProps) {
    super(scope, id);
    const { detectionName, detectionsLayer } = props;

    const detectionDirectory = path.resolve(props.matanoUserDirectory, "detections", detectionName);

    const lambdaFunction = new PythonFunction(this, `MatanoDetection${detectionName}`, {
      functionName: `matano-detection-${detectionName}`,
      description: `Matano managed function for detection: ${detectionName}.`,
      entry: detectionDirectory,
      runtime: lambda.Runtime.PYTHON_3_9,
      index: "detect.py", // unused
      layers: [detectionsLayer],
      environment: {
        MATANO_DETECTION_NAME: detectionName,
        MATANO_RAW_EVENTS_BUCKET: props.rawEventsBucket.bucketName,
      },
    });
    (lambdaFunction.node.defaultChild as lambda.CfnFunction).handler = "detection.handler.handler";

    const config = readDetectionConfig(detectionDirectory);

    const logSources = config["log_sources"];
    if (logSources.length == 0) {
      throw "Must have at least one log source configured for a detection.";
    }
    for (const logSource of logSources) {
      const kafkaSourceProps = {
        topic: `${logSource}-output`,
        batchSize: 10_000, // TODO
        startingPosition: lambda.StartingPosition.LATEST, // TODO
      };
      const eventSource =
        props.kafkaCluster.clusterType === "self-managed"
          ? new SelfManagedKafkaEventSource({
              ...kafkaSourceProps,
              authenticationMethod: AuthenticationMethod.SASL_SCRAM_256_AUTH,
              bootstrapServers: props.kafkaCluster.bootstrapAddress.split(","),
              secret: props.kafkaCluster.secret,
            })
          : new ManagedKafkaEventSource({
              ...kafkaSourceProps,
              clusterArn: props.kafkaCluster.clusterArn,
            });
      lambdaFunction.addEventSource(eventSource);
    }
  }
}

export interface MatanoDetectionsProps {
  rawEventsBucket: s3.Bucket;
  kafkaCluster: IKafkaCluster;
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
    const detectionNames = getDirectories(detectionsDirectory);

    for (const detectionName of detectionNames) {
      new Detection(this, `Detection-${detectionName}`, {
        detectionName,
        detectionsLayer,
        matanoUserDirectory: matanoUserDirectory,
        rawEventsBucket: props.rawEventsBucket,
        kafkaCluster: props.kafkaCluster,
      });
    }
  }
}
