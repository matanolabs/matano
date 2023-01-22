import * as fs from "fs-extra";
import * as path from "path";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
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
  realtimeBucket: s3.IBucket;
  matanoSourcesBucket: s3.IBucket;
}

export class MatanoDetections extends Construct {
  detectionsQueue: sqs.Queue;
  detectionConfigs: any;
  detectionFunction: lambda.Function;
  constructor(scope: Construct, id: string, props: MatanoDetectionsProps) {
    super(scope, id);

    const remoteCacheTable = new ddb.Table(this, "RemoteCacheTable", {
      partitionKey: { name: "pk", type: ddb.AttributeType.STRING },
      sortKey: { name: "sk1", type: ddb.AttributeType.STRING },
      timeToLiveAttribute: "ttl",
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
    });

    const matanoUserDirectory = (cdk.Stack.of(this) as MatanoStack).matanoUserDirectory;

    const detectionsLayer = new lambda.LayerVersion(this, "CommonLayer", {
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_9],
      code: getLocalAsset("MatanoDetectionsCommonLayer"),
    });

    const detectionsDirectory = path.join(matanoUserDirectory, "detections");
    const detectionNames = getDirectories(detectionsDirectory);

    this.detectionFunction = new lambda.Function(this, "Function", {
      handler: "detection.common.handler",
      description: `Matano managed detections function.`,
      runtime: lambda.Runtime.PYTHON_3_9,
      code: lambda.Code.fromAsset(detectionsDirectory, {
        exclude: ["*.pyc"],
        bundling: {
          image: lambda.Runtime.PYTHON_3_9.bundlingImage,
          local: {
            // attempts local bundling first, if it fails, then executes docker based build
            tryBundle(outputDir: string) {
              try {
                const targetDirectory = `${outputDir}`;
                if (!fs.existsSync(targetDirectory)) {
                  fs.mkdirSync(targetDirectory, {
                    recursive: true,
                  });
                }
                fs.copySync(detectionsDirectory, targetDirectory, {
                  overwrite: true,
                  errorOnExist: false,
                  recursive: true,
                });
                if (!fs.readdirSync(targetDirectory).length) {
                  fs.writeFileSync(path.join(targetDirectory, "placeholder"), "");
                }
              } catch (error) {
                console.error("Error with local bundling", error);
                return false;
              }
              return true;
            },
          },
          command: [
            "bash",
            "-c",
            `cp -rT /asset-input/ /asset-output && ([ -z "$(ls -A /asset-output)" ] && touch /asset-output/placeholder)`,
          ],
        },
      }),
      layers: [detectionsLayer],
      memorySize: 1800,
      timeout: cdk.Duration.seconds(60),
      environment: {
        SOURCES_S3_BUCKET: props.matanoSourcesBucket.bucketName,
        REMOTE_CACHE_TABLE_NAME: remoteCacheTable.tableName,
      },
    });

    props.realtimeBucket.grantRead(this.detectionFunction);
    props.matanoSourcesBucket.grantWrite(this.detectionFunction);
    remoteCacheTable.grantReadWriteData(this.detectionFunction);

    const detectionDLQ = new sqs.Queue(this, "DLQ");
    this.detectionsQueue = new sqs.Queue(this, "Queue", {
      deadLetterQueue: { queue: detectionDLQ, maxReceiveCount: 3 },
      visibilityTimeout: cdk.Duration.seconds(Math.max(this.detectionFunction.timeout!.toSeconds(), 30)),
    });

    const sqsEventSource = new SqsEventSource(this.detectionsQueue, {
      batchSize: 1,
    });
    this.detectionFunction.addEventSource(sqsEventSource);

    this.detectionConfigs = {};
    let tablesWithDetections: string[] = [];
    for (const detectionName of detectionNames) {
      const detectionDirectory = path.resolve(matanoUserDirectory, "detections", detectionName);
      const config = readDetectionConfig(detectionDirectory);
      validateDetectionConfig(config, detectionDirectory);
      this.detectionConfigs[detectionName] = config;
      tablesWithDetections = [...tablesWithDetections, ...config["tables"]];
    }
    tablesWithDetections = [...new Set(tablesWithDetections)];

    props.realtimeTopic.addSubscription(
      new SqsSubscription(this.detectionsQueue, {
        rawMessageDelivery: true,
        filterPolicy: {
          resolved_table_name: sns.SubscriptionFilter.stringFilter({ allowlist: tablesWithDetections }),
        },
      })
    );
  }
}
function validateDetectionConfig(config: Record<string, any>, detectionDirPath: string) {
  if (
    Array.isArray(config?.alert?.destinations) &&
    (config?.alert?.severity == null || config?.alert?.severity == "info")
  ) {
    throw new Error(
      `An info-level detection cannot be configured to deliver alerts to external destinations. ${detectionDirPath}: Please change the alert.severity to \"notice\" or remove the alert.destinations configuration.`
    );
  }
}
