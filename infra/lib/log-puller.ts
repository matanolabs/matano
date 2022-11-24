import { Construct, Node } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import { SqsQueue as SqsQueueTarget } from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { RustFunctionCode } from "./rust-function-layer";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";

interface ExternalLogPullerProps {
  logSources: string[];
  ingestionBucket: s3.IBucket;
}

export class ExternalLogPuller extends Construct {
  constructor(scope: Construct, id: string, props: ExternalLogPullerProps) {
    super(scope, id);

    // Managed log source types that support pulling.
    const pullerLogSourceTypes: string[] = ["office365"];
    const logSourceSecretMap: Record<string, string> = {};

    const func = new lambda.Function(this, "Function", {
      description: "[Matano] Pulls external logs for ingestion on a schedule.",
      runtime: lambda.Runtime.PROVIDED_AL2,
      code: RustFunctionCode.assetCode({ package: "log_puller" }),
      handler: "main",
      timeout: cdk.Duration.minutes(2),
      memorySize: 512,
      environment: {
        RUST_LOG: "warn,log_puller=info",
        PULLER_LOG_SOURCE_TYPES: JSON.stringify(pullerLogSourceTypes),
        INGESTION_BUCKET_NAME: props.ingestionBucket.bucketName,
      },
    });

    for (const logSourceName of props.logSources) {
      const secret = new secretsmanager.Secret(this, `Secret-${logSourceName}`, {
        description: `[Matano] Secret for log pulling for log source: ${logSourceName}`,
        secretObjectValue: {
          example_key: cdk.SecretValue.unsafePlainText("example_secret_value"),
        },
      });
      secret.grantRead(func);
      logSourceSecretMap[logSourceName] = secret.secretArn;
    }

    func.addEnvironment("LOG_SOURCE_TO_SECRET_ARN_MAP", JSON.stringify(logSourceSecretMap));

    props.ingestionBucket.grantWrite(func);

    const dlq = new sqs.Queue(this, "DLQ", {});

    const queue = new sqs.Queue(this, "Queue", {
      visibilityTimeout: cdk.Duration.seconds(130),
      deadLetterQueue: {
        queue: dlq,
        maxReceiveCount: 3,
      },
    });

    const scheduleRule = new events.Rule(this, "EventsRule", {
      description: "[Matano] Schedules the external Log puller lambda function.",
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
    });

    for (const logSourceName of props.logSources) {
      scheduleRule.addTarget(
        new SqsQueueTarget(queue, {
          message: events.RuleTargetInput.fromObject({
            time: events.EventField.time,
            log_source_name: logSourceName,
          }),
        })
      );
    }

    func.addEventSource(
      new SqsEventSource(queue, {
        batchSize: 10000,
        maxBatchingWindow: cdk.Duration.seconds(20),
        reportBatchItemFailures: true,
      })
    );
  }
}
