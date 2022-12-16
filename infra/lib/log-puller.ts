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
import { fail } from "./utils";

interface ExternalLogPullerProps {
  logSources: string[];
  ingestionBucket: s3.IBucket;
}

// Managed log source types that support pulling.
export const PULLER_LOG_SOURCE_TYPES: string[] = [
  "o365",
  "enrich_abusech_urlhaus",
  "enrich_abusech_malware_bazaar",
  "enrich_abusech_threatfox",
];
const LOG_SOURCE_RATES: Record<string, cdk.Duration> = {
  o365: cdk.Duration.minutes(1),
  enrich_abusech_urlhaus: cdk.Duration.minutes(5),
  enrich_abusech_malware_bazaar: cdk.Duration.hours(1),
  enrich_abusech_threatfox: cdk.Duration.hours(1),
};

export class ExternalLogPuller extends Construct {
  function: lambda.Function;
  constructor(scope: Construct, id: string, props: ExternalLogPullerProps) {
    super(scope, id);

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
        PULLER_LOG_SOURCE_TYPES: JSON.stringify(PULLER_LOG_SOURCE_TYPES),
        INGESTION_BUCKET_NAME: props.ingestionBucket.bucketName,
      },
    });
    this.function = func;

    for (const logSourceName of props.logSources) {
      let placeholder = {};
      let placeholder_val = cdk.SecretValue.unsafePlainText("<placeholder>");
      if (logSourceName.startsWith("o365")) {
        placeholder = {
          client_secret: placeholder_val,
        };
      } else {
        placeholder = {
          placeholder_key: placeholder_val,
        };
      }
      const secret = new secretsmanager.Secret(this, `Secret-${logSourceName}`, {
        description: `[Matano] ${logSourceName} - log pulling secret`,
        secretObjectValue: placeholder,
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

    const scheduleRules: Record<string, events.Rule> = {};
    for (const logSourceName of props.logSources) {
      const [_, rate] =
        Object.entries(LOG_SOURCE_RATES).find(([k, _]) => logSourceName.startsWith(k)) ?? fail("Invalid log source.");
      let scheduleRule;
      if (Object.keys(scheduleRules).includes(rate.toSeconds().toString())) {
        scheduleRule = scheduleRules[rate.toSeconds()];
      } else {
        scheduleRule = new events.Rule(this, `EventsRule-${rate.toSeconds()}`, {
          description: "[Matano] Schedules the external Log puller lambda function.",
          schedule: events.Schedule.rate(rate),
        });
        scheduleRules[rate.toSeconds()] = scheduleRule;
      }

      scheduleRule.addTarget(
        new SqsQueueTarget(queue, {
          message: events.RuleTargetInput.fromObject({
            time: events.EventField.time,
            log_source_name: logSourceName,
            rate_minutes: rate.toMinutes(),
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
