import * as path from "path";
import * as fs from "fs";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import { MatanoIcebergTable } from "../lib/iceberg";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { MatanoStack } from "./MatanoStack";
import { resolveSchema } from "./schema";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { dataDirPath, fail, mergeDeep, readConfig } from "./utils";

export const MATANO_DATABASE_NAME = "matano";

export interface LogSourceConfig {
  name: string;
  schema?: {
    ecs_field_names?: string[],
    fields?: any[],
  },
  ingest?: {
    s3_source?: {
      bucket_name?: string;
      key_prefix?: string;
      expand_records_from_object?: string;
    };
  }
  transform?: {
    vrl?: string
  }
  managed?: {
    type?: string;
  }
  [key: string]: any;
}

interface MatanoLogSourceProps {
  config: LogSourceConfig;
  realtimeTopic: sns.Topic;
  lakeIngestionLambda: lambda.Function;
}

const MANAGED_LOG_SOURCE_CATEGORY_MAP: Record<string, string> = {
  "aws_cloudtrail": "aws",
  "matano_alerts": "matano_alerts", // doesn't really make sense but OK
}

function getCategoryForManagedLogSourceType(logSourceType: string) {
  return MANAGED_LOG_SOURCE_CATEGORY_MAP[logSourceType];
}

const MANAGED_LOG_SOURCES_DIR = path.join(dataDirPath, "managed");

export class MatanoLogSource extends Construct {
  name: string;
  schema: Record<string, any>;
  sourceConfig: LogSourceConfig;
  constructor(scope: Construct, id: string, props: MatanoLogSourceProps) {
    super(scope, id);

    const { name: logSourceName, ingest: ingestConfig } = props.config;
    this.name = logSourceName;

    if (props.config?.managed) {
      const managedLogSourceType = props.config?.managed?.type?.toLowerCase();
      if (!managedLogSourceType) {
        fail("Invalid Managed Log source type: cannot be empty");
      }
      const managedConfigPath = path.join(MANAGED_LOG_SOURCES_DIR, managedLogSourceType);
      if (!fs.existsSync(managedConfigPath)) {
        fail(`The managed log source type: ${managedLogSourceType} does not exist. Available managed log sources: ${JSON.stringify(Object.keys(MANAGED_LOG_SOURCE_CATEGORY_MAP))}`);
      }

      const category = getCategoryForManagedLogSourceType(managedLogSourceType);
      if (!props.config.name.startsWith(category)) {
        fail(`Since you are using the managed log source type: ${managedLogSourceType}, your log source name must be prefixed with ${category}. Please rename your log source as: ${category}_${props.config.name}`);
      }
      const managedConfig = readConfig(managedConfigPath, "log_source.yml");
      this.sourceConfig = mergeDeep(props.config, managedConfig);
    } else {
      this.sourceConfig = props.config;
    }

    this.schema = resolveSchema(this.sourceConfig.schema?.ecs_field_names, this.sourceConfig.schema?.fields);

    const matanoIcebergTable = new MatanoIcebergTable(this, "MatanoIcebergTable", {
      tableName: logSourceName,
      schema: this.schema,
    });

    const ingestionDlq = new sqs.Queue(this, "LakeIngestionDLQ", {
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const ingestionQueue = new sqs.Queue(this, "LakeIngestionQueue", {
      deadLetterQueue: {
        queue: ingestionDlq,
        maxReceiveCount: 3,
      },
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    props.realtimeTopic.addSubscription(
      new SqsSubscription(ingestionQueue, {
        rawMessageDelivery: true,
        filterPolicy: {
          "log_source": sns.SubscriptionFilter.stringFilter({ allowlist: [logSourceName] })
        },
      }),
    );

    props.lakeIngestionLambda.addEventSource(new SqsEventSource(ingestionQueue, {
      batchSize: 10,
      maxBatchingWindow: cdk.Duration.seconds(20),
    }));

  }
}
