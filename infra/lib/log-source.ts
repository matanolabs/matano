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
}

interface MatanoLogSourceProps {
  config: LogSourceConfig;
  defaultSourceBucket: s3.Bucket;
  realtimeTopic: sns.Topic;
  lakeIngestionLambda: lambda.Function;
  resolvedSchema: any;
}

const MATANO_GLUE_DATABASE_NAME = "matano";
export class MatanoLogSource extends Construct {
  name: string;
  constructor(scope: Construct, id: string, props: MatanoLogSourceProps) {
    super(scope, id);

    const { name: logSourceName, ingest: ingestConfig } = props.config;
    const s3SourceConfig = ingestConfig?.s3_source;
    // const sourceBucket =
    //   s3SourceConfig == null
    //     ? props.defaultSourceBucket
    //     : s3.Bucket.fromBucketName(this, "SourceBucket", s3SourceConfig.bucket_name);

    this.name = logSourceName;

    const matanoIcebergTable = new MatanoIcebergTable(this, "MatanoIcebergTable", {
      logSourceName,
      schema: props.resolvedSchema,
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
