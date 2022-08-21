import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as firehose from "@aws-cdk/aws-kinesisfirehose-alpha";
import * as kinesisDestinations from "@aws-cdk/aws-kinesisfirehose-destinations-alpha";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { MatanoIcebergTable } from "../lib/iceberg";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { MatanoStack } from "./MatanoStack";
import { resolveSchema } from "./schema";

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
  outputBucket: s3.Bucket;
}

const MATANO_GLUE_DATABASE_NAME = "matano";
export class MatanoLogSource extends Construct {
  constructor(scope: Construct, id: string, props: MatanoLogSourceProps) {
    super(scope, id);

    const stack = cdk.Stack.of(this) as MatanoStack;

    const { name: logSourceName, schema, ingest: ingestConfig } = props.config;
    const s3SourceConfig = ingestConfig?.s3_source;
    // const sourceBucket =
    //   s3SourceConfig == null
    //     ? props.defaultSourceBucket
    //     : s3.Bucket.fromBucketName(this, "SourceBucket", s3SourceConfig.bucket_name);

    const resolvedSchema = resolveSchema(schema?.ecs_field_names, schema?.fields);

    const matanoIcebergTable = new MatanoIcebergTable(this, "MatanoIcebergTable", {
      logSourceName,
      schema: resolvedSchema,
      icebergS3BucketName: props.outputBucket.bucketName,
    });

  }
}
