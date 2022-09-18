import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { LogSourceConfig, MatanoLogSource } from "./log-source";

interface MatanoS3SourcesProps {
  logSources: MatanoLogSource[];
  sourcesIngestionTopic: sns.Topic;
}

/** Configures custom S3 sources (BYOB), primarily notifications. */
export class MatanoS3Sources extends Construct {
  constructor(scope: Construct, id: string, props: MatanoS3SourcesProps) {
    super(scope, id);

    type FilterSource = {
      bucket_name: string;
      key_prefixes: string[];
    };
    const finalSources: FilterSource[] = [];

    // Get sources with custom sources and collect them into buckets + associated prefixes
    for (const logSource of props.logSources) {
      const { bucket_name, key_prefix } = logSource?.sourceConfig?.ingest?.s3_source ?? {};

      if (!bucket_name || !key_prefix) {
        continue;
      }

      if (!finalSources.map((ls) => ls?.bucket_name).includes(bucket_name)) {
        finalSources.push({ bucket_name: bucket_name!!, key_prefixes: [key_prefix!!] });
      } else {
        finalSources.find((ls) => ls?.bucket_name === bucket_name)!!.key_prefixes.push(key_prefix);
      }
    }

    for (const finalSource of finalSources) {
      const importedBucket = s3.Bucket.fromBucketName(
        this,
        `ImportedSourcesBucket-${finalSource!!.bucket_name!!}`,
        finalSource!!.bucket_name!!
      );

      const filters: s3.NotificationKeyFilter[] = finalSource?.key_prefixes.map((p) => ({ prefix: p }));

      importedBucket.addEventNotification(
        s3.EventType.OBJECT_CREATED_PUT,
        new s3n.SnsDestination(props.sourcesIngestionTopic),
        ...filters
      );
    }
  }
}
