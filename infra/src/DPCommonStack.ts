import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";
import * as athena from "aws-cdk-lib/aws-athena";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { Bucket, BlockPublicAccess } from "aws-cdk-lib/aws-s3";
import { Topic } from "aws-cdk-lib/aws-sns";

export const MATANO_DATABASE_NAME = "matano";
interface DPCommonStackProps extends MatanoStackProps {}
export class DPCommonStack extends MatanoStack {
  matanoIngestionBucket: S3BucketWithNotifications;
  matanoLakeStorageBucket: S3BucketWithNotifications;
  realtimeBucket: Bucket;
  realtimeBucketTopic: Topic;

  constructor(scope: Construct, id: string, props: DPCommonStackProps) {
    super(scope, id, props);

    this.matanoIngestionBucket = new S3BucketWithNotifications(this, "MatanoIngestionBucket", {
      bucketProps: {
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      }
    });

    this.matanoLakeStorageBucket = new S3BucketWithNotifications(this, "MatanoLakeStorageBucket", {
      bucketProps: {
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      },
      queueProps: {
        visibilityTimeout: cdk.Duration.seconds(185),
      },
      s3Filters: [{ prefix: "lake", suffix: "parquet" }],
    });

    this.realtimeBucket = new Bucket(this, "MatanoRealtimeBucket", {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
    });
    this.realtimeBucketTopic = new Topic(this, "MatanoRealtimeBucketNotifications", {
      displayName: "MatanoRealtimeBucketNotifications",
    });

    const matanoDatabase = new glue.CfnDatabase(this, "MatanoDatabase", {
      databaseInput: {
        name: MATANO_DATABASE_NAME,
        description: "Glue database storing Matano Iceberg tables.",
        locationUri: `s3://${this.matanoLakeStorageBucket.bucket.bucketName}/lake`,
      },
      catalogId: cdk.Aws.ACCOUNT_ID,
    });

    const matanoAthenaResultsBucket = new s3.Bucket(this, "MatanoAthenaResults");
    const matanoAthenaWorkgroup = new athena.CfnWorkGroup(this, "MatanoAthenaWorkGroup", {
      name: "matano",
      description: "[Matano] Matano Athena Work Group.",
      workGroupConfiguration: {
        engineVersion: {
          selectedEngineVersion: "Athena engine version 2",
        },
        resultConfiguration: {
          outputLocation: `s3://${matanoAthenaResultsBucket.bucketName}/results`,
        },
      },
    });

    this.humanCfnOutput("MatanoIngestionS3BucketName", {
      value: this.matanoIngestionBucket.bucket.bucketName,
      description:
        "The name of the S3 Bucket used for Matano ingestion. See https://www.matano.dev/docs/log-sources/ingestion",
    });

    this.humanCfnOutput("MatanoLakeStorageS3BucketName", {
      value: this.matanoLakeStorageBucket.bucket.bucketName,
      description:
        "The name of the S3 Bucket used for long term storage backing your data lake. See https://www.matano.dev/docs/tables/querying",
    });

    // important: to prevent output deletion
    this.exportValue(this.matanoIngestionBucket.topic.topicArn);
  }
}
