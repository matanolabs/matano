import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as glue from "aws-cdk-lib/aws-glue";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { Bucket } from "aws-cdk-lib/aws-s3";
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

    this.matanoIngestionBucket = new S3BucketWithNotifications(this, "MatanoIngestionBucket", {});
    this.exportValue(this.matanoIngestionBucket.bucket.bucketName);

    this.matanoLakeStorageBucket = new S3BucketWithNotifications(this, "MatanoLakeStorageBucket", {
      queueProps: {
        visibilityTimeout: cdk.Duration.seconds(185),
      },
      s3Filters: [{ prefix: "lake", suffix: "parquet" }],
    });

    this.realtimeBucket = new Bucket(this, "MatanoRealtimeBucket");
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
  }
}
