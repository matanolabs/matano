import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as glue from "aws-cdk-lib/aws-glue";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";


export const MATANO_DATABASE_NAME = "matano";
interface DPCommonStackProps extends MatanoStackProps {
}
export class DPCommonStack extends MatanoStack {

  rawEventsBucketWithNotifications: S3BucketWithNotifications;
  outputEventsBucketWithNotifications: S3BucketWithNotifications;
  matanoIngestionBucket: S3BucketWithNotifications;
  matanoLakeStorageBucket: S3BucketWithNotifications;

  constructor(scope: Construct, id: string, props: DPCommonStackProps) {
    super(scope, id, props);

    this.matanoIngestionBucket = new S3BucketWithNotifications(this, "MatanoIngestionBucket", {
    });

    this.matanoLakeStorageBucket = new S3BucketWithNotifications(this, "MatanoLakeStorageBucket", {
      queueProps: {
        visibilityTimeout: cdk.Duration.seconds(185),
      },
      s3Filters: [
        { prefix: "lake", suffix: "parquet" },
      ],
    });

    this.rawEventsBucketWithNotifications = new S3BucketWithNotifications(this, "RawEventsBucket", {
      // bucketName: "matano-raw-events",
    });

    this.outputEventsBucketWithNotifications = new S3BucketWithNotifications(this, "OutputEventsBucket", {
      // bucketName: "matano-output-events",
      queueProps: {
        visibilityTimeout: cdk.Duration.seconds(185),
      },
      s3Filters: [
        { prefix: "lake", suffix: "parquet" },
      ]
    });

    cdk.Tags.of(this.rawEventsBucketWithNotifications.bucket).add("Name", "matano-raw-events");
    cdk.Tags.of(this.outputEventsBucketWithNotifications.bucket).add("Name", "matano-output-events");

    const matanoDatabase = new glue.CfnDatabase(this, "MatanoDatabase", {
      databaseInput: {
        name: MATANO_DATABASE_NAME,
        description: "Glue database storing Matano Iceberg tables.",
        locationUri: `s3://${this.outputEventsBucketWithNotifications.bucket.bucketName}/lake`,
      },
      catalogId: cdk.Aws.ACCOUNT_ID,
    });

  }
}
