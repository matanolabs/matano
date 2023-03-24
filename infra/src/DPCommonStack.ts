import * as fs from "fs-extra";
import * as path from "path";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
import * as athena from "aws-cdk-lib/aws-athena";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { Bucket, BlockPublicAccess } from "aws-cdk-lib/aws-s3";
import { Topic } from "aws-cdk-lib/aws-sns";
import { IntegrationsStore } from "../lib/integrations-store";

export const MATANO_DATABASE_NAME = "matano";
interface DPCommonStackProps extends MatanoStackProps {}
export class DPCommonStack extends MatanoStack {
  matanoIngestionBucket: S3BucketWithNotifications;
  matanoLakeStorageBucket: S3BucketWithNotifications;
  realtimeBucket: Bucket;
  realtimeBucketTopic: Topic;
  integrationsStore: IntegrationsStore;
  alertTrackerTable: ddb.Table;
  matanoAthenaResultsBucket: s3.Bucket;
  transformerSidelineBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: DPCommonStackProps) {
    super(scope, id, props);

    this.matanoIngestionBucket = new S3BucketWithNotifications(this, "MatanoIngestionBucket", {
      bucketProps: {
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      },
    });

    // For delivering Cloudtrail, S3 access logs
    // TODO: ideally parse and add sourceArn, sourceAccount here
    this.matanoIngestionBucket.bucket.grantWrite(
      new iam.CompositePrincipal(
        new iam.ServicePrincipal("cloudtrail.amazonaws.com"),
        new iam.ServicePrincipal("logging.s3.amazonaws.com")
      )
    );
    this.matanoIngestionBucket.bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetBucketAcl"],
        principals: [new iam.ServicePrincipal("cloudtrail.amazonaws.com")],
        resources: [this.matanoIngestionBucket.bucket.bucketArn],
      })
    );

    this.alertTrackerTable = new ddb.Table(this, "MatanoAlertTrackingTable", {
      partitionKey: { name: "id", type: ddb.AttributeType.STRING },
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
    });

    this.matanoLakeStorageBucket = new S3BucketWithNotifications(this, "MatanoLakeStorageBucket", {
      bucketProps: {
        blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      },
      queueProps: {
        visibilityTimeout: cdk.Duration.seconds(185),
      },
      s3Filters: [{ prefix: "lake", suffix: "mtn_append.zstd.parquet" }],
    });

    this.realtimeBucket = new Bucket(this, "MatanoRealtimeBucket", {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [{ expiration: cdk.Duration.days(7) }],
    });
    this.realtimeBucketTopic = new Topic(this, "MatanoRealtimeBucketNotifications", {
      displayName: "MatanoRealtimeBucketNotifications",
    });

    const matanoDatabase = new glue.CfnDatabase(this, "MatanoDatabase", {
      databaseInput: {
        name: MATANO_DATABASE_NAME,
        description: "[Matano] Main Glue database storing Matano Iceberg tables.",
        locationUri: `s3://${this.matanoLakeStorageBucket.bucket.bucketName}/lake`,
      },
      catalogId: cdk.Aws.ACCOUNT_ID,
    });

    const matanoSystemDatabase = new glue.CfnDatabase(this, "MatanoSystemDatabase", {
      databaseInput: {
        name: "matano_system",
        description: "[Matano] Glue database storing temporary and system Matano Iceberg tables.",
        locationUri: `s3://${this.matanoLakeStorageBucket.bucket.bucketName}/lake`,
      },
      catalogId: cdk.Aws.ACCOUNT_ID,
    });

    this.matanoAthenaResultsBucket = new s3.Bucket(this, "MatanoAthenaResults", {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [{ expiration: cdk.Duration.days(60) }],
    });
    const matanoDefaultAthenaWorkgroup = new athena.CfnWorkGroup(this, "MatanoDefault", {
      name: "matano_default",
      description:
        "[Matano] Matano Default Athena Work Group. This is a preconfigured Athena workgroup. You can use it for querying.",
      workGroupConfiguration: {
        engineVersion: {
          selectedEngineVersion: "Athena engine version 3",
        },
        resultConfiguration: {
          outputLocation: `s3://${this.matanoAthenaResultsBucket.bucketName}/results/matano-default`,
        },
      },
    });

    const matanoSystemAthenaWorkgroup = new athena.CfnWorkGroup(this, "MatanoSystem", {
      name: "matano_system",
      description: "[Matano] Matano System Athena Work Group. Used for system queries such as table maintenance.",
      workGroupConfiguration: {
        engineVersion: {
          selectedEngineVersion: "Athena engine version 2",
        },
        resultConfiguration: {
          outputLocation: `s3://${this.matanoAthenaResultsBucket.bucketName}/results/matano-system`,
        },
      },
    });

    const matanoSystemV3AthenaWorkgroup = new athena.CfnWorkGroup(this, "MatanoSystemV3", {
      name: "matano_system_v3",
      description: "[Matano] Matano System Athena Work Group. Used for system queries such as table maintenance.",
      workGroupConfiguration: {
        engineVersion: {
          selectedEngineVersion: "Athena engine version 3",
        },
        resultConfiguration: {
          outputLocation: `s3://${this.matanoAthenaResultsBucket.bucketName}/results/matano-system_v3`,
        },
      },
    });

    const integrationsDir = path.join(this.matanoUserDirectory, "integrations");
    const usesIntegrations = fs.existsSync(integrationsDir);
    if (usesIntegrations) {
      this.integrationsStore = new IntegrationsStore(this, "MatanoIntegrationsStore", {});
    }

    this.transformerSidelineBucket = new Bucket(this, "MatanoTransformerSidelineBucket");

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

    this.humanCfnOutput("MatanoTransformerSidelineS3BucketName", {
      value: this.transformerSidelineBucket.bucketName,
      description: "The name of the S3 Bucket where erroring lines are sidelined.",
    });

    // important: to prevent output deletion
    this.exportValue(this.matanoIngestionBucket.topic.topicArn);
    this.exportValue(this.transformerSidelineBucket.bucketArn);
    this.exportValue(this.matanoIngestionBucket.bucket.bucketArn);
    this.exportValue(this.matanoAthenaResultsBucket.bucketArn);
  }
}
