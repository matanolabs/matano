import * as path from "path"
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as firehose from "@aws-cdk/aws-kinesisfirehose-alpha";
import * as kinesisDestinations from "@aws-cdk/aws-kinesisfirehose-destinations-alpha";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";
import { MatanoIcebergTable } from "../lib/iceberg";
import { getDirectories } from "../lib/utils";
import { readConfig } from "../lib/detections";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";
import { KafkaTopic } from "../lib/KafkaTopic";
import { IKafkaCluster, KafkaCluster } from "../lib/KafkaCluster";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { MatanoLogSource } from "../lib/log-source";

export const MATANO_DATABASE_NAME = "matano";

interface DPStorageStackProps extends MatanoStackProps {
  kafkaCluster?: IKafkaCluster;
}

export class DPStorageStack extends MatanoStack {
  rawEventsBucketWithNotifications: S3BucketWithNotifications;
  outputEventsBucketWithNotifications: S3BucketWithNotifications;
  constructor(scope: Construct, id: string, props: DPStorageStackProps) {
    super(scope, id, props);

    this.rawEventsBucketWithNotifications = new S3BucketWithNotifications(this, "RawEventsBucket", {
      // bucketName: "matano-raw-events",
    });

    this.outputEventsBucketWithNotifications = new S3BucketWithNotifications(this, "OutputEventsBucket", {
      // bucketName: "matano-output-events",
      s3Filters: [
        { prefix: "lake", suffix: "parquet" },
      ]
    });

    const matanoDatabase = new glue.CfnDatabase(this, "MatanoDatabase", {
      databaseInput: {
        name: MATANO_DATABASE_NAME,
        description: "Glue database storing Matano Iceberg tables.",
        locationUri: `s3://${this.outputEventsBucketWithNotifications.bucketName}/lake`,
      },
      catalogId: cdk.Fn.ref("AWS::AccountId"),
    });

    const firehoseRole = new iam.Role(this, "MatanoFirehoseRole", {
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com", {
        conditions: {
          "StringEquals": { "sts:ExternalId": cdk.Stack.of(this).account }
        },
      }),
    });

    console.log(this.matanoUserDirectory);

    const logSourcesDirectory = path.join(this.matanoUserDirectory, "log_sources");
    const logSources = getDirectories(logSourcesDirectory);

    for (const logSource of logSources) {
      new MatanoLogSource(this, `MatanoLogSource${logSource}`, {
        logSourceDirectory: path.join(logSourcesDirectory, logSource),
        outputBucket: this.outputEventsBucketWithNotifications,
        firehoseRole,
        kafkaCluster: props.kafkaCluster,
      })
    }

  }
}
