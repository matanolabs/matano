import * as path from "path";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as firehose from "@aws-cdk/aws-kinesisfirehose-alpha";
import * as kinesisDestinations from "@aws-cdk/aws-kinesisfirehose-destinations-alpha";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";
import { IcebergMetadata, MatanoIcebergTable } from "../lib/iceberg";
import { getDirectories } from "../lib/utils";
import { readConfig } from "../lib/detections";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";
import { KafkaTopic } from "../lib/KafkaTopic";
import { IKafkaCluster, KafkaCluster } from "../lib/KafkaCluster";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { MatanoLogSource } from "../lib/log-source";

interface DPMainStackProps extends MatanoStackProps {
  rawEventsBucket: S3BucketWithNotifications;
  outputEventsBucket: S3BucketWithNotifications;
  kafkaCluster?: IKafkaCluster;
}

export class DPMainStack extends MatanoStack {
  constructor(scope: Construct, id: string, props: DPMainStackProps) {
    super(scope, id, props);

    const firehoseRole = new iam.Role(this, "MatanoFirehoseRole", {
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com", {
        conditions: {
          StringEquals: { "sts:ExternalId": cdk.Stack.of(this).account },
        },
      }),
    });

    console.log(this.matanoUserDirectory);

    const logSourcesDirectory = path.join(this.matanoUserDirectory, "log_sources");
    const logSources = getDirectories(logSourcesDirectory);

    for (const logSource of logSources) {
      new MatanoLogSource(this, `MatanoLogSource${logSource}`, {
        logSourceDirectory: path.join(logSourcesDirectory, logSource),
        outputBucket: props.outputEventsBucket,
        firehoseRole,
        kafkaCluster: props.kafkaCluster,
      });
    }

    new IcebergMetadata(this, "IcebergMetadata", {
      outputBucket: props.outputEventsBucket,
    });
  }
}
