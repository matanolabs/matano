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
import { readConfig } from "../lib/utils";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";
import { KafkaTopic } from "../lib/KafkaTopic";
import { IKafkaCluster, KafkaCluster } from "../lib/KafkaCluster";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { MatanoLogSource } from "../lib/log-source";
import { MatanoDetections } from "../lib/detections";

interface DPMainStackProps extends MatanoStackProps {
  rawEventsBucket: S3BucketWithNotifications;
  outputEventsBucket: S3BucketWithNotifications;
  kafkaCluster: IKafkaCluster;
}

export class DPMainStack extends MatanoStack {
  constructor(scope: Construct, id: string, props: DPMainStackProps) {
    super(scope, id, props);

    const firehoseRole = new iam.Role(this, "MatanoFirehoseRole", {
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com", {
        conditions: {
          StringEquals: { "sts:ExternalId": cdk.Aws.ACCOUNT_ID },
        },
      }),
    });

    const logSourcesDirectory = path.join(this.matanoUserDirectory, "log_sources");
    const logSources = getDirectories(logSourcesDirectory);

    for (const logSource of logSources) {
      new MatanoLogSource(this, `MatanoLogSource${logSource}`, {
        logSourceDirectory: path.join(logSourcesDirectory, logSource),
        outputBucket: props.outputEventsBucket.bucket,
        firehoseRole,
        kafkaCluster: props.kafkaCluster,
      });
    }

    new IcebergMetadata(this, "IcebergMetadata", {
      outputBucket: props.outputEventsBucket,
    });

    // new MatanoDetections(this, "MatanoDetections", {
    //   rawEventsBucket: props.rawEventsBucket.bucket,
    // });

    // const transformerLambda = new NodejsFunction(this, "TransformerLambda", {
    //   functionName: "MatanoTransformerLambdaFunction",
    //   entry: "../lib/js/vrl-transform/transform.ts",
    //   depsLockFilePath: "../lib/js/package-lock.json",
    //   runtime: lambda.Runtime.NODEJS_14_X,
    //   ...vpcProps,
    //   allowPublicSubnet: true,
    //   bundling: {
    //     externalModules: ["aws-sdk", "@matano/vrl-transform-bindings"],
    //     // nodeModules: ["@matano/vrl-transform-bindings"],
    //   },
    //   timeout: cdk.Duration.seconds(30),
    //   initialPolicy: [
    //     new iam.PolicyStatement({
    //       actions: ["secretsmanager:*", "kafka:*", "kafka-cluster:*", "dynamodb:*", "s3:*", "athena:*", "glue:*"],
    //       resources: ["*"],
    //     }),
    //   ],
    // });
    // transformerLambda.addEventSource(
    //   new SqsEventSource(
    //     props.rawEventsBucketWithNotifications.queue,
    //     {
    //       batchSize: 100,
    //       maxBatchingWindow: cdk.Duration.seconds(1),
    //     }
    //   )
    // );

  }
}
