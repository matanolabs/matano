import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { PythonFunction } from "@aws-cdk/aws-lambda-python-alpha";
import {
  SelfManagedKafkaEventSource,
  AuthenticationMethod,
  ManagedKafkaEventSource,
  SelfManagedKafkaEventSourceProps,
  ManagedKafkaEventSourceProps,
  SqsEventSource
} from "aws-cdk-lib/aws-lambda-event-sources";
import * as glue from "aws-cdk-lib/aws-glue";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { KafkaCluster } from "../lib/KafkaCluster";
import { KafkaTopic } from "../lib/KafkaTopic";
import { NodejsFunction, NodejsFunctionProps } from "aws-cdk-lib/aws-lambda-nodejs";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";


export const MATANO_DATABASE_NAME = "matano";

// The list of Kafka brokers
const bootstrapServers = ["present-tadpole-14955-us1-kafka.upstash.io:9092"];
const logsources = ["coredns"];

interface DPCommonStackProps extends MatanoStackProps {
}
export class DPCommonStack extends MatanoStack {

  rawEventsBucketWithNotifications: S3BucketWithNotifications;
  outputEventsBucketWithNotifications: S3BucketWithNotifications;
  kafkaCluster: KafkaCluster;

  constructor(scope: Construct, id: string, props: DPCommonStackProps) {
    super(scope, id, props);

    this.kafkaCluster = new KafkaCluster(this, "KafkaCluster", {
      clusterName: "matano-msk-cluster",
      clusterType: this.matanoConfig.kafka_cluster_type,
      vpc: this.matanoVpc,
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
      catalogId: cdk.Fn.ref("AWS::AccountId"),
    });

  }
}
