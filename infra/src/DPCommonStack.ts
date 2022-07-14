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
} from "aws-cdk-lib/aws-lambda-event-sources";

import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { S3BucketWithNotifications } from "./DPStorageStack";
import { KafkaCluster, MskClusterType } from "../lib/KafkaCluster";
import { KafkaTopic } from "../lib/KafkaTopic";

// The list of Kafka brokers
const bootstrapServers = ["present-tadpole-14955-us1-kafka.upstash.io:9092"];
const logsources = ["cloudtrail"];

interface DPCommonStackProps extends MatanoStackProps {
  rawEventsBucketWithNotifications: S3BucketWithNotifications;
  outputEventsBucketWithNotifications: S3BucketWithNotifications;
}
export class DPCommonStack extends MatanoStack {
  constructor(scope: Construct, id: string, props: DPCommonStackProps) {
    super(scope, id, props);

    const cluster = process.env.MATANO_KAFKA_SELFMANAGED
      ? KafkaCluster.fromSelfManagedAttributes(this, "Cluster", {
          bootstrapAddress: bootstrapServers.join(","),
          secretArn:
            "arn:aws:secretsmanager:us-west-2:903370141120:secret:apptrail-security-lake-test-kafka-secret-FCNzLo",
        })
      : new KafkaCluster(this, "MatanoKafkaCluster", {
          clusterName: "serverless-matanor-cluster",
          clusterType: MskClusterType.SERVERLESS,
        });

    const forwarderLambda = new PythonFunction(this, "MatanoForwarderLambda", {
      functionName: "MatanoForwarderLambdaFunction",
      entry: "../lambdas/MatanoForwarderLambda",
      index: "matano_forwarder_lambda/main.py",
      handler: "lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_9,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(100),
      environment: {
        USAGE_BUCKET_NAME: "dd",
      },
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:*", "kafka:*", "kafka-cluster:*", "dynamodb:*", "s3:*", "athena:*", "glue:*"],
          resources: ["*"],
        }),
      ],
    });

    const topics = ([] as string[]).concat(...logsources.map((l) => [l, `raw.${l}`])).map((topicName) => {
      const topic = new KafkaTopic(this, `${topicName}Topic`, {
        cluster: cluster,
        topicName,
        topicConfig: {
          numPartitions: 2,
          replicationFactor: 3,
        },
      });
      forwarderLambda.node.addDependency(topic);

      const kafkaSourceProps = {
        topic: topicName,
        batchSize: 10_000, // TODO
        startingPosition: lambda.StartingPosition.LATEST, // TODO
      };

      const kafkaSource =
        cluster.clusterType === "self-managed"
          ? new SelfManagedKafkaEventSource({
              ...kafkaSourceProps,
              authenticationMethod: AuthenticationMethod.SASL_SCRAM_256_AUTH,
              bootstrapServers: bootstrapServers,
              secret: cluster.secret,
            })
          : new ManagedKafkaEventSource({
              ...kafkaSourceProps,
              clusterArn: cluster.clusterArn,
            });

      forwarderLambda.addEventSource(kafkaSource);
    });
  }
}
