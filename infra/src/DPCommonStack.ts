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

  constructor(scope: Construct, id: string, props: DPCommonStackProps) {
    super(scope, id, props);

    const cluster = new KafkaCluster(this, "KafkaCluster", {
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

    const matanoDatabase = new glue.CfnDatabase(this, "MatanoDatabase", {
      databaseInput: {
        name: MATANO_DATABASE_NAME,
        description: "Glue database storing Matano Iceberg tables.",
        locationUri: `s3://${this.outputEventsBucketWithNotifications.bucketName}/lake`,
      },
      catalogId: cdk.Fn.ref("AWS::AccountId"),
    });

    // const forwarderLambda = new PythonFunction(this, "MatanoForwarderLambda", {
    //   // functionName: "MatanoForwarderLambdaFunction",
    //   entry: "../lib/python/matano_forwarder",
    //   index: "matano_forwarder_lambda/main.py",
    //   handler: "lambda_handler",
    //   runtime: lambda.Runtime.PYTHON_3_9,
    //   memorySize: 1024,
    //   timeout: cdk.Duration.seconds(100),
    //   environment: {
    //   },
    //   initialPolicy: [
    //     new iam.PolicyStatement({
    //       actions: ["secretsmanager:*", "kafka:*", "kafka-cluster:*", "dynamodb:*", "s3:*", "athena:*", "glue:*"],
    //       resources: ["*"],
    //     }),
    //   ],
    // });

    // const vpcProps: Partial<NodejsFunctionProps> | {} = {
    //   vpc: cluster.vpc,
    //   vpcSubnets: cluster.vpc.publicSubnets.map((subnet) => subnet.subnetId),
    //   securityGroups: [cluster.securityGroup],
    // };

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

    // const topics = ([] as string[]).concat(...logsources.map((l) => [l, `raw.${l}`])).map((topicName) => {
    //   const topic = new KafkaTopic(this, `${topicName}Topic`, {
    //     cluster: cluster,
    //     topicName,
    //     topicConfig: {
    //       numPartitions: 2,
    //       replicationFactor: 3,
    //     },
    //   });
    //   topic.node.addDependency(cluster);
    //   forwarderLambda.node.addDependency(topic);

    //   const kafkaSourceProps = {
    //     topic: topicName,
    //     batchSize: 10_000, // TODO
    //     startingPosition: lambda.StartingPosition.LATEST, // TODO
    //   };

    //   const kafkaSource = new ManagedKafkaEventSource({
    //     ...kafkaSourceProps,
    //     clusterArn: cluster.clusterArn,
    //   });

    //   forwarderLambda.addEventSource(kafkaSource);
    // });
  }
}
