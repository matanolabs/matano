import * as path from "path";
import * as fs from "fs";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";

import * as sns from "aws-cdk-lib/aws-sns";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { IcebergMetadata } from "../lib/iceberg";
import { getDirectories, readConfig } from "../lib/utils";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { MatanoLogSource, LogSourceConfig } from "../lib/log-source";
import { MatanoDetections } from "../lib/detections";
import { NodejsFunction, NodejsFunctionProps } from "aws-cdk-lib/aws-lambda-nodejs";
import { DockerImage } from "aws-cdk-lib";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { execSync } from "child_process";
import { SecurityGroup, SubnetType } from "aws-cdk-lib/aws-ec2";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { DataBatcher } from "../lib/data-batcher";
import { RustFunctionLayer } from '../lib/rust-function-layer';
import { LayerVersion } from "aws-cdk-lib/aws-lambda";
import { LakeIngestion } from "../lib/lake-ingestion";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";

interface DPMainStackProps extends MatanoStackProps {
  rawEventsBucket: S3BucketWithNotifications;
  outputEventsBucket: S3BucketWithNotifications;
}

export class DPMainStack extends MatanoStack {
  constructor(scope: Construct, id: string, props: DPMainStackProps) {
    super(scope, id, props);

    const logSourcesDirectory = path.join(this.matanoUserDirectory, "log_sources");
    const logSourceConfigs = getDirectories(logSourcesDirectory)
      .map((d) => path.join(logSourcesDirectory, d))
      .map((p) => readConfig(p, "log_source.yml") as LogSourceConfig);

    const rawEventsBatcher = new DataBatcher(this, "DataBatcher", {
      s3Bucket: props.rawEventsBucket,
    });

    const realtimeBucket = new s3.Bucket(this, "MatanoRealtimeBucket");
    const realtimeBucketTopic = new sns.Topic(this, "MatanoRealtimeBucketNotifications", {
      displayName: "MatanoRealtimeBucketNotifications"
    });

    const detections = new MatanoDetections(this, "MatanoDetections", {
      rawEventsBucket: props.rawEventsBucket.bucket,
    });

    const lakeIngestion = new LakeIngestion(this, "LakeIngestion", {
      outputBucketName: props.outputEventsBucket.bucket.bucketName,
      outputObjectPrefix: "lake",
    });

    for (const logSourceConfig of logSourceConfigs) {
      new MatanoLogSource(this, `MatanoLogSource${logSourceConfig.name}`, {
        config: logSourceConfig,
        defaultSourceBucket: props.rawEventsBucket.bucket,
        realtimeTopic: realtimeBucketTopic,
        lakeIngestionLambda: lakeIngestion.lakeIngestionLambda,
      });
    }

    new IcebergMetadata(this, "IcebergMetadata", {
      outputBucket: props.outputEventsBucket,
    });

    // const vrlBindingsPath = path.resolve(path.join("../lambdas/vrl-transform-bindings"));
    // const vrlBindingsLayer = new lambda.LayerVersion(this, "VRLBindingsLayer", {
    //   code: lambda.Code.fromAsset(vrlBindingsPath, {
    //     bundling: {
    //       image: DockerImage.fromBuild(vrlBindingsPath),
    //       command: [
    //         "bash",
    //         "-c",
    //         "npm install && mkdir -p /asset-output/nodejs/node_modules/@matano/vrl-transform-bindings/ts/ && cp -a ts/* /asset-output/nodejs/node_modules/@matano/vrl-transform-bindings/ts/",
    //       ],
    //     },
    //   }),
    //   compatibleRuntimes: [lambda.Runtime.NODEJS_14_X],
    //   license: "Apache-2.0",
    //   description: "A layer for NodeJS bindings to VRL.",
    // });

    // const transformerLambda = new NodejsFunction(this, "TransformerLambda", {
    //   functionName: "MatanoTransformerLambdaFunction",
    //   entry: "../lambdas/vrl-transform/transform.ts",
    //   depsLockFilePath: "../lambdas/package-lock.json",
    //   runtime: lambda.Runtime.NODEJS_14_X,
    //   layers: [vrlBindingsLayer],
    //   // ...lambdaVpcProps,
    //   allowPublicSubnet: true,
    //   bundling: {
    //     // target: "node14.8",
    //     // forceDockerBundling: true,
    //     externalModules: ["aws-sdk", "@matano/vrl-transform-bindings"],
    //   },
    //   environment: {
    //     RAW_EVENTS_BUCKET_NAME: props.rawEventsBucket.bucket.bucketName,
    //   },
    //   timeout: cdk.Duration.seconds(30),
    //   initialPolicy: [
    //     new iam.PolicyStatement({
    //       actions: ["secretsmanager:*", "kafka:*", "kafka-cluster:*", "dynamodb:*", "s3:*", "athena:*", "glue:*"],
    //       resources: ["*"],
    //     }),
    //   ],
    // });

    // // const firehoseWriterLambda = new NodejsFunction(this, "FirehoseWriterLambda", {
    // //   functionName: "MatanoFirehoseLambdaFunction",
    // //   entry: "../lambdas/vrl-transform/writer.ts",
    // //   depsLockFilePath: "../lambdas/package-lock.json",
    // //   runtime: lambda.Runtime.NODEJS_14_X,
    // //   layers: [vrlBindingsLayer],
    // //   // ...lambdaVpcProps,
    // //   allowPublicSubnet: true,
    // //   bundling: {
    // //     // target: "node14.8",
    // //     // forceDockerBundling: true,
    // //     externalModules: ["aws-sdk", "@matano/vrl-transform-bindings"],
    // //   },
    // //   environment: {
    // //     RAW_EVENTS_BUCKET_NAME: props.rawEventsBucket.bucket.bucketName,
    // //   },
    // //   timeout: cdk.Duration.seconds(30),
    // //   initialPolicy: [
    // //     new iam.PolicyStatement({
    // //       actions: ["secretsmanager:*", "kafka:*", "kafka-cluster:*", "dynamodb:*", "s3:*", "athena:*", "glue:*", "firehose:*"],
    // //       resources: ["*"],
    // //     }),
    // //   ],
    // // });


    // const logSourcesConfigurationPath = path.resolve(path.join("../lambdas/log_sources_configuration.json"));
    // fs.writeFileSync(logSourcesConfigurationPath, JSON.stringify(logSourceConfigs, null, 2));
    // const logSourcesConfigurationLayer = new lambda.LayerVersion(this, "LogSourcesConfigurationLayer", {
    //   code: lambda.Code.fromAsset("../lambdas", {
    //     bundling: {
    //       volumes: [
    //         {
    //           hostPath: logSourcesConfigurationPath,
    //           containerPath: "/asset-input/log_sources_configuration.json",
    //         },
    //       ],
    //       image: DockerImage.fromBuild(vrlBindingsPath),
    //       command: [
    //         "bash",
    //         "-c",
    //         "cp /asset-input/log_sources_configuration.json /asset-output/log_sources_configuration.json",
    //       ],
    //     },
    //   }),
    //   description: "A layer for Matano Log Source Configurations.",
    // });
    // transformerLambda.addLayers(logSourcesConfigurationLayer);
    // // firehoseWriterLambda.addLayers(logSourcesConfigurationLayer);


    // const forwarderLambda = new NodejsFunction(this, "ForwarderLambda", {
    //   functionName: "MatanoForwarderLambdaFunction",
    //   entry: "../lambdas/vrl-transform/forward.ts",
    //   depsLockFilePath: "../lambdas/package-lock.json",
    //   runtime: lambda.Runtime.NODEJS_14_X,
    //   layers: [vrlBindingsLayer, logSourcesConfigurationLayer],
    //   // ...lambdaVpcProps,
    //   allowPublicSubnet: true,
    //   bundling: {
    //     // target: "node14.8",
    //     // forceDockerBundling: true,
    //     externalModules: ["aws-sdk", "@matano/vrl-transform-bindings"],
    //   },
    //   environment: {
    //     RAW_EVENTS_BUCKET_NAME: props.rawEventsBucket.bucket.bucketName,
    //     KAFKAJS_NO_PARTITIONER_WARNING: "1",
    //     // BOOTSTRAP_ADDRESS: kafkaCluster.bootstrapAddress,
    //   },
    //   timeout: cdk.Duration.seconds(30),
    //   initialPolicy: [
    //     new iam.PolicyStatement({
    //       actions: ["secretsmanager:*", "kafka:*", "kafka-cluster:*", "dynamodb:*", "s3:*", "athena:*", "glue:*"],
    //       resources: ["*"],
    //     }),
    //   ],
    // });
    // forwarderLambda.addEventSource(
    //   new SqsEventSource(props.rawEventsBucket.queue, {
    //     batchSize: 100,
    //     maxBatchingWindow: cdk.Duration.seconds(1),
    //   })
    // );
  }
}
