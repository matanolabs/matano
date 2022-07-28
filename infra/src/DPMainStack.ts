import * as path from "path";
import * as fs from "fs";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { IcebergMetadata } from "../lib/iceberg";
import { getDirectories, readConfig } from "../lib/utils";
import { IKafkaCluster } from "../lib/KafkaCluster";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { MatanoLogSource, LogSourceConfig } from "../lib/log-source";
import { MatanoDetections } from "../lib/detections";
import { NodejsFunction, NodejsFunctionProps } from "aws-cdk-lib/aws-lambda-nodejs";
import { DockerImage } from "aws-cdk-lib";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { execSync } from "child_process";
import { SecurityGroup } from "aws-cdk-lib/aws-ec2";
import { Bucket } from "aws-cdk-lib/aws-s3";

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
    const logSourceConfigs = getDirectories(logSourcesDirectory)
      .map((d) => path.join(logSourcesDirectory, d))
      .map((p) => readConfig(p, "log_source.yml") as LogSourceConfig);
    
    const kafkaCluster = props.kafkaCluster;

    new IcebergMetadata(this, "IcebergMetadata", {
      outputBucket: props.outputEventsBucket,
    });

    new MatanoDetections(this, "MatanoDetections", {
      rawEventsBucket: props.rawEventsBucket.bucket,
      kafkaCluster: props.kafkaCluster,
    });

    const lambdaVpcProps: Partial<NodejsFunctionProps> | {} =
      kafkaCluster.clusterType != "self-managed"
        ? {
            vpc: kafkaCluster.vpc,
            vpcSubnets: kafkaCluster.vpc.publicSubnets.map((subnet) => subnet.subnetId),
            securityGroups: [
              kafkaCluster.securityGroup,
              new SecurityGroup(this, "LambdaSG", {
                vpc: kafkaCluster.vpc,
                allowAllOutbound: true,
              }),
            ],
          }
        : {};

    const vrlBindingsPath = path.resolve(path.join("../lambdas/vrl-transform-bindings"));
    const vrlBindingsLayer = new lambda.LayerVersion(this, "VRLBindingsLayer", {
      code: lambda.Code.fromAsset(vrlBindingsPath, {
        assetHashType: cdk.AssetHashType.OUTPUT,
        bundling: {
          volumes: [
            {
              hostPath: vrlBindingsPath,
              containerPath: "/asset-input",
            },
          ],
          image: DockerImage.fromBuild(vrlBindingsPath),
          command: [
            "bash",
            "-c",
            "npm install && mkdir -p /asset-output/nodejs/node_modules/@matano/vrl-transform-bindings/ts/ && cp -a ts/* /asset-output/nodejs/node_modules/@matano/vrl-transform-bindings/ts/",
          ],
          // local: {
          //   tryBundle(outputDir, options) {
          //     execSync(
          //       `cd ${vrlBindingsPath} && npm install && mkdir -p ${outputDir}/nodejs/node_modules/@matano/vrl-transform-bindings/ts/ && cp -a ts/*  ${outputDir}/nodejs/node_modules/@matano/vrl-transform-bindings/ts/ && cd -`,
          //       { stdio: "pipe" }
          //     );
          //     return true;
          //   },
          // },
        },
      }),
      compatibleRuntimes: [lambda.Runtime.NODEJS_14_X],
      license: "Apache-2.0",
      description: "A layer for NodeJS bindings to VRL.",
    });

    const transformerLambda = new NodejsFunction(this, "TransformerLambda", {
      functionName: "MatanoTransformerLambdaFunction",
      entry: "../lambdas/vrl-transform/transform.ts",
      depsLockFilePath: "../lambdas/package-lock.json",
      runtime: lambda.Runtime.NODEJS_14_X,
      layers: [vrlBindingsLayer],
      ...lambdaVpcProps,
      allowPublicSubnet: true,
      bundling: {
        // target: "node14.8",
        // forceDockerBundling: true,
        externalModules: ["aws-sdk", "@matano/vrl-transform-bindings"],
      },
      environment: {
        RAW_EVENTS_BUCKET_NAME: props.rawEventsBucket.bucket.bucketName,
        KAFKAJS_NO_PARTITIONER_WARNING: "1",
        BOOTSTRAP_ADDRESS: kafkaCluster.bootstrapAddress,
      },
      timeout: cdk.Duration.seconds(30),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:*", "kafka:*", "kafka-cluster:*", "dynamodb:*", "s3:*", "athena:*", "glue:*"],
          resources: ["*"],
        }),
      ],
    });

    for (const logSourceConfig of logSourceConfigs) {
      new MatanoLogSource(this, `MatanoLogSource${logSourceConfig.name}`, {
        config: logSourceConfig,
        defaultSourceBucket: props.rawEventsBucket.bucket,
        outputBucket: props.outputEventsBucket.bucket,
        transformLambda: transformerLambda,
        firehoseRole,
        kafkaCluster: props.kafkaCluster,
      });
    }


    const logSourcesConfigurationPath = path.resolve(path.join("../lambdas/log_sources_configuration.json"));
    fs.writeFileSync(logSourcesConfigurationPath, JSON.stringify(logSourceConfigs, null, 2));
    const logSourcesConfigurationLayer = new lambda.LayerVersion(this, "LogSourcesConfigurationLayer", {
      code: lambda.Code.fromAsset("../lambdas", {
        assetHashType: cdk.AssetHashType.OUTPUT,
        bundling: {
          volumes: [
            {
              hostPath: logSourcesConfigurationPath,
              containerPath: "/asset-input/log_sources_configuration.json",
            },
          ],
          image: DockerImage.fromBuild(vrlBindingsPath),
          command: [
            "bash",
            "-c",
            "cp /asset-input/log_sources_configuration.json /asset-output/log_sources_configuration.json",
          ],
        },
      }),
      description: "A layer for Matano Log Source Configurations.",
    });

    const forwarderLambda = new NodejsFunction(this, "ForwarderLambda", {
      functionName: "MatanoForwarderLambdaFunction",
      entry: "../lambdas/vrl-transform/forward.ts",
      depsLockFilePath: "../lambdas/package-lock.json",
      runtime: lambda.Runtime.NODEJS_14_X,
      layers: [vrlBindingsLayer, logSourcesConfigurationLayer],
      ...lambdaVpcProps,
      allowPublicSubnet: true,
      bundling: {
        // target: "node14.8",
        // forceDockerBundling: true,
        externalModules: ["aws-sdk", "@matano/vrl-transform-bindings"],
      },
      environment: {
        RAW_EVENTS_BUCKET_NAME: props.rawEventsBucket.bucket.bucketName,
        KAFKAJS_NO_PARTITIONER_WARNING: "1",
        BOOTSTRAP_ADDRESS: kafkaCluster.bootstrapAddress,
      },
      timeout: cdk.Duration.seconds(30),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:*", "kafka:*", "kafka-cluster:*", "dynamodb:*", "s3:*", "athena:*", "glue:*"],
          resources: ["*"],
        }),
      ],
    });
    forwarderLambda.addEventSource(
      new SqsEventSource(props.rawEventsBucket.queue, {
        batchSize: 100,
        maxBatchingWindow: cdk.Duration.seconds(1),
      })
    );
  }
}
