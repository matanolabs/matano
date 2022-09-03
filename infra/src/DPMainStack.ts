import * as path from "path";
import * as fs from "fs";
import * as YAML from "yaml";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as os from "os";

import * as sns from "aws-cdk-lib/aws-sns";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { IcebergMetadata } from "../lib/iceberg";
import { dualAsset, getDirectories, getLocalAssetPath, makeTempDir, MATANO_USED_RUNTIMES, readConfig } from "../lib/utils";
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
import { RustFunctionLayer } from "../lib/rust-function-layer";
import { LayerVersion } from "aws-cdk-lib/aws-lambda";
import { LakeIngestion } from "../lib/lake-ingestion";
import { Transformer } from "../lib/transformer";

import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";

interface DPMainStackProps extends MatanoStackProps {
  matanoSourcesBucket: S3BucketWithNotifications;
  lakeStorageBucket: S3BucketWithNotifications;
  realtimeBucket: Bucket;
  realtimeBucketTopic: sns.Topic;
}

export class DPMainStack extends MatanoStack {
  constructor(scope: Construct, id: string, props: DPMainStackProps) {
    super(scope, id, props);

    const logSourcesDirectory = path.join(this.matanoUserDirectory, "log_sources");
    const logSourceConfigs = getDirectories(logSourcesDirectory)
      .map((d) => path.join(logSourcesDirectory, d))
      .map((p) => readConfig(p, "log_source.yml") as LogSourceConfig);
      
    const logSourcesConfigurationPath = path.resolve(path.join(makeTempDir(), "log_sources_configuration.yml"));
    fs.writeFileSync(logSourcesConfigurationPath, YAML.stringify(logSourceConfigs));

    const rawDataBatcher = new DataBatcher(this, "DataBatcher", {
      s3Bucket: props.matanoSourcesBucket,
    });

    const detections = new MatanoDetections(this, "MatanoDetections", {});

    const lakeIngestion = new LakeIngestion(this, "LakeIngestion", {
      outputBucketName: props.lakeStorageBucket.bucket.bucketName,
      outputObjectPrefix: "lake",
    });

    const transformer = new Transformer(this, "Transformer", {
      realtimeBucketName: props.realtimeBucket.bucketName,
      logSourcesConfigurationPath,
    });

    transformer.transformerLambda.addEventSource(new SqsEventSource(rawDataBatcher.outputQueue, {
      batchSize: 1,
    }));

    const logSources = [];
    const tempSchemasDir = makeTempDir("matano-schemas");

    for (const logSourceConfig of logSourceConfigs) {
      const logSource = new MatanoLogSource(this, `MatanoLogSource${logSourceConfig.name}`, {
        config: logSourceConfig,
        defaultSourceBucket: props.matanoSourcesBucket.bucket,
        realtimeTopic: props.realtimeBucketTopic,
        lakeIngestionLambda: lakeIngestion.lakeIngestionLambda,
      });

      const schemaDir = path.join(tempSchemasDir, logSourceConfig.name);
      fs.mkdirSync(schemaDir);
      fs.writeFileSync(path.join(schemaDir, "iceberg_schema.json"), JSON.stringify(logSource.resolvedSchema));
      logSources.push(logSource);
    }

    // matano-java-scripts
    const schemasLayer = new lambda.LayerVersion(this, "MatanoSchemasLayer", {
      compatibleRuntimes: MATANO_USED_RUNTIMES,
      code: dualAsset("matano-java-scripts", 
        () => lambda.Code.fromAsset(path.resolve(path.join("../lib/java/matano")), {
          assetHashType: cdk.AssetHashType.OUTPUT,
          bundling: {
            volumes: [
              { hostPath: tempSchemasDir, containerPath: "/schemas" },
              { hostPath: path.resolve("../local-assets"), containerPath: "/local-assets" },
            ],
            image: lambda.Runtime.JAVA_11.bundlingImage,
            command: ["./gradlew", ":scripts:run", "--args", "gen-schemas /schemas", ":scripts:buildJar"],
          },
        }),
        () => lambda.Code.fromAsset(getLocalAssetPath("matano-java-scripts"), {
          assetHashType: cdk.AssetHashType.OUTPUT,
          bundling: {
            volumes: [
              { hostPath: tempSchemasDir, containerPath: "/schemas" },
            ],
            image: lambda.Runtime.JAVA_11.bundlingImage,
            command: ["bash", "-c", "java -jar /asset-input/matano-scripts.jar gen-schemas /schemas", ],
          },
        })
      ),
    });

    new IcebergMetadata(this, "IcebergMetadata", {
      lakeStorageBucket: props.lakeStorageBucket,
    });

    // const logSourcesConfigurationLayer = new lambda.LayerVersion(this, "LogSourcesConfigurationLayer", {
    //   code: lambda.Code.fromAsset("../lambdas", {
    //     bundling: {
    //       volumes: [
    //         {
    //           hostPath: logSourcesConfigurationPath,
    //           containerPath: "/asset-input/log_sources_configuration.yml",
    //         },
    //       ],
    //       image: transformer.rustFunctionLayer.image,
    //       command: [
    //         "bash",
    //         "-c",
    //         "cp /asset-input/log_sources_configuration.yml /asset-output/log_sources_configuration.yml",
    //       ],
    //     },
    //   }),
    //   description: "A layer for Matano Log Source Configurations.",
    // });
  }
}
