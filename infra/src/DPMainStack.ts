import * as path from "path";
import * as crypto from "crypto";
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
import { IcebergMetadata, MatanoSchemas } from "../lib/iceberg";
import { getDirectories, getLocalAssetPath, makeTempDir, MATANO_USED_RUNTIMES, md5Hash, readConfig } from "../lib/utils";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { MatanoLogSource, LogSourceConfig } from "../lib/log-source";
import { MatanoDetections } from "../lib/detections";
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
import { MatanoAlerting } from "../lib/alerting";
import { resolveSchema } from "../lib/schema";
import { MatanoS3Sources } from "../lib/s3-sources";

interface DPMainStackProps extends MatanoStackProps {
  matanoSourcesBucket: S3BucketWithNotifications;
  lakeStorageBucket: S3BucketWithNotifications;
  realtimeBucket: Bucket;
  realtimeBucketTopic: sns.Topic;
}

export class DPMainStack extends MatanoStack {
  configTempDir: string;
  constructor(scope: Construct, id: string, props: DPMainStackProps) {
    super(scope, id, props);
    this.configTempDir = this.createConfigTempDir();

    const logSourcesDirectory = path.join(this.matanoUserDirectory, "log_sources");
    const logSourceConfigs = getDirectories(logSourcesDirectory)
      .map((d) => path.join(logSourcesDirectory, d))
      .map((p) => readConfig(p, "log_source.yml") as LogSourceConfig);

    const rawDataBatcher = new DataBatcher(this, "DataBatcher", {
      s3Bucket: props.matanoSourcesBucket,
    });

    const matanoAlerting = new MatanoAlerting(this, "MatanoAlerting", {});

    const detections = new MatanoDetections(this, "MatanoDetections", {
      alertingSnsTopic: matanoAlerting.alertingTopic,
      realtimeTopic: props.realtimeBucketTopic,
      matanoSourcesBucketName: props.matanoSourcesBucket.bucket.bucketName,
    });
    this.addConfigFile("detections_config.json", JSON.stringify(detections.detectionConfigs));

    const lakeIngestion = new LakeIngestion(this, "LakeIngestion", {
      outputBucketName: props.lakeStorageBucket.bucket.bucketName,
      outputObjectPrefix: "lake",
    });

    const logSources: MatanoLogSource[] = [];
    for (const logSourceConfig of logSourceConfigs) {
      const logSource = new MatanoLogSource(this, `MatanoLogSource${logSourceConfig.name}`, {
        config: logSourceConfig,
        defaultSourceBucket: props.matanoSourcesBucket.bucket,
        realtimeTopic: props.realtimeBucketTopic,
        lakeIngestionLambda: lakeIngestion.lakeIngestionLambda,
      });
      logSources.push(logSource);
    }

    const resolvedLogSourceConfigs = logSources.map(ls => ls.sourceConfig);
    this.addConfigFile("log_sources_configuration.yml", YAML.stringify(resolvedLogSourceConfigs, { blockQuote: "literal" }));

    new MatanoS3Sources(this, "CustomIngestionLogSources", {
      logSources,
      sourcesIngestionTopic: props.matanoSourcesBucket.topic,
    });

    const allResolvedSchemasHashStr = logSources
      .map(ls => ls.schema)
      .reduce((prev, cur) => prev + JSON.stringify(cur), "");
    const schemasHash = md5Hash(allResolvedSchemasHashStr);

    const schemasCR = new MatanoSchemas(this, "MatanoSchemasCustomResource", {
      schemaOutputPath: schemasHash,
      logSources: logSources.map(ls => ls.name),
    });

    for (const logSource of logSources) {
      schemasCR.node.addDependency(logSource);
    }

    const schemasLayer = new lambda.LayerVersion(this, "MatanoSchemasLayer", {
      compatibleRuntimes: MATANO_USED_RUNTIMES,
      code: lambda.Code.fromBucket(this.cdkAssetsBucket, schemasHash + ".zip"),
    });

    schemasLayer.node.addDependency(schemasCR);

    const transformer = new Transformer(this, "Transformer", {
      realtimeBucketName: props.realtimeBucket.bucketName,
      realtimeTopic: props.realtimeBucketTopic,
      matanoSourcesBucketName: props.matanoSourcesBucket.bucket.bucketName,
      logSourcesConfigurationPath: path.join(this.configTempDir, "config"), // TODO: weird fix later (@shaeq)
      schemasLayer: schemasLayer,
    });

    transformer.transformerLambda.addEventSource(
      new SqsEventSource(rawDataBatcher.outputQueue, {
        batchSize: 1,
      })
    );

    new IcebergMetadata(this, "IcebergMetadata", {
      lakeStorageBucket: props.lakeStorageBucket,
    });

    const configLayer = new lambda.LayerVersion(this, "ConfigurationLayer", {
      code: lambda.Code.fromAsset(this.configTempDir),
      description: "A layer for static Matano configurations.",
    });

    detections.detectionFunction.addLayers(configLayer);

    this.humanCfnOutput("MatanoAlertingSnsTopicArn", {
      value: matanoAlerting.alertingTopic.topicArn,
      description: "The ARN of the SNS topic used for Matano alerts. See https://www.matano.dev/docs/detections/alerting",
    });

  }

  private createConfigTempDir() {
    const configTempDir = makeTempDir("mtnconfig");
    fs.mkdirSync(path.join(configTempDir, "config"));
    return configTempDir;
  }

  private addConfigFile( filename: string, content: string) {
    fs.writeFileSync(path.join(this.configTempDir, "config", filename), content);
  }
}
