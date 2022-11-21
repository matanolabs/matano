import * as path from "path";
import * as crypto from "crypto";
import * as fs from "fs-extra";
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
import { IcebergMetadata, MatanoIcebergTable, MatanoSchemas } from "../lib/iceberg";
import {
  fromEntries,
  getDirectories,
  getLocalAssetPath,
  makeTempDir,
  MATANO_USED_RUNTIMES,
  md5Hash,
  readConfig,
} from "../lib/utils";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";
import { MatanoLogSource, LogSourceConfig } from "../lib/log-source";
import { MatanoDetections } from "../lib/detections";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { execSync } from "child_process";
import { SecurityGroup, SubnetType } from "aws-cdk-lib/aws-ec2";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { DataBatcher } from "../lib/data-batcher";
import { RustFunctionLayer } from "../lib/rust-function-layer";
import { LayerVersion } from "aws-cdk-lib/aws-lambda";
import { LakeWriter } from "../lib/lake-writer";
import { Transformer } from "../lib/transformer";

import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { MatanoAlerting } from "../lib/alerting";
import { MatanoS3Sources } from "../lib/s3-sources";
import { IcebergMaintenance } from "../lib/iceberg-maintenance";
import { MatanoSQSSources } from "../lib/sqs-sources";
import { Enrichment } from "../lib/enrichment";

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
    const logSourceConfigPaths = getDirectories(logSourcesDirectory).map((d) => path.join(logSourcesDirectory, d));

    const matanoAlerting = new MatanoAlerting(this, "Alerting", {});

    const detections = new MatanoDetections(this, "Detections", {
      alertingSnsTopic: matanoAlerting.alertingTopic,
      realtimeTopic: props.realtimeBucketTopic,
      matanoSourcesBucketName: props.matanoSourcesBucket.bucket.bucketName,
    });
    this.addConfigFile("detections_config.json", JSON.stringify(detections.detectionConfigs));

    const lakeWriter = new LakeWriter(this, "LakeWriter", {
      outputBucketName: props.lakeStorageBucket.bucket.bucketName,
      outputObjectPrefix: "lake",
    });

    const logSources: MatanoLogSource[] = [];
    for (const logSourceConfigPath of logSourceConfigPaths) {
      const logSource = new MatanoLogSource(
        this,
        `MatanoLogs${path.relative(logSourcesDirectory, logSourceConfigPath)}`,
        {
          configPath: logSourceConfigPath,
          realtimeTopic: props.realtimeBucketTopic,
          lakeWriterLambda: lakeWriter.lakeWriterLambda,
        }
      );
      (logSource.node.id as any) = `MatanoLogs${logSource.logSourceLevelConfig
        .name!.split("_")
        .map((substr) => substr.charAt(0).toUpperCase() + substr.slice(1))}`; // TODO(shaeq): fix this
      logSources.push(logSource);
    }

    // Not a log source but just use it to represent
    const matanoAlertsSource = new MatanoLogSource(this, "Alerts", {
      config: {
        name: "matano_alerts",
        managed: {
          type: "matano_alerts",
          properties: {},
        },
      },
      realtimeTopic: props.realtimeBucketTopic,
      lakeWriterLambda: lakeWriter.alertsLakeWriterLambda,
    });
    logSources.push(matanoAlertsSource);

    const resolvedLogSourceConfigs: Record<string, any> = fromEntries(
      logSources.map((ls) => [
        ls.logSourceConfig.name,
        { base: ls.logSourceLevelConfig, tables: ls.tablesConfig },
      ]) as any
    );

    const resolvedTableNames = logSources.flatMap((ls) => {
      return Object.values(ls.tablesConfig).map((t) => t.resolved_name);
    });

    for (const logSource in resolvedLogSourceConfigs) {
      this.addConfigFile(
        `log_sources/${logSource}/log_source.yml`,
        YAML.stringify(resolvedLogSourceConfigs[logSource].base, {
          aliasDuplicateObjects: false,
          blockQuote: "literal",
        })
      );
      for (const table in resolvedLogSourceConfigs[logSource].tables) {
        this.addConfigFile(
          `log_sources/${logSource}/tables/${table}.yml`,
          YAML.stringify(resolvedLogSourceConfigs[logSource].tables[table], {
            aliasDuplicateObjects: false,
            blockQuote: "literal",
          })
        );
      }
    }

    new MatanoS3Sources(this, "CustomIngestionLogSources", {
      logSources: logSources.filter((ls) => ls.name !== "matano_alerts"),
      sourcesIngestionTopic: props.matanoSourcesBucket.topic,
    });

    const sqsSources = new MatanoSQSSources(this, "SQSIngestionLogSources", {
      logSources: logSources.filter(
        (ls) => ls.name !== "matano_alerts" && ls.logSourceConfig?.ingest?.sqs_source?.enabled === true,
      ),
      resolvedLogSourceConfigs: resolvedLogSourceConfigs
    });

    const allResolvedSchemasHashStr = logSources
      .flatMap((ls) => Object.values(ls.tablesSchemas))
      .reduce((prev, cur) => prev + JSON.stringify(cur), "");
    const schemasHash = md5Hash(allResolvedSchemasHashStr);

    const schemasCR = new MatanoSchemas(this, "SchemasCustomResource", {
      schemaOutputPath: schemasHash,
      tables: resolvedTableNames,
    });

    for (const logSource of logSources) {
      schemasCR.node.addDependency(logSource);
    }

    const schemasLayer = new lambda.LayerVersion(this, "SchemasLayer", {
      compatibleRuntimes: MATANO_USED_RUNTIMES,
      code: lambda.Code.fromBucket(this.cdkAssetsBucket, schemasHash + ".zip"),
    });

    schemasLayer.node.addDependency(schemasCR);

    const rawDataBatcher = new DataBatcher(this, "DataBatcher", {
      s3Bucket: props.matanoSourcesBucket,
    });

    const transformer = new Transformer(this, "Transformer", {
      realtimeBucketName: props.realtimeBucket.bucketName,
      realtimeTopic: props.realtimeBucketTopic,
      matanoSourcesBucketName: props.matanoSourcesBucket.bucket.bucketName,
      logSourcesConfigurationPath: path.join(this.configTempDir, "config"), // TODO: weird fix later (@shaeq)
      schemasLayer: schemasLayer,
      sqsMetadata: sqsSources.sqsMetadata
    });
    transformer.node.addDependency(sqsSources);

    transformer.transformerLambda.addEventSource(
      new SqsEventSource(rawDataBatcher.outputQueue, {
        batchSize: 1,
      })
    );
    
    for (const sqsSource of sqsSources.ingestionQueues) {
      transformer.transformerLambda.addEventSource(
        new SqsEventSource(sqsSource, {
          enabled: false,
          batchSize: 10000,
          maxBatchingWindow: cdk.Duration.seconds(1),
        })
      );
    }

    const icebergMetadata = new IcebergMetadata(this, "IcebergMetadata", {
      lakeStorageBucket: props.lakeStorageBucket,
    });

    lakeWriter.alertsLakeWriterLambda.addEnvironment(
      "ALERT_HELPER_FUNCTION_NAME",
      icebergMetadata.alertsHelperFunction.functionName
    );
    icebergMetadata.alertsHelperFunction.grantInvoke(lakeWriter.alertsLakeWriterLambda);

    const userEnrichmentDir = path.join(this.matanoUserDirectory, "enrichment");
    let enrichment: Enrichment | undefined = undefined;
    const usesEnrichment = fs.existsSync(userEnrichmentDir);
    if (usesEnrichment) {
      enrichment = new Enrichment(this, "Enrichment", {
        lakeStorageBucket: props.lakeStorageBucket.bucket,
      });

      this.addConfigDir(userEnrichmentDir, "enrichment", {
        filter: (s, _) => {
          return fs.lstatSync(s).isDirectory() || s.endsWith(".yml");
        },
      });
    }

    const configLayer = new lambda.LayerVersion(this, "ConfigurationLayer", {
      code: lambda.Code.fromAsset(this.configTempDir),
      description: "A layer for static Matano configurations.",
    });

    lakeWriter.lakeWriterLambda.addLayers(schemasLayer);
    lakeWriter.alertsLakeWriterLambda.addLayers(schemasLayer);

    detections.detectionFunction.addLayers(configLayer);
    rawDataBatcher.batcherFunction.addLayers(configLayer);

    enrichment?.bindLayers(configLayer);

    const allIcebergTableNames = [...resolvedTableNames];
    if (usesEnrichment) {
      const enrichTableNames = Object.keys(enrichment!!.enrichmentConfigs).map((n) => `enrich_${n}`);
      allIcebergTableNames.push(...enrichTableNames);
    }

    new IcebergMaintenance(this, "MatanoIcebergMaintenance", {
      tableNames: allIcebergTableNames,
    });

    this.humanCfnOutput("AlertingSnsTopicArn", {
      value: matanoAlerting.alertingTopic.topicArn,
      description:
        "The ARN of the SNS topic used for Matano alerts. See https://www.matano.dev/docs/detections/alerting",
    });
  }

  private createConfigTempDir() {
    const configTempDir = makeTempDir("mtnconfig");
    fs.mkdirSync(path.join(configTempDir, "config"));

    if (process.env.DEBUG) {
      console.log(`Created temporary directory for configuration files: ${path.join(configTempDir, "config")}`);
    }
    return configTempDir;
  }

  private addConfigFile(fileSubpath: string, content: string) {
    const filePath = path.join(this.configTempDir, "config", fileSubpath);
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, content);
  }

  private addConfigDir(srcDir: string, destSubDir: string, opts?: fs.CopyOptionsSync) {
    const destDir = path.join(this.configTempDir, "config", destSubDir);
    fs.mkdirSync(destDir, { recursive: true });
    fs.copySync(srcDir, destDir, opts);
  }
}
