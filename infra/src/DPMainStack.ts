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
  getDirectories,
  getLocalAssetPath,
  makeTempDir,
  matanoResourceToCdkName,
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
import { Table } from "aws-cdk-lib/aws-dynamodb";
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
import { IntegrationsStore } from "../lib/integrations-store";
import { ExternalLogPuller, PULLER_LOG_SOURCE_TYPES } from "../lib/log-puller";

interface DPMainStackProps extends MatanoStackProps {
  matanoSourcesBucket: S3BucketWithNotifications;
  lakeStorageBucket: S3BucketWithNotifications;
  realtimeBucket: Bucket;
  realtimeBucketTopic: sns.Topic;
  matanoAthenaResultsBucket: s3.Bucket;
  integrationsStore: IntegrationsStore;
  alertTrackerTable: Table;
}

const MATANO_LOG_PARTITION_SPEC = [
  { column: "ts", transform: "hour" },
  { column: "partition_hour", transform: "identity" },
];

export class DPMainStack extends MatanoStack {
  configTempDir: string;
  constructor(scope: Construct, id: string, props: DPMainStackProps) {
    super(scope, id, props);
    lambda.Function.classifyVersionProperty("SnapStart", true);

    this.configTempDir = this.createConfigTempDir();

    const logSourcesDirectory = path.join(this.matanoUserDirectory, "log_sources");
    const logSourceConfigPaths = getDirectories(logSourcesDirectory).map((d) => path.join(logSourcesDirectory, d));

    const matanoAlerting = new MatanoAlerting(this, "Alerting", {
      integrationsStore: props.integrationsStore,
      alertTrackerTable: props.alertTrackerTable,
    });

    const detections = new MatanoDetections(this, "Detections", {
      realtimeTopic: props.realtimeBucketTopic,
      matanoSourcesBucketName: props.matanoSourcesBucket.bucket.bucketName,
    });
    this.addConfigFile("detections_config.json", JSON.stringify(detections.detectionConfigs));

    const lakeWriter = new LakeWriter(this, "LakeWriter", {
      alertingSnsTopic: matanoAlerting.alertingTopic,
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
          partitions: MATANO_LOG_PARTITION_SPEC,
          realtimeTopic: props.realtimeBucketTopic,
          lakeWriterLambda: lakeWriter.lakeWriterLambda,
        }
      );
      const formattedName = matanoResourceToCdkName(logSource.logSourceLevelConfig.name!);
      (logSource.node.id as any) = `MatanoLogs${formattedName}`; // TODO(shaeq): fix this
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
      partitions: MATANO_LOG_PARTITION_SPEC,
      realtimeTopic: props.realtimeBucketTopic,
      lakeWriterLambda: lakeWriter.alertsLakeWriterLambda,
    });
    logSources.push(matanoAlertsSource);

    new MatanoS3Sources(this, "CustomIngestionLogSources", {
      logSources: logSources.filter((ls) => ls.isDataLogSource),
      sourcesIngestionTopic: props.matanoSourcesBucket.topic,
    });

    const rawDataBatcher = new DataBatcher(this, "DataBatcher", {
      s3Bucket: props.matanoSourcesBucket,
    });

    const icebergMetadata = new IcebergMetadata(this, "IcebergMetadata", {
      lakeStorageBucket: props.lakeStorageBucket,
    });

    lakeWriter.alertsLakeWriterLambda.addEnvironment(
      "ALERT_HELPER_FUNCTION_NAME",
      `${icebergMetadata.alertsHelperFunction.functionName}:current`
    );
    icebergMetadata.alertsHelperFunction.grantInvoke(lakeWriter.alertsLakeWriterLambda);

    const userEnrichmentDir = path.join(this.matanoUserDirectory, "enrichment");
    let enrichment: Enrichment | undefined = undefined;
    const usesEnrichment = fs.existsSync(userEnrichmentDir);
    if (usesEnrichment) {
      enrichment = new Enrichment(this, "Enrichment", {
        lakeStorageBucket: props.lakeStorageBucket.bucket,
        enrichmentIngestionBucket: props.matanoSourcesBucket.bucket,
        realtimeTopic: props.realtimeBucketTopic,
        lakeWriterLambda: lakeWriter.lakeWriterLambda,
        matanoAthenaResultsBucket: props.matanoAthenaResultsBucket,
      });

      this.addConfigDir(userEnrichmentDir, "enrichment", {
        filter: (s, _) => {
          return fs.lstatSync(s).isDirectory() || s.endsWith(".yml");
        },
      });
      const enrichmentLogSources = Object.values(enrichment.enrichmentLogSources);
      logSources.push(...enrichmentLogSources);
      detections.detectionFunction.addEnvironment(
        "ENRICHMENT_TABLES_BUCKET",
        enrichment.enrichmentTablesBucket.bucketName
      );
    }

    const resolvedLogSourceConfigs: Record<string, any> = Object.fromEntries(
      logSources.map((ls) => [
        ls.logSourceConfig.name,
        { base: ls.logSourceLevelConfig, tables: ls.tablesConfig },
      ]) as any
    );

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

    const sqsSources = new MatanoSQSSources(this, "SQSIngest", {
      logSources: logSources.filter(
        (ls) => ls.isDataLogSource && ls.logSourceConfig?.ingest?.sqs_source?.enabled === true
      ),
      resolvedLogSourceConfigs: resolvedLogSourceConfigs,
    });

    const transformer = new Transformer(this, "Transformer", {
      realtimeBucketName: props.realtimeBucket.bucketName,
      realtimeTopic: props.realtimeBucketTopic,
      matanoSourcesBucketName: props.matanoSourcesBucket.bucket.bucketName,
      logSourcesConfigurationPath: path.join(this.configTempDir, "config"), // TODO: weird fix later (@shaeq)
      sqsMetadata: sqsSources.sqsMetadata,
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

    const resolvedTableNames = logSources.flatMap((ls) => {
      return Object.values(ls.tablesConfig).map((t) => t.resolved_name);
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

    const configLayer = new lambda.LayerVersion(this, "ConfigurationLayer", {
      code: lambda.Code.fromAsset(this.configTempDir),
      description: "A layer for static Matano configurations.",
    });

    lakeWriter.lakeWriterLambda.addLayers(schemasLayer);
    lakeWriter.alertsLakeWriterLambda.addLayers(schemasLayer);
    transformer.transformerLambda.addLayers(schemasLayer);

    detections.detectionFunction.addLayers(configLayer);
    rawDataBatcher.batcherFunction.addLayers(configLayer);
    icebergMetadata.metadataWriterFunction.addLayers(configLayer);
    enrichment?.bindLayers(configLayer);

    const allIcebergTableNames = [...resolvedTableNames];
    if (usesEnrichment) {
      const enrichTableNames = Object.keys(enrichment!!.enrichmentConfigs).map((n) => `enrich_${n}`);
      allIcebergTableNames.push(...enrichTableNames);
    }

    new IcebergMaintenance(this, "MatanoIcebergMaintenance", {
      tableNames: allIcebergTableNames,
    });

    const externalLogPuller = new ExternalLogPuller(this, "ExternalLogPuller", {
      logSources: logSources
        .filter(
          (ls) =>
            ls.isDataLogSource &&
            ls.managedLogSourceType != null &&
            PULLER_LOG_SOURCE_TYPES.includes(ls.managedLogSourceType)
        )
        .map((ls) => ls.name),
      ingestionBucket: props.matanoSourcesBucket.bucket,
    });
    externalLogPuller.function.addLayers(configLayer);

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
