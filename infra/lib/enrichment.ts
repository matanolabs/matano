import * as fs from "fs-extra";
import * as path from "path";
import * as YAML from "yaml";
import { Construct, Node } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as events from "aws-cdk-lib/aws-events";
import { SqsQueue as SqsQueueTarget } from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import { fail, getLocalAsset } from "./utils";
import { S3BucketWithNotifications } from "./s3-bucket-notifs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { MatanoStack } from "./MatanoStack";
import { MatanoIcebergTable } from "./iceberg";
import { resolveSchema } from "./schema";

interface EnrichmentProps {
  lakeStorageBucket: s3.IBucket;
}

interface EnrichmentCRProps {
  staticTablesBucket: string;
  staticTablesKey: string;
  ingestionBucket: string;
}

interface EnrichmentTableProps {
  enrichConfig: any;
  enrichmentIngestionBucket: s3.IBucket;
  enrichmentSyncerQueue: sqs.IQueue;
  dynamicScheduleRule: events.Rule;
  dataFilePath?: any;
}

export class EnrichmentTable extends Construct {
  constructor(scope: Construct, id: string, props: EnrichmentTableProps) {
    super(scope, id);

    const tableName = props.enrichConfig.name;

    const resolvedSchema = resolveSchema([], props.enrichConfig.schema.fields, true);

    const icebergTable = new MatanoIcebergTable(this, `IcebergTable`, {
      tableName: `enrich_${tableName}`,
      schema: resolvedSchema,
    });

    if (props.enrichConfig.enrichment_type === "static") {
      const dataFileDir = path.resolve(props.dataFilePath!!, "..");
      // Scrappy way to do static table w/o a custom resource. TODO: maybe change to CR
      const deploy = new s3deploy.BucketDeployment(this, `s3deploy-${tableName}`, {
        sources: [s3deploy.Source.asset(dataFileDir)],
        destinationBucket: props.enrichmentIngestionBucket,
        destinationKeyPrefix: `${tableName}`,
        memoryLimit: 256,
        exclude: ["*"],
        include: ["data.json"],
      });
      deploy.node.addDependency(icebergTable);
    } else {
      props.dynamicScheduleRule.addTarget(
        new SqsQueueTarget(props.enrichmentSyncerQueue, {
          message: events.RuleTargetInput.fromObject({
            time: events.EventField.time,
            table_name: tableName,
          }),
        })
      );
    }
  }
}

export class Enrichment extends Construct {
  enrichmentIngestorFunc: lambda.Function;
  enrichmentSyncerFunc: lambda.Function;

  constructor(scope: Construct, id: string, props: EnrichmentProps) {
    super(scope, id);

    const enrichmentConfigs = this.loadEnrichmentTables();
    const enrichmentIngestionBucket = new S3BucketWithNotifications(this, "Ingestion", {
      queueProps: {
        visibilityTimeout: cdk.Duration.seconds(310),
      },
    });

    const enrichmentTablesBucket = new s3.Bucket(this, "EnrichmentTables", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    this.enrichmentIngestorFunc = new lambda.Function(this, "Ingestor", {
      description: "[Matano] Ingests enrichment table data",
      runtime: lambda.Runtime.JAVA_11,
      handler: "com.matano.iceberg.EnrichmentIngestorHandler::handleRequest",
      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      environment: {
        ENRICHMENT_TABLES_BUCKET: enrichmentTablesBucket.bucketName,
      },
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["glue:*"],
          resources: ["*"],
        }),
      ],
      code: getLocalAsset("iceberg_table_cfn"),
    });

    this.enrichmentSyncerFunc = new lambda.Function(this, "Syncer", {
      description: "[Matano] Syncs enrichment table data",
      runtime: lambda.Runtime.JAVA_11,
      handler: "com.matano.iceberg.EnrichmentSyncerHandler::handleRequest",
      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      environment: {
        ENRICHMENT_TABLES_BUCKET: enrichmentTablesBucket.bucketName,
      },
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["glue:*"],
          resources: ["*"],
        }),
      ],
      code: getLocalAsset("iceberg_table_cfn"),
    });

    props.lakeStorageBucket.grantReadWrite(this.enrichmentIngestorFunc);
    props.lakeStorageBucket.grantReadWrite(this.enrichmentSyncerFunc);
    enrichmentIngestionBucket.bucket.grantRead(this.enrichmentIngestorFunc);
    enrichmentTablesBucket.grantWrite(this.enrichmentSyncerFunc);

    this.enrichmentIngestorFunc.addEventSource(
      new SqsEventSource(enrichmentIngestionBucket.queue, {
        batchSize: 1000,
        maxBatchingWindow: cdk.Duration.seconds(10),
      })
    );

    const enrichmentSyncerDLQ = new sqs.Queue(this, "SyncerDLQ");
    const enrichmentSyncerQueue = new sqs.Queue(this, "SyncerQueue", {
      visibilityTimeout: cdk.Duration.seconds(320),
      deadLetterQueue: { queue: enrichmentSyncerDLQ, maxReceiveCount: 3 },
    });

    const syncerEventSource = new SqsEventSource(enrichmentSyncerQueue, {
      batchSize: 10000,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });
    this.enrichmentSyncerFunc.addEventSource(syncerEventSource);

    const scheduleRule = new events.Rule(this, "EventsRule", {
      description: "[Matano] Schedules the enrichment table log syncer.",
      schedule: events.Schedule.rate(cdk.Duration.minutes(3)),
    });

    for (const [tableName, { enrichConfig, dataFilePath }] of Object.entries(enrichmentConfigs)) {
      new EnrichmentTable(this, `EnrichTable-${tableName}`, {
        enrichConfig,
        dataFilePath,
        enrichmentIngestionBucket: enrichmentIngestionBucket.bucket,
        enrichmentSyncerQueue,
        dynamicScheduleRule: scheduleRule,
      });
    }
  }

  loadEnrichmentTables(): { enrichConfig: any; dataFilePath?: string }[] {
    const stack = cdk.Stack.of(this) as MatanoStack;
    const enrichmentDir = path.resolve(stack.matanoUserDirectory, "enrichment");
    const entries = fs.readdirSync(enrichmentDir).map((tableSubDir) => {
      const tableDir = path.resolve(enrichmentDir, tableSubDir);
      const enrichConfig = YAML.parse(fs.readFileSync(path.resolve(tableDir, "enrichment_table.yml"), "utf-8"));

      if (enrichConfig.enrichment_type === "static" && enrichConfig.write_mode != undefined) {
        fail(`Static enrichment tables always have write mode 'overwrite', in ${enrichConfig.name}`);
      }

      const ret: any = { enrichConfig };
      if (enrichConfig.enrichment_type === "static") {
        const dataFile = fs
          .readdirSync(tableDir)
          .filter((p) => fs.statSync(path.resolve(tableDir, p)).isFile())
          .filter((p) => p.startsWith("data."))?.[0];
        if (!dataFile) {
          fail(`Missing data file for static enrichment table: ${enrichConfig.name}`);
        }
        ret["dataFilePath"] = path.resolve(tableDir, dataFile);
      }
      return [enrichConfig.name, ret];
    });
    return Object.fromEntries(entries);
  }

  bindLayers(...layers: lambda.ILayerVersion[]) {
    for (const func of [this.enrichmentIngestorFunc, this.enrichmentSyncerFunc]) {
      func.addLayers(...layers);
    }
  }
}
