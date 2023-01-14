import * as fs from "fs-extra";
import * as path from "path";
import * as YAML from "yaml";
import { Construct, Node } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sns from "aws-cdk-lib/aws-sns";
import * as events from "aws-cdk-lib/aws-events";
import { SqsQueue as SqsQueueTarget } from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import { dataDirPath, fail, getLocalAsset, makeLambdaSnapstart } from "./utils";
import { S3BucketWithNotifications } from "./s3-bucket-notifs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { MatanoStack } from "./MatanoStack";
import { MatanoIcebergTable } from "./iceberg";
import { resolveSchema } from "./schema";
import { MatanoLogSource } from "./log-source";

interface EnrichmentProps {
  lakeStorageBucket: s3.IBucket;
  enrichmentIngestionBucket: s3.IBucket;
  realtimeTopic: sns.Topic;
  lakeWriterLambda: lambda.Function;
  matanoAthenaResultsBucket: s3.IBucket;
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
  realtimeTopic: sns.Topic;
  lakeWriterLambda: lambda.Function;
  dataFilePath?: any;
}

const MANAGED_ENRICHMENT_DIR = path.join(dataDirPath, "managed/enrichment");
const MANAGED_ENRICHMENT_PREFIX_MAP: Record<string, string> = {
  "abusech_urlhaus": "abusech_urlhaus",
  "abusech_malwarebazaar": "abusech_malwarebazaar",
  "abusech_threatfox": "abusech_threatfox",
  "otx": "otx",
}

export class EnrichmentTable extends Construct {
  logSource: MatanoLogSource;
  constructor(scope: Construct, id: string, props: EnrichmentTableProps) {
    super(scope, id);

    const tableName = props.enrichConfig.name;
    const enrichTableName = `enrich_${tableName}`;

    const lsConfig = convertEnrichToLogSourceConfig(props.enrichConfig);
    this.logSource = new MatanoLogSource(this, `LogSource`, {
      config: lsConfig,
      realtimeTopic: props.realtimeTopic,
      lakeWriterLambda: props.lakeWriterLambda,
      noDefaultEcsFields: props.enrichConfig?.schema?.ecs_field_names == null,
      noDefaultTs: true,
    });
    const schema = this.logSource.matanoTables[0].schema;
    const tempTable = new MatanoIcebergTable(this, `MergeTempTable`, {
      tableName: `${enrichTableName}_temp`,
      schema,
      glueDatabaseName: "matano_system",
    });

    if (props.enrichConfig.enrichment_type === "static") {
      const dataFileDir = path.resolve(props.dataFilePath!!, "..");
      // Scrappy way to do static table w/o a custom resource. TODO: maybe change to CR
      const deploy = new s3deploy.BucketDeployment(this, `s3deploy`, {
        sources: [s3deploy.Source.asset(dataFileDir)],
        destinationBucket: props.enrichmentIngestionBucket,
        destinationKeyPrefix: enrichTableName,
        memoryLimit: 256,
        exclude: ["*"],
        include: ["data.json"],
      });
      for (const matanoTable of this.logSource.matanoTables) {
        deploy.node.addDependency(matanoTable.icebergTable);
      }
    }

    const scheduleRule = new events.Rule(this, "EventsRule", {
      description: `[Matano] Schedules the enrichment table log syncer for table: ${tableName}`,
      schedule: events.Schedule.rate(cdk.Duration.minutes(3)),
    });

    // run for both dynamic and static to do syncing (simplify)
    scheduleRule.addTarget(
      new SqsQueueTarget(props.enrichmentSyncerQueue, {
        message: events.RuleTargetInput.fromObject({
          time: events.EventField.time,
          table_name: tableName,
        }),
      })
    );
  }
}

function loadManagedEnrichmentConfig(enrichConfig: any) {
  const managedType = enrichConfig.managed.type;
  if (!managedType) {
    fail("Managed enrichment type cannot be empty");
  }
  const managedConfigPath = path.join(MANAGED_ENRICHMENT_DIR, managedType);
  if (!fs.existsSync(managedConfigPath)) {
    fail(
      `The managed enrichment type: ${managedType} does not exist. Available managed enrichment types: ${JSON.stringify(
        Object.keys(MANAGED_ENRICHMENT_PREFIX_MAP)
      )}`
    );
  }

  const prefix = MANAGED_ENRICHMENT_PREFIX_MAP[managedType];
  if (!enrichConfig.name.startsWith(prefix)) {
    fail(
      `Since you are using the managed enrichment type: ${managedType}, your name must be prefixed with ${prefix}.`
    );
  }
  const managedConfig = YAML.parse(fs.readFileSync(path.join(MANAGED_ENRICHMENT_DIR, `${managedType}/enrichment.yml`), "utf-8"));
  return {
    name: enrichConfig.name,
    ...managedConfig,
  }
}

type EnrichConfig = { enrichConfig: any; dataFilePath?: string };
export class Enrichment extends Construct {
  enrichmentSyncerFunc: lambda.Function;
  enrichmentConfigs: Record<string, EnrichConfig>;
  enrichmentLogSources: Record<string, MatanoLogSource> = {};
  enrichmentTablesBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: EnrichmentProps) {
    super(scope, id);

    this.enrichmentConfigs = this.loadEnrichmentTables();

    this.enrichmentTablesBucket = new s3.Bucket(this, "EnrichmentTables", {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    this.enrichmentSyncerFunc = new lambda.Function(this, "Syncer", {
      description: "[Matano] Syncs enrichment table data.",
      runtime: lambda.Runtime.JAVA_11,
      handler: "com.matano.iceberg.EnrichmentSyncerHandler::handleRequest",
      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      environment: {
        ENRICHMENT_TABLES_BUCKET: this.enrichmentTablesBucket.bucketName,
      },
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["glue:*"],
          resources: ["*"],
        }),
        new iam.PolicyStatement({
          actions: [
            "athena:StartQueryExecution",
            "athena:GetQueryExecution",
            "athena:GetQueryResults",
            "s3:PutObject",
            "s3:GetObject",
          ],
          resources: [
            `arn:${cdk.Stack.of(this).partition}:athena:*:${cdk.Stack.of(this).account}:workgroup/matano_system`
          ],
        }),
      ],
      code: getLocalAsset("iceberg_metadata"),
    });
    makeLambdaSnapstart(this.enrichmentSyncerFunc);

    props.lakeStorageBucket.grantReadWrite(this.enrichmentSyncerFunc);
    this.enrichmentTablesBucket.grantReadWrite(this.enrichmentSyncerFunc);
    props.matanoAthenaResultsBucket.grantReadWrite(this.enrichmentSyncerFunc);

    const enrichmentSyncerDLQ = new sqs.Queue(this, "SyncerDLQ");
    const enrichmentSyncerQueue = new sqs.Queue(this, "SyncerQueue", {
      visibilityTimeout: cdk.Duration.seconds(320),
      deadLetterQueue: { queue: enrichmentSyncerDLQ, maxReceiveCount: 3 },
    });

    const syncerEventSource = new SqsEventSource(enrichmentSyncerQueue, {
      batchSize: 10000,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });
    this.enrichmentSyncerFunc.currentVersion.addEventSource(syncerEventSource);

    for (const [tableName, { enrichConfig, dataFilePath }] of Object.entries(this.enrichmentConfigs)) {
      const enrichTable = new EnrichmentTable(this, `EnrichTable-${tableName}`, {
        enrichConfig,
        dataFilePath,
        enrichmentIngestionBucket: props.enrichmentIngestionBucket,
        enrichmentSyncerQueue,
        realtimeTopic: props.realtimeTopic,
        lakeWriterLambda: props.lakeWriterLambda,
      });
      this.enrichmentLogSources[tableName] = enrichTable.logSource;
    }
  }

  loadEnrichmentTables(): Record<string, EnrichConfig> {
    const stack = cdk.Stack.of(this) as MatanoStack;
    const enrichmentDir = path.resolve(stack.matanoUserDirectory, "enrichment");
    const entries = fs.readdirSync(enrichmentDir).map((tableSubDir) => {
      const tableDir = path.resolve(enrichmentDir, tableSubDir);
      let enrichConfig = YAML.parse(fs.readFileSync(path.resolve(tableDir, "enrichment.yml"), "utf-8"));

      if (enrichConfig?.managed?.type) {
        // Allows user addition of top level keys e.g. `lookup_keys`.
        enrichConfig = { ...loadManagedEnrichmentConfig(enrichConfig), ...enrichConfig };
      }

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
    for (const func of [this.enrichmentSyncerFunc]) {
      func.addLayers(...layers);
    }
  }
}

export function convertEnrichToLogSourceConfig(enrichConfig: any) {
  const ret = {
    name: `enrich_${enrichConfig.name}`,
    transform: enrichConfig.transform,
    schema: {
      ecs_field_names: enrichConfig.schema.ecs_field_names,
      fields: enrichConfig.schema.fields,
    },
  };

  return ret;
}
