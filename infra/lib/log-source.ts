import * as path from "path";
import * as fs from "fs";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as s3 from "aws-cdk-lib/aws-s3";
import { MatanoIcebergTable } from "../lib/iceberg";
import { resolveSchema, serializeToFields, mergeSchema, fieldsToSchema } from "./schema";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { SqsEventSource, SqsEventSourceProps } from "aws-cdk-lib/aws-lambda-event-sources";
import { dataDirPath, fail, matanoResourceToCdkName, mergeDeep, readConfig, walkdirSync } from "./utils";

export const MATANO_DATABASE_NAME = "matano";

type ConfigurationInfo = { type: "shared" | "managed" | "user"; relativePath: string };

function mergeManagedConfig(userConfig: any, managedConfig: any, managedLogSourceType: string) {
  const merged = mergeDeep(userConfig, managedConfig);

  if (userConfig?.ingest?.csv_headers != null) {
    if (!merged.ingest) merged.ingest = {};
    merged.ingest.csv_headers = userConfig.ingest.csv_headers;
  }

  if (managedConfig.transform != null && userConfig.transform != null) {
    const headerLength = managedConfig.transform.split("\n", 1).shift().length;
    managedConfig.transform += `${"#".repeat(headerLength)}\n`;
    merged.transform = managedConfig.transform + userConfig.transform;
  }

  if (managedConfig.schema?.fields && userConfig.schema?.fields) {
    const managedSchema = fieldsToSchema(managedConfig.schema?.fields);
    let mergedSchema = JSON.parse(JSON.stringify(fieldsToSchema(userConfig.schema?.fields)));
    mergedSchema = mergeSchema(mergedSchema, managedSchema);
    merged.schema.fields = serializeToFields(mergedSchema);
  }

  return merged;
}

function diff(tgt: any, src: any) {
  if (Array.isArray(tgt)) {
    return tgt;
  }
  var rst: any = {};
  for (var k in tgt) {
    // visit all fields
    if (typeof src[k] === "object") {
      // if field contains object (or array because arrays are objects too)
      rst[k] = diff(tgt[k], src[k]); // diff the contents
    } else if (src[k] !== tgt[k]) {
      // if field is not an object and has changed
      rst[k] = tgt[k]; // use new value
    }
    // otherwise just skip it
  }
  return rst;
}

export interface LogSourceConfig {
  name: string;
  schema?: {
    ecs_field_names?: string[];
    fields?: any[];
  };
  ingest?: {
    select_table_from_payload_metadata?: string;
    select_table_from_payload?: string;
    expand_records_from_payload?: string;
    compression: string;
    csv_headers: string[];
    s3_source?: {
      bucket_name?: string;
      key_prefix?: string;
      key_pattern?: string;
    };
    sqs_source?: {
      enabled?: boolean;
    };
  };
  transform?: string;
  managed?: {
    type?: string;
    properties: Record<string, any>;
  };
  [key: string]: any;
}

interface MatanoLogSourceProps {
  config?: LogSourceConfig;
  configPath?: string;
  realtimeTopic: sns.Topic;
  lakeWriterLambda: lambda.Function;
  partitions?: any[];
  noDefaultEcsFields?: boolean;
  noDefaultTs?: boolean;
  eventSourceProps?: SqsEventSourceProps;
  lakeStorageBucket: s3.IBucket;
  athenaResultsBucket?: s3.IBucket;
}

const MANAGED_LOG_SOURCE_PREFIX_MAP: Record<string, string> = {
  onepassword: "onepassword",
  aws_cloudtrail: "aws",
  aws_route53_resolver_logs: "aws",
  aws_s3access: "aws",
  aws_s3inventory: "aws",
  aws_elb: "aws",
  aws_inspector: "aws",
  aws_config_history: "aws",
  aws_vpcflow: "aws",
  aws_waf: "aws",
  crowdstrike: "crowdstrike",
  crowdstrike_falcon: "crowdstrike",
  duo: "duo",
  msft: "msft",
  o365: "o365",
  google_workspace: "google_workspace",
  github_audit: "github",
  okta: "okta",
  snyk: "snyk",
  suricata: "suricata",
  zeek: "zeek",
  matano_alerts: "matano_alerts", // doesn't really make sense but OK
};

function getPrefixForManagedLogSourceType(logSourceType: string) {
  return MANAGED_LOG_SOURCE_PREFIX_MAP[logSourceType];
}

const MANAGED_LOG_SOURCES_DIR = path.join(dataDirPath, "managed/log_sources");

export interface MatanoTableProps {
  tableName: string;
  schema: any;
  realtimeTopic: sns.Topic;
  lakeWriterLambda: lambda.Function;
  eventSourceProps?: SqsEventSourceProps;
  partitions?: any[];
  lakeStorageBucket: s3.IBucket;
  athenaResultsBucket?: s3.IBucket;
}
export class MatanoTable extends Construct {
  icebergTable: MatanoIcebergTable;
  schema: any;
  constructor(scope: Construct, id: string, props: MatanoTableProps) {
    super(scope, id);
    this.schema = props.schema;

    this.icebergTable = new MatanoIcebergTable(this, `Default`, {
      tableName: props.tableName,
      schema: props.schema,
      partitions: props.partitions,
      lakeStorageBucket: props.lakeStorageBucket,
      athenaResultsBucket: props.athenaResultsBucket,
    });

    const lakeWriterDlq = new sqs.Queue(this, `LakeWriterDLQ`, {
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const lakeWriterQueue = new sqs.Queue(this, `LakeWriterQueue`, {
      deadLetterQueue: {
        queue: lakeWriterDlq,
        maxReceiveCount: 3,
      },
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      visibilityTimeout: cdk.Duration.seconds(Math.max(props.lakeWriterLambda.timeout!.toSeconds(), 30)),
    });

    props.realtimeTopic.addSubscription(
      new SqsSubscription(lakeWriterQueue, {
        rawMessageDelivery: true,
        filterPolicy: {
          resolved_table_name: sns.SubscriptionFilter.stringFilter({ allowlist: [props.tableName] }),
        },
      })
    );

    props.lakeWriterLambda.addEventSource(
      new SqsEventSource(lakeWriterQueue, {
        batchSize: 25,
        maxBatchingWindow: cdk.Duration.seconds(20),
        reportBatchItemFailures: true,
        ...props.eventSourceProps,
      })
    );
  }
}

export class MatanoLogSource extends Construct {
  name: string;
  schema: Record<string, any>;
  logSourceConfig: LogSourceConfig;
  logSourceLevelConfig: Partial<LogSourceConfig>;
  tablesSchemas: Record<string, any> = {};
  tablesConfig: Record<string, Record<string, any>> = {};
  matanoTables: MatanoTable[] = [];
  managedLogSourceType?: string;

  constructor(scope: Construct, id: string, props: MatanoLogSourceProps) {
    super(scope, id);

    const logSourceConfig = props.config
      ? props.config
      : (readConfig(props.configPath!, "log_source.yml") as LogSourceConfig);
    this.logSourceConfig = logSourceConfig;

    if (props.config?.name === "matano_alerts") {
      const managedConf = readConfig(MANAGED_LOG_SOURCES_DIR, "matano_alerts/log_source.yml");
      this.logSourceConfig = mergeDeep(logSourceConfig, managedConf);
      this.tablesConfig["matano_alerts"] = {};
    }

    if (props.config?.name.startsWith("enrich_")) {
      this.tablesConfig[props.config.name] = {};
    }

    const { name: logSourceName, ingest: ingestConfig } = logSourceConfig;
    this.name = logSourceName;

    if (logSourceConfig?.managed && logSourceConfig.name !== "matano_alerts") {
      const configPath = props.configPath!;
      const managedLogSourceType = logSourceConfig?.managed?.type?.toLowerCase();
      this.managedLogSourceType = managedLogSourceType;
      if (!managedLogSourceType) {
        fail("Invalid Managed Log source type: cannot be empty");
      }
      const managedConfigPath = path.join(MANAGED_LOG_SOURCES_DIR, managedLogSourceType);
      const prefix = getPrefixForManagedLogSourceType(managedLogSourceType);
      if (!fs.existsSync(managedConfigPath) || !prefix) {
        fail(
          `The managed log source type: ${managedLogSourceType} does not exist. Available managed log sources: ${JSON.stringify(
            Object.keys(MANAGED_LOG_SOURCE_PREFIX_MAP)
          )}`
        );
      }

      if (!logSourceConfig.name.startsWith(prefix)) {
        fail(
          `Since you are using the managed log source type: ${managedLogSourceType}, your log source name must be prefixed with ${prefix}. Please rename your log source as: ${prefix}_${logSourceConfig.name}`
        );
      }

      let managedConfigFilePaths = walkdirSync(managedConfigPath).map((p) => path.relative(managedConfigPath, p));
      let userConfigFilePaths = walkdirSync(configPath).map((p) => path.relative(configPath, p));

      const sharedConfigFilePaths = managedConfigFilePaths.filter((p) => userConfigFilePaths.includes(p));
      managedConfigFilePaths = managedConfigFilePaths.filter((p) => !sharedConfigFilePaths.includes(p));
      userConfigFilePaths = userConfigFilePaths.filter((p) => !sharedConfigFilePaths.includes(p));

      const configInfos = [
        ...(sharedConfigFilePaths.map((p) => ({
          type: "shared",
          relativePath: p,
        })) as ConfigurationInfo[]),
        ...(managedConfigFilePaths.map((p) => ({
          type: "managed",
          relativePath: p,
        })) as ConfigurationInfo[]),
        ...(userConfigFilePaths.map((p) => ({
          type: "user",
          relativePath: p,
        })) as ConfigurationInfo[]),
      ];

      for (const configInfo of configInfos) {
        // merge the shared logsource and table level configs
        const isLogSourceConfig = configInfo.relativePath == "log_source.yml";
        const isTableConfig = configInfo.relativePath.startsWith("tables/");

        if (isTableConfig && configInfo.type == "managed") {
          // skip managed table configs that haven't been imported by the user
          // TODO: we should implement non path-based table imports here as well
          continue;
        }

        if (isLogSourceConfig || isTableConfig) {
          const managedConfig =
            configInfo.type == "managed" || configInfo.type == "shared"
              ? readConfig(managedConfigPath, configInfo.relativePath)
              : {};
          const userConfig =
            configInfo.type == "user" || configInfo.type == "shared"
              ? readConfig(configPath, configInfo.relativePath)
              : {};

          if (managedConfig.transform) {
            const header = `#### Matano managed transform for ${managedLogSourceType} ${
              isTableConfig ? `- ${managedConfig.name} table` : ""
            } - DO NOT EDIT ####`;

            if (managedLogSourceType == "o365") {
              const tenantName = logSourceConfig?.managed?.properties?.tenant_name;
              const injectTenantsConfig = `tenants = ${JSON.stringify(
                tenantName ? { [logSourceConfig?.managed?.properties?.tenant_id]: tenantName } : {}
              )}`;
              managedConfig.transform = `${injectTenantsConfig}\n${managedConfig.transform}\n`;
            }

            managedConfig.transform = `${header}\n${managedConfig.transform}\n`;
          }

          if (
            isTableConfig &&
            userConfig.name != null &&
            managedConfig.name != null &&
            userConfig.name != managedConfig.name
          ) {
            fail(
              `The table name cannot be changed for a table defined by a managed log source. Please rename the table back to: ${managedConfig.name} (log source: ${logSourceName})`
            );
          }

          const config: any =
            configInfo.type == "shared"
              ? mergeManagedConfig(userConfig, managedConfig, managedLogSourceType)
              : configInfo.type == "user"
              ? userConfig
              : managedConfig;

          if (isTableConfig) {
            // table config
            if (config.name == null) {
              config.name = "default";
            }
            if (config.name in this.tablesConfig) {
              fail(`Table name ${config.name} is already defined`);
            }
            if (!!managedConfig.name) {
              this.tablesConfig[config.name] = config;
            } else {
              throw new Error(`Invalid table: ${config.name} for log source: ${logSourceName}`);
            }
          } else {
            // log source config
            this.logSourceConfig = config;
          }
        } else {
        }
      }
    }

    if (this.logSourceConfig.name == null) {
      fail(`Log source name cannot be empty: ${props.configPath}`);
    }

    let logSourceLevelConfig: any = {
      // config that only applies to log sources, and not tables
      name: this.logSourceConfig.name,
      ingest: {
        compression: this.logSourceConfig.ingest?.compression,
        s3_source: this.logSourceConfig?.ingest?.s3_source,
        sqs_source: this.logSourceConfig?.ingest?.sqs_source,
        select_table_from_payload_metadata: this.logSourceConfig.ingest?.select_table_from_payload_metadata,
        select_table_from_payload: this.logSourceConfig.ingest?.select_table_from_payload,
        csv_headers: this.logSourceConfig.ingest?.csv_headers,
      },
      managed: this.logSourceConfig.managed,
    };
    this.logSourceLevelConfig = logSourceLevelConfig;
    this.schema = resolveSchema(
      this.logSourceConfig.schema?.ecs_field_names,
      this.logSourceConfig.schema?.fields,
      props.noDefaultEcsFields,
      props.noDefaultTs
    );

    const logSourceConfigToMerge = diff(this.logSourceConfig, logSourceLevelConfig);
    if (Object.keys(this.tablesConfig).length == 0) {
      this.tablesConfig["default"] = {};
    }

    if (logSourceConfigToMerge.transform) {
      logSourceConfigToMerge.transform = `########################## log source transform ###########################\n\n${logSourceConfigToMerge.transform}\n`;
    }

    for (const tableName in this.tablesConfig) {
      const logSourceConfigBase = JSON.parse(JSON.stringify(logSourceConfigToMerge)); // get rid of mutating merge fns so this cloning nonsense isnt needed
      const merged = mergeDeep(this.tablesConfig[tableName], logSourceConfigBase);

      if (!merged.name || merged.name == "default") {
        merged.name = "default";
        merged.resolved_name = logSourceConfig.name;
      } else {
        merged.resolved_name = `${logSourceConfig.name}_${merged.name}`;
      }

      if (this.tablesConfig[tableName].transform) {
        this.tablesConfig[
          tableName
        ].transform = `########################## table transform ###########################\n\n${this.tablesConfig[tableName].transform}\n`;
      }

      // Allow composing certain fields. For example, transformations defined for log_source and/or table.
      if (logSourceConfigBase.transform && this.tablesConfig[tableName].transform) {
        merged.transform = logSourceConfigBase.transform + "\n\n" + this.tablesConfig[tableName].transform;
      }

      if (logSourceConfigBase.schema?.ecs_field_names && this.tablesConfig[tableName].schema?.ecs_field_names) {
        merged.schema.ecs_field_names = [
          ...new Set([
            ...(logSourceConfigBase.schema.ecs_field_names ?? []),
            ...(this.tablesConfig[tableName].schema.ecs_field_names ?? []),
          ]),
        ];
      }

      if (logSourceConfigBase.schema?.fields && this.tablesConfig[tableName].schema?.fields) {
        const logSourceSchema = fieldsToSchema(logSourceConfigBase.schema?.fields ?? []);
        let tableSchema = fieldsToSchema(this.tablesConfig[tableName].schema?.fields ?? []);
        tableSchema = mergeSchema(tableSchema, logSourceSchema);
        merged.schema.fields = serializeToFields(tableSchema);
      }

      let tableSchema = resolveSchema(
        merged.schema?.ecs_field_names,
        merged.schema?.fields,
        props.noDefaultEcsFields,
        props.noDefaultTs
      );
      // partial sort to move ts to top
      tableSchema.fields = sortBy(tableSchema.fields, (e) => e.name, ["ts"]);

      this.tablesSchemas[tableName] = tableSchema;
      merged.schema.fields = tableSchema.fields; // store the resolved fields & schema in the config

      this.tablesConfig[tableName] = merged;
      const tableConfig = this.tablesConfig[tableName];
      const resolvedTableName = tableConfig.resolved_name;

      const formattedName = matanoResourceToCdkName(merged.name);

      this.matanoTables.push(
        new MatanoTable(this, `${formattedName}Table`, {
          tableName: resolvedTableName,
          schema: tableSchema,
          realtimeTopic: props.realtimeTopic,
          lakeWriterLambda: props.lakeWriterLambda,
          partitions: props.partitions,
          lakeStorageBucket: props.lakeStorageBucket,
          athenaResultsBucket: props.athenaResultsBucket,
        })
      );
    }
  }

  get isDataLogSource() {
    return this.name !== "matano_alerts" && !this.name.startsWith("enrich_");
  }
}

function sortBy<T, U>(arr: T[], elemFn: (e: T) => U, orderList: U[]) {
  let ret: T[] = [];
  for (const orderKey of orderList) {
    const elemIdx = arr.findIndex((e) => elemFn(e) === orderKey);
    if (elemIdx !== -1) {
      ret.push(arr[elemIdx]);
      arr.splice(elemIdx, 1);
    }
  }
  for (const remainingElem of arr) {
    ret.push(remainingElem);
  }
  return ret;
}
