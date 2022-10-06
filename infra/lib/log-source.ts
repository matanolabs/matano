import * as path from "path";
import * as fs from "fs";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import { MatanoIcebergTable } from "../lib/iceberg";
import { resolveSchema, serializeToFields, merge, fieldsToSchema } from "./schema";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { dataDirPath, fail, mergeDeep, readConfig } from "./utils";

export const MATANO_DATABASE_NAME = "matano";

function walkdirSync(dir: string): string[] {
  return fs.readdirSync(dir).reduce(function (result: string[], file) {
    const filePath = path.join(dir, file);
    const isDir = fs.statSync(filePath).isDirectory();
    return result.concat(isDir ? walkdirSync(filePath) : [filePath]);
  }, []);
}

type ConfigurationInfo = { type: "shared" | "managed" | "user"; relativePath: string };

function mergeManagedConfig(userConfig: any, managedConfig: any, managedLogSourceType: string) {
  const merged = mergeDeep(userConfig, managedConfig);

  if (managedConfig.transform && userConfig.transform) {
    const headerLength = managedConfig.transform.split("\n", 1).shift().length;
    managedConfig.transform += `${"#".repeat(headerLength)}\n`;
    merged.transform = managedConfig.transform + userConfig.transform;
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
    s3_source?: {
      bucket_name?: string;
      key_prefix?: string;
      expand_records_from_object?: string;
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
  configPath: string;
  realtimeTopic: sns.Topic;
  lakeIngestionLambda: lambda.Function;
}

const MANAGED_LOG_SOURCE_PREFIX_MAP: Record<string, string> = {
  aws_cloudtrail: "aws",
  matano_alerts: "matano_alerts", // doesn't really make sense but OK
};

function getPrefixForManagedLogSourceType(logSourceType: string) {
  return MANAGED_LOG_SOURCE_PREFIX_MAP[logSourceType];
}

const MANAGED_LOG_SOURCES_DIR = path.join(dataDirPath, "managed");

export class MatanoLogSource extends Construct {
  name: string;
  schema: Record<string, any>;
  logSourceConfig: LogSourceConfig;
  logSourceLevelConfig: Partial<LogSourceConfig>;
  tablesSchemas: Record<string, any> = {};
  tablesConfig: Record<string, Record<string, any>> = {};

  constructor(scope: Construct, id: string, props: MatanoLogSourceProps) {
    super(scope, id);

    const logSourceConfig = readConfig(props.configPath, "log_source.yml") as LogSourceConfig;
    this.logSourceConfig = logSourceConfig;

    const { name: logSourceName, ingest: ingestConfig } = logSourceConfig;
    this.name = logSourceName;

    if (logSourceConfig?.managed) {
      const managedLogSourceType = logSourceConfig?.managed?.type?.toLowerCase();
      if (!managedLogSourceType) {
        fail("Invalid Managed Log source type: cannot be empty");
      }
      const managedConfigPath = path.join(MANAGED_LOG_SOURCES_DIR, managedLogSourceType);
      if (!fs.existsSync(managedConfigPath)) {
        fail(
          `The managed log source type: ${managedLogSourceType} does not exist. Available managed log sources: ${JSON.stringify(
            Object.keys(MANAGED_LOG_SOURCE_PREFIX_MAP)
          )}`
        );
      }

      const prefix = getPrefixForManagedLogSourceType(managedLogSourceType);
      if (!logSourceConfig.name.startsWith(prefix)) {
        fail(
          `Since you are using the managed log source type: ${managedLogSourceType}, your log source name must be prefixed with ${prefix}. Please rename your log source as: ${prefix}_${logSourceConfig.name}`
        );
      }

      let managedConfigFilePaths = walkdirSync(managedConfigPath).map((p) => path.relative(managedConfigPath, p));
      let userConfigFilePaths = walkdirSync(props.configPath).map((p) => path.relative(props.configPath, p));

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
        if (isLogSourceConfig || isTableConfig) {
          const managedConfig =
            configInfo.type == "managed" || configInfo.type == "shared"
              ? readConfig(managedConfigPath, configInfo.relativePath)
              : {};
          const userConfig =
            configInfo.type == "user" || configInfo.type == "shared"
              ? readConfig(props.configPath, configInfo.relativePath)
              : {};

          if (managedConfig.transform) {
            const header = `#### Matano managed transform for ${managedLogSourceType} ${
              isTableConfig ? `- ${managedConfig.name} table` : ""
            } - DO NOT EDIT ####`;
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
            this.tablesConfig[config.name] = config;
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
        select_table_from_payload_metadata: this.logSourceConfig.ingest?.select_table_from_payload_metadata,
      },
      managed: this.logSourceConfig.managed,
    };
    this.logSourceLevelConfig = logSourceLevelConfig;
    this.schema = resolveSchema(this.logSourceConfig.schema?.ecs_field_names, this.logSourceConfig.schema?.fields);

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
        tableSchema = merge(tableSchema, logSourceSchema);
        merged.schema.fields = serializeToFields(tableSchema);
      }

      const tableSchema = (this.tablesSchemas[tableName] = resolveSchema(
        merged.schema?.ecs_field_names,
        merged.schema?.fields
      ));
      merged.schema.fields = tableSchema.fields; // store the resolved fields & schema in the config

      this.tablesConfig[tableName] = merged;
      const resolvedTableName = this.tablesConfig[tableName].resolved_name;

      const formattedName = merged.name.charAt(0).toUpperCase() + merged.name.slice(1);
      const matanoIcebergTable = new MatanoIcebergTable(this, `${formattedName}Table`, {
        tableName: resolvedTableName,
        schema: tableSchema,
      });

      const ingestionDlq = new sqs.Queue(this, `${formattedName}LakeIngestionDLQ`, {
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      });

      const ingestionQueue = new sqs.Queue(this, `${formattedName}LakeIngestionQueue`, {
        deadLetterQueue: {
          queue: ingestionDlq,
          maxReceiveCount: 3,
        },
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      });

      props.realtimeTopic.addSubscription(
        new SqsSubscription(ingestionQueue, {
          rawMessageDelivery: true,
          filterPolicy: {
            resolved_table_name: sns.SubscriptionFilter.stringFilter({ allowlist: [resolvedTableName] }),
          },
        })
      );

      props.lakeIngestionLambda.addEventSource(
        new SqsEventSource(ingestionQueue, {
          batchSize: 10,
          maxBatchingWindow: cdk.Duration.seconds(20),
        })
      );
    }
  }
}
