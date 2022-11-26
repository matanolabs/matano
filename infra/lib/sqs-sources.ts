import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { MatanoLogSource } from "./log-source";
import { matanoResourceToCdkName } from "./utils";

interface MatanoSQSSourcesProps {
  logSources: MatanoLogSource[];
  resolvedLogSourceConfigs: Record<string, any>;
}

export class MatanoSQSSources extends Construct {
  ingestionQueues: sqs.Queue[] = [];
  sqsMetadata: string;

  constructor(scope: Construct, id: string, props: MatanoSQSSourcesProps) {
    super(scope, id);

    const logSources = props.logSources;
    const resolvedLogSourceConfigs = props.resolvedLogSourceConfigs;
    let sqsMetadata: Map<string, string> = new Map<string, string>();

    for (const ls of logSources) {
      const tables = resolvedLogSourceConfigs[ls.name].tables;
      // The resolved table name is:
      // 1) <log_source_name>_<table_name>
      // 2) OR if table_name == default: <log_source_name>
      const resolvedTableNames: string[] = Object.values(tables).map((t: any) => t.resolved_name);
      for (const resolvedTableName of resolvedTableNames) {
        const formattedTableName = matanoResourceToCdkName(resolvedTableName);

        const ingestionDLQ = new sqs.Queue(this, `${formattedTableName}DLQ`);
        const ingestionQueue = new sqs.Queue(this, `${formattedTableName}Queue`, {
          deadLetterQueue: { queue: ingestionDLQ, maxReceiveCount: 3 },
        });

        this.ingestionQueues.push(ingestionQueue);
        sqsMetadata.set(ingestionQueue.queueName, resolvedTableName);
      }
    }

    const obj = Object.fromEntries(sqsMetadata);
    this.sqsMetadata = JSON.stringify(obj);
  }
}
