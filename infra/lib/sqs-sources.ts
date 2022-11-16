import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { MatanoLogSource } from "./log-source";

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
    let sqsMetadata: Map<string, string[]> = new Map<string, string[]>();

    for (const ls of logSources) {
      for (const table in resolvedLogSourceConfigs[ls.name].tables) {

        const lsName = ls.name.split("_")
          .map((substr) => substr.charAt(0).toUpperCase() + substr.slice(1));

        const tableName = table.charAt(0).toUpperCase() + table.slice(1);

        const ingestionDLQ = new sqs.Queue(this, `${lsName}${tableName}IngestDLQ`);
        const ingestionQueue = new sqs.Queue(this, `${lsName}${tableName}IngestQueue`, {
          deadLetterQueue: { queue: ingestionDLQ, maxReceiveCount: 3 },
        });

        this.ingestionQueues.push(ingestionQueue);
        sqsMetadata.set(ingestionQueue.queueName, [ls.name, table]);
      }
    }

    const obj = Object.fromEntries(sqsMetadata);
    this.sqsMetadata = JSON.stringify(obj);
    
  }
}

