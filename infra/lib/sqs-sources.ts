import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { MatanoLogSource } from "./log-source";

interface MatanoSQSSourcesProps {
  logSources: MatanoLogSource[];
}

export class MatanoSQSSources extends Construct {
  ingestionQueues: sqs.Queue[] = [];
  constructor(scope: Construct, id: string, props: MatanoSQSSourcesProps) {
    super(scope, id);

    for (const logSource of props.logSources) {
      const name = logSource.name.split("_")
        .map((substr) => substr.charAt(0).toUpperCase() + substr.slice(1));

      const ingestionDLQ = new sqs.Queue(this, `${name}IngestionDLQ`);
      const ingestionQueue = new sqs.Queue(this, `${name}IngestionQueue`, {
        deadLetterQueue: { queue: ingestionDLQ, maxReceiveCount: 3 },
      });

      this.ingestionQueues.push(ingestionQueue);
    }
  }
}
