import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { MatanoLogSource } from "./log-source";

interface MatanoSQSSourcesProps {
  logSources: MatanoLogSource[];
}

export class MatanoSQSSources extends Construct {
  outputQueues: sqs.Queue[] = [];
  constructor(scope: Construct, id: string, props: MatanoSQSSourcesProps) {
    super(scope, id);

    for (const logSource of props.logSources) {
      const name = logSource.name.split("_")
        .map((substr) => substr.charAt(0).toUpperCase() + substr.slice(1));

      const outputDLQ = new sqs.Queue(this, `${name}OutputDLQ`);
      const outputQueue = new sqs.Queue(this, `${name}OutputQueue`, {
        deadLetterQueue: { queue: outputDLQ, maxReceiveCount: 3 },
      });

      this.outputQueues.push(outputQueue);
    }
  }
}
