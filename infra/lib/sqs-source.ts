import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";

interface MatanoSQSSourceProps {
  logSourceName: String;
}

export class MatanoSQSSource extends Construct {
  ingestionQueue: sqs.Queue;

  constructor(scope: Construct, id: string, props: MatanoSQSSourceProps) {
    super(scope, id);

    const logSourceName = props.logSourceName;

    const ingestionDLQ = new sqs.Queue(this, `${logSourceName}IngestDLQ`);
    const ingestionQueue = new sqs.Queue(this, `${logSourceName}IngestQueue`, {
      deadLetterQueue: { queue: ingestionDLQ, maxReceiveCount: 3 },
    });

    this.ingestionQueue = ingestionQueue;
  }
}
