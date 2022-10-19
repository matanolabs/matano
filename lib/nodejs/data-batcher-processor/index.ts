import { Context, S3Event, S3EventRecord, SQSEvent } from "aws-lambda";
import * as AWS from "aws-sdk";

const sqs = new AWS.SQS({ region: process.env.AWS_REGION });

export async function handler(event: SQSEvent, context: Context) {
  const eventRecords: S3EventRecord[] = event.Records.map((r) => JSON.parse(r.body).Records?.[0]).filter((x) => !!x);
  if (eventRecords.length === 0) {
    return;
  }
  let inputBytesSize = 0;
  const newSqsRecords = [];
  for (const record of eventRecords) {
    const s3Object = record.s3.object;
    inputBytesSize += s3Object.size;
    const data = {
      bucket: record.s3.bucket.name,
      key: s3Object.key,
      size: s3Object.size,
      sequencer: record.s3.object.sequencer,
    };
    newSqsRecords.push(data);
  }

  const batchedSqsRecords: AWS.SQS.SendMessageBatchRequestEntry[] = chunkedBy(newSqsRecords, {
    maxSize: 32 * 1000 * 1000, // 32MB in bytes
    sizeFn: (item) => item.size,
  }).map((batch) => {
    return {
      Id: batch.reduce((p, c) => p + c.size, 0).toString(),
      MessageBody: JSON.stringify(batch),
    };
  });

  const outputLength = batchedSqsRecords.length;
  const averageOutputSize = inputBytesSize / outputLength;
  console.log(
    `Coalesced ${inputBytesSize} bytes of data from ${eventRecords.length} input records to ${outputLength} output records of average size ${averageOutputSize} bytes.`
  );

  const sqsBatchEntryLists = chunkedBy(batchedSqsRecords, { maxLength: 10 });

  const sqsPromises = [];
  for (const batchEntries of sqsBatchEntryLists) {
    const params: AWS.SQS.SendMessageBatchRequest = {
      QueueUrl: process.env.OUTPUT_QUEUE_URL!!,
      Entries: batchEntries,
    };
    const sqsPromise = sqs.sendMessageBatch(params).promise();
    sqsPromises.push(sqsPromise);
  }
  // TODO: handle failure!
  await Promise.all(sqsPromises);
}

interface ChunkingProps<T> {
  maxLength?: number;
  maxSize?: number;
  sizeFn?: (item: T) => number;
}

function chunkedBy<T>(arr: T[], { maxLength, maxSize, sizeFn }: ChunkingProps<T>): T[][] {
  if ((maxSize == null) != (sizeFn == null)) {
    throw new Error("maxSize and sizeFn must be specified together.");
  }

  let result: T[][] = [];
  let sublist: T[] = [];
  let sublistSize = 0;
  for (const item of arr) {
    let startNewArray = false;
    if (maxLength && sublist.length >= maxLength) {
      startNewArray = true;
    }
    if (maxSize && sizeFn && sublistSize + sizeFn(item) > maxSize) {
      startNewArray = true;
    }
    if (startNewArray) {
      result.push(sublist);
      sublist = [];
      sublistSize = 0;
    }
    sublist.push(item);
    if (sizeFn) sublistSize += sizeFn(item);
  }
  if (sublist.length > 0) result.push(sublist);
  return result;
}
