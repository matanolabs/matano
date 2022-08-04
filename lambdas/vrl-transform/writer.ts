
import { MSKHandler } from "aws-lambda";
import * as AWS from "aws-sdk";
import { chunkedBy } from "./utils";

const firehose = new AWS.Firehose();


export const handler: MSKHandler = async (mskEvent) => {
  console.log("start");
  const decodedMskRecords = Object.values(mskEvent.records).flatMap(_ => _).map(r => {
    return [
      r.topic,
      Buffer.from(r.value, 'base64').toString()
    ]
  });

  const logSourceToRecordsBatch = decodedMskRecords.reduce((acc, [logSource, eventString]) => {
    const recordsBatch = acc[logSource] ?? [];
    if (recordsBatch.length && Buffer.byteLength(recordsBatch[recordsBatch.length - 1]) + Buffer.byteLength(eventString) <= 1_000_000) {
      recordsBatch[recordsBatch.length - 1] += '\n' + eventString
    } else {
      recordsBatch.push(eventString)
    }
    return { ...acc, [logSource]: recordsBatch }
  }, {} as Record<string, string[]>);

  const promises = Object.entries(logSourceToRecordsBatch).flatMap(([logSourceName, recordsBatch]) => {
    const recordsBatches = chunkedBy(recordsBatch, {
      maxLength: 500,
      maxSize: 4_000_000,
      sizeFn: Buffer.byteLength
    });

    return recordsBatches.map(recordsBatch => {
      const firehoseRecords = recordsBatch.map(r => ({ Data: Buffer.from(r) }));
      return firehose.putRecordBatch({
        DeliveryStreamName: `${logSourceName}-lake-firehose`,
        Records: firehoseRecords,
      }).promise();
    })
  });

  const results = await Promise.allSettled(promises);

  const failures = results.flatMap((result) =>
    result.status !== "fulfilled"
      ? [String(result.reason.message)]
      : result.value.RequestResponses.filter(r => r.ErrorCode != null).map(r => r.ErrorMessage)
  );
  if (failures.length) console.warn(`Failures: ${JSON.stringify(failures)}`);

  const recordIds = results.flatMap((result) =>
    result.status === "fulfilled"
      ? result.value.RequestResponses.map(r => r.RecordId).filter(id => id != null)
      : []
  );
  console.log(`Processed ${decodedMskRecords.length} user records. Put (${recordIds.length} Kinesis Records) to Firehose sucessfully, in ${promises.length} requests. Encountered ${failures.length} errors.`);
  console.log("end");
};
