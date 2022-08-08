import { Kafka, CompressionTypes, TopicMessages } from "kafkajs";
import { awsIamAuthenticator } from "@matano/msk-authenticator";
import { vrl } from "@matano/vrl-transform-bindings/ts/index.js";
import { S3EventRecord, SQSHandler } from "aws-lambda";
import * as AWS from "aws-sdk";
import { Ok, Err, Result, to_res, chunkedBy, readFile } from "./utils";

import * as fs from "fs";
import * as zlib from "zlib";
import path = require("path");
import console = require("console");

const s3 = new AWS.S3();


// declare const logSourcesConfiguration: Record<string, any>[];
const logSourcesConfiguration: Record<string, any>[] = JSON.parse(
  fs.readFileSync(path.join("/opt/log_sources_configuration.json"), "utf8")
);
const logSourcesMetatdata: Record<string, Record<string, Record<string, any>>> = logSourcesConfiguration.reduce(
  (acc, logSourceConfig) => {
    const bucketName = logSourceConfig.ingest?.s3_source?.bucket_name ?? process.env.RAW_EVENTS_BUCKET_NAME;
    const keyPrefix = logSourceConfig.ingest?.s3_source?.key_prefix ?? logSourceConfig.name;
    acc[bucketName] = {
      [keyPrefix]: logSourceConfig,
      ...(acc[bucketName] ?? {}),
    };
    return acc;
  }, {}
);

const bootstrapServers = process.env.BOOTSTRAP_ADDRESS!!.split(",");
const kafka = new Kafka({
  clientId: `matano-ingestor-handler`,
  brokers: bootstrapServers,
  ssl: true,
  sasl: {
    mechanism: "AWS_MSK_IAM",
    authenticationProvider: awsIamAuthenticator(process.env.AWS_REGION!!, "900"),
  },
});
const producer = kafka.producer();

async function* getLinesByFormat(bucketName: string, objectKey: string, logSourceMetadata: Record<string, any>): AsyncGenerator<Result<string, string>> {
  const csvHeaderSettings = logSourceMetadata.ingest?.s3_source?.csv?.header;
  const expandRecordsFromObjectVrl = logSourceMetadata.ingest?.s3_source?.expand_records_from_object;
  const isCsv = objectKey.endsWith(".csv") || objectKey.endsWith(".csv.gz");
  const isJson = objectKey.endsWith(".json") || objectKey.endsWith(".json.gz")
  let lineIterator: AsyncIterableIterator<string> | undefined;

  if (expandRecordsFromObjectVrl != null) {
    const s3Response = await s3
      .getObject({
        Bucket: bucketName,
        Key: objectKey,
      })
      .promise();
    const objectDataString = /\.gz$/i.test(objectKey) ? zlib.gunzipSync(s3Response.Body as any).toString() : s3Response.Body?.toString();
    if (objectDataString == null) return [];
    const result = vrl(`result = {
      ${expandRecordsFromObjectVrl}
    }
    assert!(result == null || is_array(result), "The expand_records_from_object VRL expression must return the expanded records from the .__raw object string as an array, or return null to skip expansion.")
    if result != null {
      result = map_values(array!(result)) -> |v| {
        obj = if is_object(v) { v } else { {"message": to_string!(v)} }
        encode_json(obj)
      }
    } 
    . = result`, {
      __raw: objectDataString,
      __key: objectKey,
    });
    if (result.error != null) yield Err(result.error);
    else {
      const output = result.success?.result as string[] | null;
      if (output == null) {
        async function* genLines() {
          for (const line of objectDataString!!.split("\n")) {
            yield line;
          }
        }
        lineIterator = genLines();
      } else {
        for (const line of output) yield Ok(line);
      }
    }
  } else {
    const s3Response = s3
      .getObject({
        Bucket: bucketName,
        Key: objectKey,
      })
      .createReadStream();
    lineIterator = readFile(s3Response, /\.gz$/i.test(objectKey))[Symbol.asyncIterator]();
  }
  if (lineIterator == null) return;
  let parsedHeaderFields: string[] | undefined;
  if (isCsv && Boolean(csvHeaderSettings)) {
    let header = lineIterator.next();
    parsedHeaderFields = vrl(`parse_csv!(.message),`, { message: header }).success?.output as string[] | undefined;
  }
  for await (const line of lineIterator) {
    if (isCsv) {
      yield to_res(vrl(`
          item = {}
          line_values = parse_csv!(.message)
          for_each(v -> |_i, v| { 
            key = if .headers && .headers.length > _i { .headers[_i] } else { "_"+_i }
            item[key] = line_values[i] 
          }
          encode_json(item)
        `, { message: line, headers: parsedHeaderFields }));
    } else if (isJson) {
      yield to_res(vrl(`encode_json(parse_json!(.message)),`, { message: line })); // Ok(line)? can i even skip parsing?
    } else {
      yield Ok(JSON.stringify({ message: line }));
    }
  }
}

export const handler: SQSHandler = async (sqsEvent, context) => {
  console.log("start");
  const s3ObjectRecords = sqsEvent.Records.map(
    (sqsRecord) =>
    ({
      ...JSON.parse(sqsRecord.body).Records.shift(),
      receiptHandle: sqsRecord.receiptHandle,
    } as S3EventRecord & { receiptHandle: string })
  );

  const [s3DownloadBatch, ...s3DownloadBatchesToRequeue] = chunkedBy(s3ObjectRecords, {
    // maxLength: 1000, // TODO: is this needed, or controlled by cdk?
    maxSize: 128 * 1024 * 1024,
    sizeFn: (item) => item.s3.object.size,
  });

  const results = await Promise.allSettled(
    s3DownloadBatch.map(async (s3Download) => {
      const [bucketName, objectKey] = [s3Download.s3.bucket.name, s3Download.s3.object.key];
      const logSourceName = objectKey.split("/").shift()!!;
      const metadata = logSourcesMetatdata[bucketName][logSourceName];
      let results: {
        topic: string;
        data: Result<string, string>;
      }[] = [];
      for await (const res of getLinesByFormat(bucketName, objectKey, metadata)) {
        results.push({
          topic: `raw.${logSourceName}`,
          data: res
        });
      }
      return results;
    })
  );
  console.log("middle");
  const failures = results.flatMap((result) =>
    result.status !== "fulfilled"
      ? [String(result.reason.message)]
      : result.value.flatMap((v) => !v.data.ok ? [v.data.error!!] : [])
  );
  if (failures.length) console.warn(`Failures: ${JSON.stringify(failures)}`);

  const topicToMessages = results.reduce((acc, result) => {
    const data = result.status === "fulfilled" ? result.value.flatMap((v) => v.data.ok ? [[v.topic, v.data.value] as [string, string]] : []) : [];
    for (const [topic, message] of data) {
      acc[topic] = {
        topic,
        messages: [...(acc[topic]?.messages ?? []), { value: message }],
      }
    }
    return acc;
  }, {} as Record<string, TopicMessages>);

  const topicMessages = Object.values(topicToMessages).flatMap(m => m);
  console.log(`Messages forwarded: ${topicMessages.length}`);

  if (topicMessages.length) {
    await producer.connect();
    await producer.sendBatch({
      timeout: 5000,
      compression: CompressionTypes.GZIP,
      topicMessages,
    });
  }
  console.log("end");
};
