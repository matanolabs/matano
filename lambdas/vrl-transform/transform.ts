import { Admin, Kafka, ITopicConfig, CompressionTypes } from "kafkajs";
import { mskIamAuthenticator } from "@matano/msk-authenticator";
import { vrl, Outcome } from "@matano/vrl-transform-bindings/ts";
import { S3EventRecord, SQSHandler } from "aws-lambda";
import * as AWS from "aws-sdk";

import * as fs from "fs";
import * as zlib from "zlib";
import * as readline from "readline";

const logSourcesMetatdata: Record<string, Record<string, { vrl: string }>> = {
  "matano-raw-bucket": {
    coredns: {
      vrl: `
      # parse the log event.
      # ts = .timestamp
      log, err = parse_regex(.message,r'\[(?P<level>[^]]+)]\s(?P<server_addr>[^:]+):(?P<server_port>\S+)\s+-\s+(?P<id>\S+)\s+"(?P<type>\S+)\s+(?P<class>\S+)\s+(?P<name>\S+)\s+(?P<proto>\S+)\s+(?P<size>\S+)\s+(?P<do>\S+)\s+(?P<bufsize>[^"]+)"\s+(?P<rcode>\S+)\s+(?P<rflags>\S+)\s+(?P<rsize>\S+)\s+(?P<duration>[\d\.]+).*')
      if err != null {
        # capture the error log. If the error log also fails to get parsed, the log event is dropped.
        log = parse_regex!(.message,r'\[(?P<level>ERROR)]\s+(?P<component>plugin/errors):\s+(?P<code>\S)+\s+(?P<name>\S+)\s+(?P<type>[^:]*):\s+(?P<error_msg>.*)')
      }
      . = log
      # add timestamp
      # .timestamp = ts
      # remove fields we don't care about
      del(.do)
      `,
    },
  },
};

function readFile(stream: NodeJS.ReadableStream, isGzip: boolean) {
  if (isGzip) {
    stream = stream.pipe(zlib.createGunzip());
  }

  return readline.createInterface({
    input: stream,
    crlfDelay: Infinity,
  });
}

const s3 = new AWS.S3();

interface ChunkingProps<T> {
  maxLength?: number;
  maxSize?: number;
  sizeFn?: (item: T) => number;
}

export const chunkedBy = <T>(arr: T[], { maxLength, maxSize, sizeFn }: ChunkingProps<T>): T[][] => {
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
};

export const handler: SQSHandler = async (sqsEvent, context) => {
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
      const s3Response = s3
        .getObject({
          Bucket: s3Download.s3.bucket.name,
          Key: s3Download.s3.object.key,
        })
        .createReadStream();
      const bucketName = s3Download.s3.bucket.name;
      const logSourceName = s3Download.s3.object.key.split("/").shift()!!;
      const metadata = logSourcesMetatdata[bucketName][logSourceName];
      const lineReader = readFile(s3Response, /\.gz$/i.test(s3Download.s3.object.key));
      let results: {
        topic: string;
        data: Outcome;
      }[] = [];

      for await (const line of lineReader) {
        console.log(line);
        const output = vrl(metadata.vrl, {
          message: line,
        });
        const res = {
          topic: logSourceName,
          data: output,
        };
        results.push();
      }
      return results;
    })
  );
  // const failures = results.filter((result) => result.status !== "fulfilled") as PromiseRejectedResult[];
  const outputData = results.flatMap((result) =>
    result.status === "fulfilled" ? result.value.filter((v) => v.data.error == null) : []
  );

  const bootstrapServers = process.env.BOOTSTRAP_ADDRESS!!.split(",");
  const kafka = new Kafka({
    clientId: `matano-ingestor-handler`,
    brokers: bootstrapServers,
    ssl: true,
    sasl: {
      mechanism: "AWS_MSK_IAM",
      authenticationProvider: mskIamAuthenticator(process.env.AWS_REGION!!),
    },
  });
  // const admin = kafka.admin();
  const producer = kafka.producer();
  await producer.connect();
  await producer.sendBatch({
    timeout: 5000,
    compression: CompressionTypes.GZIP,
    topicMessages: outputData.map((d) => ({
      topic: d.topic,
      messages: [{ value: JSON.stringify(d.data.success!!.result) }],
    })),
  });
};
