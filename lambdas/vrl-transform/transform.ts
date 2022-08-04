import { Kafka, CompressionTypes, TopicMessages } from "kafkajs";
import { awsIamAuthenticator } from "@matano/msk-authenticator";
import { vrl } from "@matano/vrl-transform-bindings/ts/index.js";
import { MSKHandler } from "aws-lambda";

import * as fs from "fs";
import path = require("path");
import { to_res } from "./utils";


// declare const logSourcesConfiguration: Record<string, any>[];
const logSourcesConfiguration: Record<string, any>[] = JSON.parse(
  fs.readFileSync(path.join("/opt/log_sources_configuration.json"), "utf8")
);
const logSourcesMetatdata: Record<string, Record<string, any>> = logSourcesConfiguration.reduce(
  (acc, logSourceConfig) => {
    acc[logSourceConfig.name] = logSourceConfig
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

export const handler: MSKHandler = async (mskEvent, context) => {
  console.log("start");
  const logSourceToEvents = Object.values(mskEvent.records).flatMap(_ => _).map(r => {
    if (!r.topic.startsWith("raw.")) {
      throw new Error("Invalid topic name.")
    }
    return [
      r.topic.slice(4),
      JSON.parse(Buffer.from(r.value, 'base64').toString())
    ]
  }).reduce((acc, [logSource, event]) => ({ ...acc, [logSource]: [...(acc[logSource] ?? []), event] }), {} as Record<string, any[]>);

  let topicToMessages: Record<string, TopicMessages> = {};
  for (const logSourceName in logSourceToEvents) {
    const transformedResults = logSourceToEvents[logSourceName].map(e => to_res(vrl(
      logSourcesMetatdata[logSourceName].transform?.vrl,
      e
    ), "result"));
    const failures = transformedResults.flatMap((v) => !v.ok ? [v.error!!] : []);
    if (failures.length) console.warn(`Failures: ${JSON.stringify(failures)}`);

    const transformedMessages = transformedResults.flatMap((v) => v.ok ? [{ value: v.value }] : []);
    topicToMessages[logSourceName] = {
      topic: logSourceName,
      messages: transformedMessages,
    };
  }
  console.log(JSON.stringify(topicToMessages));
  const topicMessages = Object.values(topicToMessages).flatMap(m => m);
  console.log("middle");

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
