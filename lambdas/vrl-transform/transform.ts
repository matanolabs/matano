import { Admin, Kafka, ITopicConfig, CompressionTypes, TopicMessages } from "kafkajs";
import { awsIamAuthenticator } from "@matano/msk-authenticator";
import { vrl, Outcome } from "@matano/vrl-transform-bindings/ts/index.js";
import { MSKHandler } from "aws-lambda";
import * as AWS from "aws-sdk";

import * as fs from "fs";
import path = require("path");



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
  const logSourceToEvents: Record<string, any[]> = Object.fromEntries(Object.values(mskEvent.records).flatMap(_ => _).map(r => {
    if (!r.topic.startsWith("raw.")) {
      throw new Error("Invalid topic name.")
    }
    return [
      r.topic.slice(4),
      JSON.parse(Buffer.from(r.value, 'base64').toString())
    ]
  }));

  let topicToMessages: Record<string, TopicMessages> = {};
  for (const logSourceName in logSourceToEvents) {
    const transformed = logSourceToEvents[logSourceName].map(e => vrl(
      logSourcesMetatdata[logSourceName].transform?.vrl,
      e
    ));
    topicToMessages[logSourceName] = {
      topic: logSourceName,
      messages: transformed.map(m => ({ value: JSON.stringify(m) }))
    };
  }
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
