import * as response from "cfn-response";
import { Admin, Kafka, ITopicConfig } from "kafkajs";
import { mskIamAuthenticator } from "@matano/msk-authenticator";

export async function onEvent(event: AWSCDKAsyncCustomResource.OnEventRequest) {
  const bootstrapServers = event.ResourceProperties.BootstrapAddress.split(",");

  const kafka = new Kafka({
    clientId: `${(event.RequestType as string).toLowerCase()}-topic-handler`,
    brokers: bootstrapServers,
    ssl: true,
    sasl: {
      mechanism: "AWS_MSK_IAM",
      authenticationProvider: mskIamAuthenticator(process.env.AWS_REGION!!),
    }
  });
  const admin = kafka.admin();

  const physicalResourceId =
    event.PhysicalResourceId ??
    `${bootstrapServers[0]}#${event.ResourceProperties.TopicName}`; // TODO: whats actually happening here...

  switch (event.RequestType) {
    case "Create":
    // case "Update":
      return createTopic(admin, event, physicalResourceId);

    case "Update":
      return {
        PhysicalResourceId: physicalResourceId,
        Data: event.OldResourceProperties,
        } as AWSCDKAsyncCustomResource.OnEventResponse;
    case "Delete":
      return deleteTopic(admin, event);
  }
}

export async function createTopic(
  admin: Admin,
  event: AWSCDKAsyncCustomResource.OnEventRequest,
  physicalResourceId: string
): Promise<AWSCDKAsyncCustomResource.OnEventResponse> {
  const topicName = event.ResourceProperties.TopicName;
  if (!topicName) {
    throw new Error('"TopicName" is required');
  }

  const replicationFactor = event.ResourceProperties.ReplicationFactor;
  if (!replicationFactor) {
    throw new Error('"ReplicationFactor" is required');
  }
  const numPartitions = event.ResourceProperties.NumPartitions;
  if (!numPartitions) {
    throw new Error('"NumPartitions" is required');
  }

  const topics: ITopicConfig[] = [
    {
      topic: topicName,
      numPartitions,
      replicationFactor,
    },
  ];

  // console.debug("Connecting to kafka admin...");
  // await admin.connect();
  // console.debug(
  //   `Creating topic: ${topicName} (p: ${numPartitions}, rf: ${replicationFactor})...`
  // );

  // const result = await admin.createTopics({ topics });
  // console.debug(`Topic created`);
  // await admin.disconnect();

  return {
    PhysicalResourceId: physicalResourceId,
    Data: {
      TopicName: topicName,
      TopicConfig: {
        NumPartitions: numPartitions,
        ReplicationFactor: replicationFactor,
      },
    },
  };
}

export async function deleteTopic(
  admin: Admin,
  event: AWSCDKAsyncCustomResource.OnEventRequest
) {
  const topicName = event.ResourceProperties.TopicName;
  if (!topicName) {
    throw new Error('"TopicName" is required');
  }

  // console.debug("Connecting to kafka admin...");
  // await admin.connect();

  // console.debug(`Deleting topic: ${topicName}...`);
  // await admin.deleteTopics({ topics: [topicName] });

  // console.debug("Topic deleted");
  // await admin.disconnect();
  return;
}
