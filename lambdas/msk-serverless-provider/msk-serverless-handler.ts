import * as response from "cfn-response";
import * as AWS from "aws-sdk";

export async function onEvent(event: AWSCDKAsyncCustomResource.OnEventRequest) {
  const kafka = new AWS.Kafka();

  switch (event.RequestType) {
    case "Create":
      return createCluster(kafka, event);

    case "Update":
      return createCluster(kafka, event);

    case "Delete":
      return deleteCluster(kafka, event);
  }
}

export async function createCluster(
  kafka: AWS.Kafka,
  event: AWSCDKAsyncCustomResource.OnEventRequest,
): Promise<AWSCDKAsyncCustomResource.OnEventResponse> {
  
  const { ServiceToken, ...parameters } = event.ResourceProperties as any;
  if (!parameters.Serverless) {
    throw new Error("Serverless property is required for MSK Serverless cluster." );
  }
  parameters.Serverless = JSON.parse(parameters.Serverless);
  // console.log("parameters", parameters);
  const response = await kafka.createClusterV2(parameters).promise();

  let prevState = "CREATING";
  while (true) {
    const cluster = await kafka.describeClusterV2({
      ClusterArn: response.ClusterArn!!,
    }).promise();
    if (cluster.ClusterInfo!!.State !== prevState && cluster.ClusterInfo!!.State != "CREATING") {
      if (cluster.ClusterInfo!!.State !== "ACTIVE") {
        throw new Error(`Cluster creation failed: ${cluster.ClusterInfo!!.State}`);
      }
      break;
    }
    prevState = cluster.ClusterInfo!!.State;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  const physicalResourceId = event.PhysicalResourceId ?? response.ClusterArn; //TODO: whats actually happening here...
  return {
    PhysicalResourceId: physicalResourceId,
    Data: {
      ClusterArn: response.ClusterArn,
      ClusterName: response.ClusterName,
    },
  };
}

export async function updateCluster(
  kafka: AWS.Kafka,
  event: AWSCDKAsyncCustomResource.OnEventRequest,
): Promise<AWSCDKAsyncCustomResource.OnEventResponse> {
  const { ServiceToken, ...parameters } = event.ResourceProperties as any;
  if (!parameters.Serverless) {
    throw new Error("Serverless property is required for MSK Serverless cluster." );
  }
  parameters.Serverless = JSON.parse(parameters.Serverless);
  console.log("parameters", parameters);
  const cluster = await kafka.describeClusterV2({
    ClusterArn: event.OldResourceProperties!!.ClusterArn!!,
  }).promise();
  // const response = await kafka.updateClusterV2(parameters).promise();

  const physicalResourceId = event.PhysicalResourceId; //TODO: whats actually happening here..
  return {
    PhysicalResourceId: physicalResourceId,
    Data: {
      ClusterArn: cluster.ClusterInfo?.ClusterArn,
      ClusterName: cluster.ClusterInfo?.ClusterName,
    },
  };
}

export async function deleteCluster(
  kafka: AWS.Kafka,
  event: AWSCDKAsyncCustomResource.OnEventRequest
) {
  const clusterArn = event.OldResourceProperties?.ClusterArn ?? event.ResourceProperties?.ClusterArn;
  if (!clusterArn) {
    return;
    // throw new Error('"ClusterArn" is required');
  }

  const response = await kafka.deleteCluster({
    ClusterArn: clusterArn,
  }).promise();

  if (response.State !== "DELETING") {
    throw new Error(`Cluster deletion failed: ${response.State}`);
  }

  return;
}
