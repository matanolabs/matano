import * as path from "path";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { CustomResource, Stack } from "aws-cdk-lib";
import { Construct, Node } from "constructs";
import * as cr from "aws-cdk-lib/custom-resources";
import { NodejsFunction, NodejsFunctionProps } from "aws-cdk-lib/aws-lambda-nodejs";

import { IKafkaCluster } from "./KafkaCluster";
import { MatanoStack } from "./MatanoStack";

interface KafkaTopicConfig {
  /**
   * The number of partitions (shards) for the topic.
   */
  numPartitions: number;
  /**
   * The number of replicas for each partition. (unnecessary for serverless)
   */
  replicationFactor?: number;
}
interface KafkaTopicProps {
  /**
   * The cluster in which the topic will be created.
   */
  readonly cluster: IKafkaCluster;

  /**
   * The topic name.
   *
   */
  readonly topicName: string;

  /**
   * The configuration for the topic. (partitions, replicas, etc.)
   */
  readonly topicConfig: KafkaTopicConfig;
}

export class KafkaTopic extends Construct {
  public readonly topicName: string;
  public readonly topicConfig: KafkaTopicConfig;

  constructor(scope: Construct, id: string, props: KafkaTopicProps) {
    super(scope, id);

    const topicProvider = KafkaTopicProvider.getOrCreate(this, props.cluster);

    const resource = new CustomResource(this, "Resource", {
      serviceToken: topicProvider.serviceToken,
      resourceType: "Custom::KafkaTopic",
      properties: {
        BootstrapAddress: props.cluster.bootstrapAddress,
        TopicName: props.topicName,
        NumPartitions: props.topicConfig.numPartitions,
        ReplicationFactor: props.topicConfig.replicationFactor,
      },
    });

    this.topicName = props.topicName;
    this.topicConfig = props.topicConfig;
  }
}

interface KafkaTopicProviderProps {
  cluster: IKafkaCluster;
  vpc: ec2.IVpc;
}

class KafkaTopicProvider extends Construct {
  private readonly provider: cr.Provider;
  /**
   * Returns the singleton provider.
   */
  public static getOrCreate(scope: Construct, cluster: IKafkaCluster) {
    const policyStatement = undefined;

    const stack = Stack.of(scope) as MatanoStack;
    const id = `KafkaTopicProvider`;
    const x =
      (Node.of(stack).tryFindChild(id) as KafkaTopicProvider) ||
      new KafkaTopicProvider(stack, id, {
        cluster,
        vpc: stack.matanoVpc,
      });
    if (policyStatement != null) {
      x.provider.onEventHandler.addToRolePolicy(policyStatement);
      x.provider.isCompleteHandler?.addToRolePolicy(policyStatement);
    }
    return x.provider;
  }

  private constructor(scope: Construct, id: string, props: KafkaTopicProviderProps) {
    super(scope, id);

    const vpcProps: Partial<NodejsFunctionProps> = {
      vpc: props.vpc,
      vpcSubnets: {
        subnets: props.vpc.publicSubnets,
      },
      securityGroups: [props.cluster.securityGroup],
    }

    // Lambda function to support cloudformation custom resource to create kafka topics.
    const kafkaTopicHandler = new NodejsFunction(this, "KafkaTopicHandler", {
      functionName: "KafkaTopicHandler",
      entry: "../lambdas/kafka-topic-provider/kafka-topic-handler.ts",
      depsLockFilePath: "../lambdas/kafka-topic-provider/package-lock.json",
      handler: "onEvent",
      runtime: lambda.Runtime.NODEJS_14_X,
      ...vpcProps,
      allowPublicSubnet: true,
      // securityGroups: [vpcStack.lambdaSecurityGroup],
      timeout: cdk.Duration.minutes(5),
    });

    kafkaTopicHandler.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["kafka:*", "kafka-cluster:*"],
        resources: ["*"],
      })
    );

    this.provider = new cr.Provider(this, "kafka-topic-provider", {
      onEventHandler: kafkaTopicHandler,
    });
  }
}
