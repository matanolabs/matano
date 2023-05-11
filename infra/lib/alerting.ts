import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as ddb from "aws-cdk-lib/aws-dynamodb";

import * as sns from "aws-cdk-lib/aws-sns";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { RustFunctionCode } from "./rust-function-layer";
import { IntegrationsStore } from "./integrations-store";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";

interface MatanoAlertingProps {
  integrationsStore?: IntegrationsStore;
  alertTrackerTable: ddb.Table;
}

export class MatanoAlerting extends Construct {
  ruleMatchesTopic: sns.Topic;
  alertingTopic: sns.Topic;
  constructor(scope: Construct, id: string, props: MatanoAlertingProps) {
    super(scope, id);

    this.ruleMatchesTopic = new sns.Topic(this, "RuleMatchesTopic", {
      displayName: "MatanoRuleMatchesTopic",
    });
    this.alertingTopic = new sns.Topic(this, "Topic", {
      displayName: "MatanoAlertingTopic",
    });

    if (props.integrationsStore) {
      const alertForwarder = new AlertForwarder(this, "Forwarder", {
        integrationsStore: props.integrationsStore,
        alertTrackerTable: props.alertTrackerTable,
        ruleMatchesSnsTopic: this.ruleMatchesTopic,
        alertingSnsTopic: this.alertingTopic,
      });
    }
  }
}

interface AlertForwarderProps {
  ruleMatchesSnsTopic: sns.Topic;
  alertingSnsTopic: sns.Topic;
  integrationsStore: IntegrationsStore;
  alertTrackerTable: ddb.Table;
}

export class AlertForwarder extends Construct {
  function: lambda.Function;
  queue: sqs.Queue;
  dlq: sqs.Queue;

  constructor(scope: Construct, id: string, props: AlertForwarderProps) {
    super(scope, id);

    const destinationToSecretArnMap = Object.fromEntries(
      Object.entries(props.integrationsStore?.integrationsSecretMap ?? {}).map(([d, s]) => [d, s.secretArn])
    );

    this.function = new lambda.Function(this, "Default", {
      code: RustFunctionCode.assetCode({ package: "alert_forwarder" }),
      handler: "main",
      memorySize: 512,
      description: "Forward Matano alerts to integrations (e.g. Slack)",
      // prevent concurrency
      reservedConcurrentExecutions: 1,
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        RUST_LOG: "warn,alert_forwarder=info",
        ALERT_TRACKER_TABLE_NAME: props.alertTrackerTable.tableName,
        ALERTING_SNS_TOPIC_ARN: props.alertingSnsTopic.topicArn,
        DESTINATION_TO_CONFIGURATION_MAP: JSON.stringify(props.integrationsStore?.integrationsInfoMap ?? {}),
        DESTINATION_TO_SECRET_ARN_MAP: JSON.stringify(destinationToSecretArnMap),
      },
      timeout: cdk.Duration.seconds(30),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:GetSecretValue"],
          resources: Object.values(destinationToSecretArnMap),
        }),
        new iam.PolicyStatement({
          actions: ["ses:SendEmail", "ses:SendRawEmail"],
          resources: ["*"],
        }),
      ],
    });
    props.alertTrackerTable.grantReadWriteData(this.function);

    this.dlq = new sqs.Queue(this, "DLQ", {
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    this.queue = new sqs.Queue(this, "Queue", {
      deadLetterQueue: {
        queue: this.dlq,
        maxReceiveCount: 3,
      },
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      visibilityTimeout: cdk.Duration.seconds(Math.max(this.function.timeout!.toSeconds(), 30)),
    });

    props.ruleMatchesSnsTopic.addSubscription(
      new SqsSubscription(this.queue, {
        rawMessageDelivery: true,
      })
    );
    props.alertingSnsTopic.grantPublish(this.function);

    this.function.addEventSource(
      new SqsEventSource(this.queue, {
        batchSize: 100,
        maxBatchingWindow: cdk.Duration.seconds(20),
      })
    );
  }
}
