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
import { LambdaSubscription, SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";

interface MatanoAlertingProps {
  integrationsStore?: IntegrationsStore;
  alertTrackerTable: ddb.Table;
  duplicatesTable: ddb.Table;
}

export class MatanoAlerting extends Construct {
  ruleMatchesTopic: sns.Topic;
  alertingTopic: sns.Topic;
  constructor(scope: Construct, id: string, props: MatanoAlertingProps) {
    super(scope, id);

    this.ruleMatchesTopic = new sns.Topic(this, "RuleMatchesTopic", {
      displayName: "MatanoRuleMatchesTopic",
      fifo: true,
      contentBasedDeduplication: true,
    });
    const ruleMatchesDlq = new sqs.Queue(this, "RuleMatchesDlq", {
      fifo: true,
      contentBasedDeduplication: true,
    });

    const internalAlertsTopic = new sns.Topic(this, "InternalAlertsTopic", {
      displayName: "MatanoInternalAlertsTopic",
      fifo: true,
      contentBasedDeduplication: true,
    });

    const internalAlertsDlq = new sqs.Queue(this, "InternalAlertsDlq", {
      fifo: true,
      contentBasedDeduplication: true,
    });
    const internalAlertsQueue = new sqs.Queue(this, "InternalAlertsQueue", {
      fifo: true,
      visibilityTimeout: cdk.Duration.seconds(40),
      deadLetterQueue: {
        queue: internalAlertsDlq,
        maxReceiveCount: 1000,
      },
      retentionPeriod: cdk.Duration.days(14),
      fifoThroughputLimit: sqs.FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
      deduplicationScope: sqs.DeduplicationScope.MESSAGE_GROUP,
      contentBasedDeduplication: true,
    });

    internalAlertsTopic.addSubscription(
      new SqsSubscription(internalAlertsQueue, {
        rawMessageDelivery: true,
      })
    );

    this.alertingTopic = new sns.Topic(this, "Topic", {
      displayName: "MatanoAlertingTopic",
    });

    if (props.integrationsStore) {
      const alertForwarder = new AlertForwarder(this, "AlertForwarder", {
        integrationsStore: props.integrationsStore,
        alertTrackerTable: props.alertTrackerTable,
        internalAlertsQueue: internalAlertsQueue,
        duplicatesTable: props.duplicatesTable,
      });

      const alertWriter = new AlertWriter(this, "AlertWriter", {
        integrationsStore: props.integrationsStore,
        alertTrackerTable: props.alertTrackerTable,
        ruleMatchesSnsTopic: this.ruleMatchesTopic,
        ruleMatchesDlq: ruleMatchesDlq,
        internalAlertsTopic: internalAlertsTopic,
        alertingSnsTopic: this.alertingTopic,
      });
    }
  }
}

interface AlertForwarderProps {
  internalAlertsQueue: sqs.Queue;
  integrationsStore: IntegrationsStore;
  alertTrackerTable: ddb.Table;
  duplicatesTable: ddb.Table;
}

export class AlertForwarder extends Construct {
  function: lambda.Function;
  dlq: sqs.Queue;

  constructor(scope: Construct, id: string, props: AlertForwarderProps) {
    super(scope, id);

    const destinationToSecretArnMap = Object.fromEntries(
      Object.entries(props.integrationsStore?.integrationsSecretMap ?? {}).map(([d, s]) => [d, s.secretArn])
    );

    this.function = new lambda.Function(this, "Default", {
      code: RustFunctionCode.assetCode({ package: "alert_forwarder" }),
      handler: "main",
      memorySize: 128,
      description: "[Matano] Forward alerts to external destinations",
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        RUST_LOG: "warn,alert_forwarder=info",
        ALERT_TRACKER_TABLE_NAME: props.alertTrackerTable.tableName,
        DESTINATION_TO_CONFIGURATION_MAP: JSON.stringify(props.integrationsStore?.integrationsInfoMap ?? {}),
        DESTINATION_TO_SECRET_ARN_MAP: JSON.stringify(destinationToSecretArnMap),
      },
      timeout: cdk.Duration.seconds(38),
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
    props.duplicatesTable.grantReadWriteData(this.function);

    // TODO: can up this to 10...
    this.function.addEventSource(
      new SqsEventSource(props.internalAlertsQueue, {
        batchSize: 1,
      })
    );
  }
}

interface AlertWriterProps {
  ruleMatchesSnsTopic: sns.Topic;
  ruleMatchesDlq: sqs.Queue;
  alertingSnsTopic: sns.Topic;
  internalAlertsTopic: sns.Topic;
  integrationsStore: IntegrationsStore;
  alertTrackerTable: ddb.Table;
}

export class AlertWriter extends Construct {
  function: lambda.Function;
  queue: sqs.Queue;
  dlq: sqs.Queue;

  constructor(scope: Construct, id: string, props: AlertWriterProps) {
    super(scope, id);

    this.dlq = new sqs.Queue(this, "DLQ", {
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      fifo: true,
      contentBasedDeduplication: true,
    });

    const functionTimeout = cdk.Duration.minutes(3);

    this.queue = new sqs.Queue(this, "Queue", {
      deadLetterQueue: {
        queue: this.dlq,
        maxReceiveCount: 3,
      },
      fifo: true,
      contentBasedDeduplication: true,
      fifoThroughputLimit: sqs.FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
      deduplicationScope: sqs.DeduplicationScope.MESSAGE_GROUP,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      visibilityTimeout: cdk.Duration.seconds(Math.max(functionTimeout!.toSeconds(), 30)),
    });

    this.function = new lambda.Function(this, "Default", {
      code: RustFunctionCode.assetCode({ package: "alert_writer" }),
      handler: "main",
      memorySize: 512,
      description: "[Matano] Extracts alerts from rule matches and publishes them.",
      // prevent concurrency
      reservedConcurrentExecutions: 1,
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        RUST_LOG: "warn,alert_writer=info",
        ALERT_TRACKER_TABLE_NAME: props.alertTrackerTable.tableName,
        RULE_MATCHES_QUEUE_URL: this.queue.queueUrl,
        RULE_MATCHES_DLQ_URL: props.ruleMatchesDlq.queueUrl,
        ALERTING_SNS_TOPIC_ARN: props.alertingSnsTopic.topicArn,
        INTERNAL_ALERTS_TOPIC_ARN: props.internalAlertsTopic.topicArn,
      },
      timeout: functionTimeout,
    });

    props.ruleMatchesSnsTopic.addSubscription(
      new SqsSubscription(this.queue, {
        rawMessageDelivery: true,
      })
    );
    props.alertTrackerTable.grantReadWriteData(this.function);
    this.queue.grantSendMessages(this.function);
    props.ruleMatchesDlq.grantSendMessages(this.function);
    props.internalAlertsTopic.grantPublish(this.function);
    props.alertingSnsTopic.grantPublish(this.function);

    this.function.addEventSource(
      new SqsEventSource(this.queue, {
        batchSize: 10,
      })
    );
  }
}
