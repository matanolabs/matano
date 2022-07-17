import * as path from "path"
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as firehose from "@aws-cdk/aws-kinesisfirehose-alpha";
import * as kinesisDestinations from "@aws-cdk/aws-kinesisfirehose-destinations-alpha";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as kinesis from "aws-cdk-lib/aws-kinesis";
import * as sns from "aws-cdk-lib/aws-sns";
import { SqsSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import { MatanoIcebergTable } from "../lib/iceberg";
import { getDirectories } from "../lib/utils";
import { readConfig } from "../lib/detections";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";

interface S3BucketWithNotificationsProps extends s3.BucketProps {
  maxReceiveCount?: number;
  eventType?: s3.EventType;
  s3Filters?: s3.NotificationKeyFilter[];
}
export class S3BucketWithNotifications extends s3.Bucket {
  queue: sqs.Queue;
  dlq: sqs.Queue;
  topic: sns.Topic;
  constructor(scope: Construct, id: string, props: S3BucketWithNotificationsProps) {
    super(scope, id, {
      bucketName: props.bucketName,
      ...props,
    });

    this.dlq = new sqs.Queue(this, "DLQ", {
      queueName: props.bucketName ? `${props.bucketName}-dlq` : undefined,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    this.queue = new sqs.Queue(this, "Queue", {
      queueName: props.bucketName ? `${props.bucketName}-queue` : undefined,
      deadLetterQueue: {
        queue: this.dlq,
        maxReceiveCount: props.maxReceiveCount ?? 3,
      },
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    this.topic = new sns.Topic(this, "Topic", {
      topicName: props.bucketName ? `${props.bucketName}-topic` : undefined,
    });

    this.topic.addSubscription(
      new SqsSubscription(this.queue, {
        rawMessageDelivery: true,
      })
    );

    this.addEventNotification(props.eventType ?? s3.EventType.OBJECT_CREATED_PUT, new s3n.SnsDestination(this.topic), ...(props.s3Filters ?? []));
  }
}

export const MATANO_DATABASE_NAME = "matano";


interface MatanoLogSourceProps {
  logSourceDirectory: string;
  outputBucket: s3.Bucket;
  firehoseRole: iam.Role;
}
const MATANO_GLUE_DATABASE_NAME = "matano";
class MatanoLogSource extends Construct {
  constructor(scope: Construct, id: string, props: MatanoLogSourceProps) {
    super(scope, id);

    const config = readConfig(props.logSourceDirectory, "log_source.yml")
    const { name: logSourceName, schema } = config;

    const matanoIcebergTable = new MatanoIcebergTable(this, "MatanoIcebergTable", {
      logSourceName,
      schema,
      icebergS3BucketName: props.outputBucket.bucketName,
    });

    const firehoseDestination = new kinesisDestinations.S3Bucket(props.outputBucket, {
      role: props.firehoseRole,
      logging: false,
      bufferingInterval: cdk.Duration.minutes(1),
      bufferingSize: cdk.Size.mebibytes(128),
      // We use multi record deaggregation to efficiently pack as many of our messages into a Kinesis Record.
      // As of now (07/2022), Kinesis Firehose (oddly) does not support record deaggregation without using dynamic partitioning.
      // We have no use of dynamic partitioning, but we need to use it to use multi record deaggregation, so we enable it and set up a
      // dummy metadata extraction query (`!{partitionKeyFromQuery:dummy}` will be mapped to the static "data", see below). Hilarious, I know.
      dataOutputPrefix: `lake/${logSourceName}/!{partitionKeyFromQuery:dummy}/`,
      errorOutputPrefix: `lake/${logSourceName}/error/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/!{firehose:error-output-type}/`
    });
    const firehoseStream = new firehose.DeliveryStream(this, "MatanoOutputFirehoseStream", {
      destinations: [firehoseDestination],
      role: props.firehoseRole,
    });
    const cfnDeliveryStream = firehoseStream.node.defaultChild as CfnDeliveryStream;

    // https://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html#using-iam-glue
    props.firehoseRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        "glue:GetTable",
        "glue:GetTableVersion",
        "glue:GetTableVersions"
      ],
      resources: [
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:table/${MATANO_GLUE_DATABASE_NAME}/${logSourceName}`,
        `arn:aws:glue:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:catalog`,
        "*",
      ]
    }));

    const conversionConfiguration: CfnDeliveryStream.DataFormatConversionConfigurationProperty = {
      inputFormatConfiguration: {
        deserializer: {
          hiveJsonSerDe: {
            timestampFormats: ["millis"],
          }
        }
      },
      outputFormatConfiguration: {
        serializer: {
          parquetSerDe: {
          }
        }
      },
      schemaConfiguration: {
        databaseName: MATANO_DATABASE_NAME,
        tableName: logSourceName,
        roleArn: props.firehoseRole.roleArn,
      },
    };

    const dynamicPartitioningConfiguration: CfnDeliveryStream.DynamicPartitioningConfigurationProperty = {
      enabled: true,
    };

    const processingConfiguration: CfnDeliveryStream.ProcessingConfigurationProperty  = {
      enabled: true,
      processors: [
        {
          type: 'RecordDeAggregation',
          parameters: [
            {
              parameterName: "SubRecordType",
              parameterValue: "DELIMITED",
            },
            {
              parameterName: "Delimiter",
              parameterValue: 'Cg'
            }
          ]
        },
        {
          type: 'MetadataExtraction',
          parameters: [
            {
              parameterName: 'MetadataExtractionQuery',
              // This is is a dummy query that will always return {"dummy": "data"}. See above for why.
              parameterValue: '{dummy: (select("@timestamp") | map_values("data") | ."@timestamp")}',
            },
            {
              parameterName: 'JsonParsingEngine',
              parameterValue: 'JQ-1.6',
            },
          ],
        },
      ],
    };

    const extendedS3DestinationConfiguration = cfnDeliveryStream.extendedS3DestinationConfiguration as CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty;

    (extendedS3DestinationConfiguration.dynamicPartitioningConfiguration as any) = dynamicPartitioningConfiguration;
    (extendedS3DestinationConfiguration.processingConfiguration as any) = processingConfiguration;
    (extendedS3DestinationConfiguration.dataFormatConversionConfiguration as any) = conversionConfiguration;

    firehoseStream.node.addDependency(matanoIcebergTable);
    firehoseStream.node.addDependency(props.firehoseRole);

  }
}

interface DPStorageStackProps extends MatanoStackProps {
}


export class DPStorageStack extends MatanoStack {
  rawEventsBucketWithNotifications: S3BucketWithNotifications;
  outputEventsBucketWithNotifications: S3BucketWithNotifications;
  constructor(scope: Construct, id: string, props: DPStorageStackProps) {
    super(scope, id, props);

    this.rawEventsBucketWithNotifications = new S3BucketWithNotifications(this, "RawEventsBucket", {
      // bucketName: "matano-raw-events",
    });

    this.outputEventsBucketWithNotifications = new S3BucketWithNotifications(this, "OutputEventsBucket", {
      // bucketName: "matano-output-events",
      s3Filters: [
        { prefix: "lake", suffix: "parquet" },
      ]
    });

    const matanoDatabase = new glue.CfnDatabase(this, "MatanoDatabase", {
      databaseInput: {
        name: MATANO_DATABASE_NAME,
        description: "Glue database storing Matano Iceberg tables.",
        locationUri: `s3://${this.outputEventsBucketWithNotifications.bucketName}/lake`,
      },
      catalogId: cdk.Fn.ref("AWS::AccountId"),
    });

    const firehoseRole = new iam.Role(this, "MatanoFirehoseRole", {
      assumedBy: new iam.ServicePrincipal("firehose.amazonaws.com", {
        conditions: {
          "StringEquals": { "sts:ExternalId": cdk.Stack.of(this).account }
        },
      }),
    });

    console.log(this.matanoUserDirectory);

    const logSourcesDirectory = path.join(this.matanoUserDirectory, "log_sources");
    const logSources = getDirectories(logSourcesDirectory);

    for (const logSource of logSources) {
      new MatanoLogSource(this, `MatanoLogSource-${logSource}`, {
        logSourceDirectory: path.join(logSourcesDirectory, logSource),
        outputBucket: this.outputEventsBucketWithNotifications,
        firehoseRole,
      })
    }

  }
}
