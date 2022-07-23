import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as firehose from "@aws-cdk/aws-kinesisfirehose-alpha";
import * as kinesisDestinations from "@aws-cdk/aws-kinesisfirehose-destinations-alpha";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";
import { MatanoIcebergTable } from "../lib/iceberg";
import { readConfig } from "../lib/utils";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";
import { KafkaTopic } from "../lib/KafkaTopic";
import { IKafkaCluster, KafkaCluster } from "../lib/KafkaCluster";

export const MATANO_DATABASE_NAME = "matano";

interface MatanoLogSourceProps {
  logSourceDirectory: string;
  outputBucket: s3.Bucket;
  firehoseRole: iam.Role;
  kafkaCluster: IKafkaCluster;
}
const MATANO_GLUE_DATABASE_NAME = "matano";
export class MatanoLogSource extends Construct {
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
              parameterValue: '{dummy: (select("ts") | map_values("data") | .ts)}',
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

    new KafkaTopic(this, `KafkaTopic-${logSourceName}-Raw`, {
        topicName: `raw.${logSourceName}`,
        cluster: props.kafkaCluster,
        topicConfig: {
            numPartitions: 1,
            replicationFactor: 1,
        }
    });

    new KafkaTopic(this, `KafkaTopic-${logSourceName}-Output`, {
      topicName: `output.${logSourceName}`,
      cluster: props.kafkaCluster,
      topicConfig: {
          numPartitions: 1,
          replicationFactor: 1,
      }
    });
  }
}
