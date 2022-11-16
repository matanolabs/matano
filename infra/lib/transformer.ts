import * as path from "path";
import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { RustFunctionLayer } from "./rust-function-layer";

interface TransformerProps {
  realtimeBucketName: string;
  realtimeTopic: sns.Topic;
  matanoSourcesBucketName: string;
  logSourcesConfigurationPath: string;
  schemasLayer: lambda.LayerVersion;
  sqsMetadata: string;
}

export class Transformer extends Construct {
  transformerLambda: lambda.Function;
  rustFunctionLayer: RustFunctionLayer;
  constructor(scope: Construct, id: string, props: TransformerProps) {
    super(scope, id);

    this.rustFunctionLayer = new RustFunctionLayer(this, "Layer", {
      package: "transformer",
      // Useful so library logs show up in CloudWatch
      setupLogging: true,
    });

    this.transformerLambda = new lambda.Function(this, "Function", {
      code: lambda.Code.fromAsset(props.logSourcesConfigurationPath),
      handler: "main",
      memorySize: 3008,
      runtime: lambda.Runtime.PROVIDED_AL2,
      architecture: this.rustFunctionLayer.arch,
      environment: {
        ...this.rustFunctionLayer.environmentVariables,
        MATANO_SOURCES_BUCKET: props.matanoSourcesBucketName,
        MATANO_REALTIME_BUCKET_NAME: props.realtimeBucketName,
        MATANO_REALTIME_TOPIC_ARN: props.realtimeTopic.topicArn,
        SQS_METADATA: props.sqsMetadata,
      },
      layers: [this.rustFunctionLayer.layer, props.schemasLayer],
      timeout: cdk.Duration.seconds(30),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:*", "dynamodb:*", "s3:*", "sns:*"],
          resources: ["*"],
        }),
      ],
    });
  }
}
