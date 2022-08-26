import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { RustFunctionLayer } from "./rust-function-layer";

interface TransformerProps {
  realtimeBucketName: string;
  logSourcesConfigurationPath: string;
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

    this.transformerLambda = new lambda.Function(this, "Lambda", {
      code: lambda.Code.fromAsset("./src", {
        bundling: {
          volumes: [
            {
              hostPath: props.logSourcesConfigurationPath,
              containerPath: "/asset-input/log_sources_configuration.json",
            },
          ],
          image: this.rustFunctionLayer.image,
          command: [
            "bash",
            "-c",
            "cp /asset-input/log_sources_configuration.json /asset-output/log_sources_configuration.json",
          ],
        },
      }),
      handler: "main",
      memorySize: 1800,
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        ...this.rustFunctionLayer.environmentVariables,
        MATANO_REALTIME_BUCKET_NAME: props.realtimeBucketName,
      },
      layers: [this.rustFunctionLayer.layer],
      timeout: cdk.Duration.seconds(5),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:*", "dynamodb:*", "s3:*"],
          resources: ["*"],
        }),
      ],
    });
  }
}
