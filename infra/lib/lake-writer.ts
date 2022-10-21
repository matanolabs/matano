import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { RustFunctionCode, RustFunctionLayer } from "./rust-function-layer";

interface LakeWriterProps {
  outputBucketName: string;
  outputObjectPrefix: string;
}

export class LakeWriter extends Construct {
  lakeWriterLambda: lambda.Function;
  alertsLakeWriterLambda: lambda.Function;
  constructor(scope: Construct, id: string, props: LakeWriterProps) {
    super(scope, id);

    this.lakeWriterLambda = new lambda.Function(this, "Function", {
      code: RustFunctionCode.assetCode({ package: "lake_writer" }),
      handler: "main",
      memorySize: 1800,
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        RUST_LOG: "warn,lake_writer=info",
        OUT_BUCKET_NAME: props.outputBucketName,
        OUT_KEY_PREFIX: props.outputObjectPrefix,
      },
      timeout: cdk.Duration.seconds(5),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:*", "dynamodb:*", "s3:*"],
          resources: ["*"],
        }),
      ],
    });

    this.alertsLakeWriterLambda = new lambda.Function(this, "AlertsFunction", {
      code: RustFunctionCode.assetCode({ package: "lake_writer" }),
      handler: "main",
      memorySize: 1800,
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        RUST_LOG: "warn,lake_writer=info",
        OUT_BUCKET_NAME: props.outputBucketName,
        OUT_KEY_PREFIX: props.outputObjectPrefix,
      },
      timeout: cdk.Duration.seconds(60),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:*", "dynamodb:*", "s3:*"],
          resources: ["*"],
        }),
      ],
      // prevent concurrency
      reservedConcurrentExecutions: 1,
    });
  }
}
