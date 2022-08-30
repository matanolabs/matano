import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { RustFunctionCode, RustFunctionLayer } from "./rust-function-layer";

interface LakeIngestionProps {
  outputBucketName: string;
  outputObjectPrefix: string;
}

export class LakeIngestion extends Construct {
  lakeIngestionLambda: lambda.Function;
  constructor(scope: Construct, id: string, props: LakeIngestionProps) {
    super(scope, id);

    this.lakeIngestionLambda = new lambda.Function(this, "LakeIngestionLambda", {
      code: RustFunctionCode.assetCode({ package: "lake_writer" }),
      handler: "main",
      memorySize: 1800,
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        OUT_BUCKET_NAME: props.outputBucketName, // "matanodpcommonstack-raweventsbucket024cde12-1x4x1mvqkuk5d",
        OUT_KEY_PREFIX:  props.outputObjectPrefix// "lake" ,
      },
      timeout: cdk.Duration.seconds(5),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["secretsmanager:*", "dynamodb:*", "s3:*", ],
          resources: ["*"],
        }),
      ],
    });
  }
}
