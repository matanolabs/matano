import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { RustFunctionLayer } from "./rust-function-layer";

interface LakeIngestionProps {}

export class LakeIngestion extends Construct {
  lakeIngestionLambda: lambda.Function;
  constructor(scope: Construct, id: string, props: LakeIngestionProps) {
    super(scope, id);

    const layer = new RustFunctionLayer(this, "LakeIngestionLambdaLayer", {
      package: "lake_writer",
      // Useful so library logs show up in CloudWatch
      setupLogging: true,
    });

    this.lakeIngestionLambda = new lambda.Function(this, "LakeIngestionLambda", {
      code: lambda.Code.fromAsset("./src"),
      handler: "main",
      memorySize: 1800,
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        ...layer.environmentVariables,
        OUT_BUCKET_NAME: "matanodpcommonstack-raweventsbucket024cde12-1x4x1mvqkuk5d",
        OUT_KEY_PREFIX: "lake" ,
      },
      layers: [layer.layer],
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
