import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import { RustFunctionCode, RustFunctionLayer } from "./rust-function-layer";

interface LakeWriterProps {
  realtimeBucket: s3.IBucket;
  outputBucket: s3.IBucket;
  outputObjectPrefix: string;
  ruleMatchesSnsTopic: sns.Topic;
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
        OUT_BUCKET_NAME: props.outputBucket.bucketName,
        OUT_KEY_PREFIX: props.outputObjectPrefix,
      },
      timeout: cdk.Duration.seconds(60),
    });
    props.realtimeBucket.grantRead(this.lakeWriterLambda);
    props.outputBucket.grantReadWrite(this.lakeWriterLambda);

    this.alertsLakeWriterLambda = new lambda.Function(this, "AlertsFunction", {
      code: RustFunctionCode.assetCode({ package: "lake_writer" }),
      handler: "main",
      memorySize: 1800,
      runtime: lambda.Runtime.PROVIDED_AL2,
      environment: {
        RUST_LOG: "warn,lake_writer=info",
        OUT_BUCKET_NAME: props.outputBucket.bucketName,
        OUT_KEY_PREFIX: props.outputObjectPrefix,
        RULE_MATCHES_SNS_TOPIC_ARN: props.ruleMatchesSnsTopic.topicArn,
      },
      timeout: cdk.Duration.seconds(120),
      // prevent concurrency
      reservedConcurrentExecutions: 1,
    });
    props.realtimeBucket.grantRead(this.alertsLakeWriterLambda);
    props.outputBucket.grantReadWrite(this.alertsLakeWriterLambda);
    props.ruleMatchesSnsTopic.grantPublish(this.alertsLakeWriterLambda);
  }
}
