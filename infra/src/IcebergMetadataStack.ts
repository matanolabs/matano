import * as path from "path";
import { Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import { MatanoStack, MatanoStackProps } from "../lib/MatanoStack";
import { execSync } from "child_process";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { S3BucketWithNotifications } from "../lib/s3-bucket-notifs";

interface IcebergMetadataStackProps extends MatanoStackProps {
  outputBucket: S3BucketWithNotifications;
}

export class IcebergMetadataStack extends MatanoStack {
  constructor(scope: Construct, id: string, props: IcebergMetadataStackProps) {
    super(scope, id, props);

    const codePath = path.resolve(path.join("../lib/java/matano"));

    const lambdaFunction = new lambda.Function(this, "MatanoIcebergMetadataWriterFunction", {
      description: "This function ingests written input files into an Iceberg table.",
      runtime: lambda.Runtime.JAVA_11,
      memorySize: 512,
      handler: "com.matano.iceberg.IcebergMetadataHandler::handleRequest",
      timeout: cdk.Duration.minutes(3),
      code: lambda.Code.fromAsset(codePath, {
        assetHashType: cdk.AssetHashType.OUTPUT,
        bundling: {
          image: lambda.Runtime.JAVA_11.bundlingImage,
          volumes: [
            {
              hostPath: path.resolve("~/.gradle/caches"),
              containerPath: "/root/.gradle/caches",
            },
          ],
          command: ["./gradlew", ":iceberg_metadata:release",],
          local:  {
            tryBundle(outputDir, options) {
              execSync(`gradle -p ${codePath} :iceberg_metadata:release && cp ../lib/java/matano/iceberg_metadata/build/libs/output.jar ${outputDir}/output.jar`)
              return true;
            },
          }
        },
      }),
      initialPolicy: [new iam.PolicyStatement({
        actions: ["glue:*", "s3:*",],
        resources: ["*"],
      })],
    });

    const eventSource = new SqsEventSource(props.outputBucket.queue, {});
    lambdaFunction.addEventSource(eventSource);
  }
}
