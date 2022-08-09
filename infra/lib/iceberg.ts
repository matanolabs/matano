import * as path from "path";
import * as crypto from 'crypto';
import { Construct, Node } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cr from "aws-cdk-lib/custom-resources";
import { CustomResource } from "aws-cdk-lib";
import { execSync } from "child_process";
import { S3BucketWithNotifications } from "./s3-bucket-notifs";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";

interface MatanoIcebergTableProps {
  logSourceName: string;
  schema?: Record<string, any>;
  icebergS3BucketName: string;
}

export class MatanoIcebergTable extends Construct {
  constructor(scope: Construct, id: string, props: MatanoIcebergTableProps) {
    super(scope, id);

    const resource = new CustomResource(this, "Resource", {
      serviceToken: IcebergTableProvider.getOrCreate(this, { icebergS3BucketName: props.icebergS3BucketName }),
      resourceType: "Custom::MatanoIcebergTable",
      properties: {
        logSourceName: props.logSourceName,
        schema: props.schema,
      },
    });
  }
}

const md5 = (s: string) => crypto.createHash('md5').update(s).digest("hex");

interface IcebergTableProviderProps {
  icebergS3BucketName: string;
}

export class IcebergTableProvider extends Construct {
  provider: cr.Provider;

  public static getOrCreate(scope: Construct, props: IcebergTableProviderProps) {
    const stack = cdk.Stack.of(scope);
    const hash = "" //md5(props.icebergS3BucketName); // TODO: or should it be in properties? think multi...
    const id = `MatanoCustomResourceIcebergTableProvider${hash}`;
    const x = (stack.node.tryFindChild(id) as IcebergTableProvider) || new IcebergTableProvider(stack, id, props);
    return x.provider.serviceToken;
  }

  constructor(scope: Construct, id: string, props: IcebergTableProviderProps) {
    super(scope, id);

    const codePath = path.resolve(path.join("../lib/java/matano"));

    const providerFunc = new lambda.Function(this, "MatanoIcebergCRProviderFunc", {
      runtime: lambda.Runtime.JAVA_11,
      handler: "com.matano.iceberg.MatanoIcebergTableCustomResource::handleRequest",
      description: "This function provides the Cloudformation custom resource for a Matano log source Iceberg table.",
      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      environment: {
        MATANO_ICEBERG_BUCKET: props.icebergS3BucketName,
      },
      code: lambda.Code.fromAsset(codePath, {
        assetHashType: cdk.AssetHashType.OUTPUT,
        bundling: {
          image: lambda.Runtime.JAVA_11.bundlingImage,
          command: ["./gradlew", ":iceberg_table_cfn:release", ],
          local:  {
            tryBundle(outputDir, options) {
              execSync(`gradle -p ${codePath} :iceberg_table_cfn:release && cp ../lib/java/matano/iceberg_table_cfn/build/libs/output.jar ${outputDir}/output.jar`)
              return true;
            },
          }
        },
      }),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["glue:*", "s3:*"],
          resources: ["*"],
        }),
      ],
    });

    this.provider = new cr.Provider(this, "MatanoIcebergTableCrProvider", {
      onEventHandler: providerFunc,
    });
  }
}


interface IcebergMetadataProps {
  outputBucket: S3BucketWithNotifications;
}
export class IcebergMetadata extends Construct {
  constructor(scope: Construct, id: string, props: IcebergMetadataProps) {
    super(scope, id);

    const codePath = path.resolve(path.join("../lib/java/matano"));

    const duplicatesTable = new ddb.Table(this, "IcebergMetadataDuplicatesTable", {
      partitionKey: { name: "sequencer", type: ddb.AttributeType.STRING },
      timeToLiveAttribute: "ttl",
    });

    const lambdaFunction = new lambda.Function(this, "MatanoIcebergMetadataWriterFunction", {
      description: "This function ingests written input files into an Iceberg table.",
      runtime: lambda.Runtime.JAVA_11,
      memorySize: 512,
      handler: "com.matano.iceberg.IcebergMetadataHandler::handleRequest",
      timeout: cdk.Duration.minutes(3),
      environment: {
        DUPLICATES_DDB_TABLE_NAME: duplicatesTable.tableName,
      },
      code: lambda.Code.fromAsset(codePath, {
        assetHashType: cdk.AssetHashType.OUTPUT,
        bundling: {
          image: lambda.Runtime.JAVA_11.bundlingImage,
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

    duplicatesTable.grantReadWriteData(lambdaFunction);

    const eventSource = new SqsEventSource(props.outputBucket.queue, {});
    lambdaFunction.addEventSource(eventSource);

  }
}
