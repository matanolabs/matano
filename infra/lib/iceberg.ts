import * as path from "path";
import * as crypto from "crypto";
import * as YAML from "yaml";
import { Construct, Node } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as ddb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cr from "aws-cdk-lib/custom-resources";
import { CustomResource } from "aws-cdk-lib";
import { execSync } from "child_process";
import { S3BucketWithNotifications } from "./s3-bucket-notifs";
import { AwsCliLayer } from "aws-cdk-lib/lambda-layer-awscli";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { getLocalAsset, makeLambdaSnapstart } from "./utils";
import { MatanoStack } from "./MatanoStack";

interface MatanoSchemasProps {
  schemaOutputPath: string;
  tables: string[];
}

export class MatanoSchemas extends Construct {
  constructor(scope: Construct, id: string, props: MatanoSchemasProps) {
    super(scope, id);

    const resource = new CustomResource(this, "Resource", {
      serviceToken: SchemasProvider.getOrCreate(this, {}),
      resourceType: "Custom::MatanoSchemas",
      properties: {
        schemaOutputPath: props.schemaOutputPath,
        logSources: props.tables,
      },
    });
  }
}

interface SchemasProviderProps {}

export class SchemasProvider extends Construct {
  provider: cr.Provider;

  public static getOrCreate(scope: Construct, props: SchemasProviderProps) {
    const stack = cdk.Stack.of(scope);
    const hash = ""; //md5(props.icebergS3BucketName); // TODO: or should it be in properties? think multi...
    const id = `MatanoCustomResourceSchemasProvider${hash}`;
    const x = (stack.node.tryFindChild(id) as SchemasProvider) || new SchemasProvider(stack, id, props);
    return x.provider.serviceToken;
  }

  constructor(scope: Construct, id: string, props: SchemasProviderProps) {
    super(scope, id);

    const providerFunc = new lambda.Function(this, "Function", {
      runtime: lambda.Runtime.JAVA_11,
      description: "[Matano] Custom resource for creating schemas.",
      handler: "com.matano.iceberg.MatanoSchemasLayerCustomResource::handleRequest",
      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      environment: {
        ASSETS_BUCKET_NAME: (cdk.Stack.of(this) as MatanoStack).cdkAssetsBucketName,
      },
      code: getLocalAsset("iceberg_table_cfn"),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["glue:*", "s3:*"],
          resources: ["*"],
        }),
      ],
    });
    makeLambdaSnapstart(providerFunc);

    this.provider = new cr.Provider(this, "Default", {
      onEventHandler: providerFunc.currentVersion,
    });
  }
}

interface MatanoIcebergTableProps {
  tableName: string;
  schema: Record<string, any>;
  partitions?: any[];
}

export class MatanoIcebergTable extends Construct {
  constructor(scope: Construct, id: string, props: MatanoIcebergTableProps) {
    super(scope, id);

    const tableProperties = {
      "format-version": "2",
      "write.parquet.compression-codec": "zstd",
      "write.avro.compression-codec": "zstd",
      "write.metadata.delete-after-commit.enabled": "true",
      write_compression: "zstd",
    };

    const resource = new CustomResource(this, "Default", {
      serviceToken: IcebergTableProvider.getOrCreate(this, {}),
      resourceType: "Custom::MatanoIcebergTable",
      properties: {
        logSourceName: props.tableName,
        tableName: props.tableName,
        schema: props.schema,
        partitions: props.partitions,
        tableProperties,
      },
    });
  }
}

interface IcebergTableProviderProps {}

export class IcebergTableProvider extends Construct {
  provider: cr.Provider;

  public static getOrCreate(scope: Construct, props: IcebergTableProviderProps) {
    const stack = cdk.Stack.of(scope);
    const hash = ""; //md5(props.icebergS3BucketName); // TODO: or should it be in properties? think multi...
    const id = `MatanoCustomResourceIcebergTableProvider${hash}`;
    const x = (stack.node.tryFindChild(id) as IcebergTableProvider) || new IcebergTableProvider(stack, id, props);
    return x.provider.serviceToken;
  }

  constructor(scope: Construct, id: string, props: IcebergTableProviderProps) {
    super(scope, id);

    const providerFunc = new lambda.Function(this, "Function", {
      runtime: lambda.Runtime.JAVA_11,
      handler: "com.matano.iceberg.MatanoIcebergTableCustomResource::handleRequest",
      description: "[Matano] This function provides the Cloudformation custom resource for a Matano Iceberg table.",
      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      environment: {
        ASSETS_BUCKET_NAME: (cdk.Stack.of(this) as MatanoStack).cdkAssetsBucketName,
      },
      code: getLocalAsset("iceberg_table_cfn"),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["glue:*", "s3:*"],
          resources: ["*"],
        }),
      ],
    });
    makeLambdaSnapstart(providerFunc);

    this.provider = new cr.Provider(this, "Default", {
      onEventHandler: providerFunc.currentVersion,
    });
  }
}

interface IcebergMetadataProps {
  lakeStorageBucket: S3BucketWithNotifications;
}
export class IcebergMetadata extends Construct {
  alertsHelperFunction: lambda.Function;
  metadataWriterFunction: lambda.Function;
  constructor(scope: Construct, id: string, props: IcebergMetadataProps) {
    super(scope, id);

    const duplicatesTable = new ddb.Table(this, "DuplicatesTable", {
      partitionKey: { name: "sequencer", type: ddb.AttributeType.STRING },
      timeToLiveAttribute: "ttl",
      billingMode: ddb.BillingMode.PAY_PER_REQUEST,
    });

    this.metadataWriterFunction = new lambda.Function(this, "WriterFunction", {
      description: "[Matano] This function ingests written input files into an Iceberg table.",
      runtime: lambda.Runtime.JAVA_11,
      memorySize: 1024,
      handler: "com.matano.iceberg.IcebergMetadataHandler::handleRequest",
      timeout: cdk.Duration.minutes(3),
      environment: {
        DUPLICATES_DDB_TABLE_NAME: duplicatesTable.tableName,
        MATANO_ICEBERG_BUCKET: props.lakeStorageBucket.bucket.bucketName,
      },
      code: getLocalAsset("iceberg_metadata"),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["glue:*", "s3:*"],
          resources: ["*"],
        }),
      ],
      reservedConcurrentExecutions: 1,
    });

    duplicatesTable.grantReadWriteData(this.metadataWriterFunction);

    const eventSource = new SqsEventSource(props.lakeStorageBucket.queue, {});
    this.metadataWriterFunction.currentVersion.addEventSource(eventSource);

    this.alertsHelperFunction = new lambda.Function(this, "AlertsHelper", {
      description: "[Matano] JVM Iceberg helper for alerting.",
      runtime: lambda.Runtime.JAVA_11,
      memorySize: 1500,
      handler: "com.matano.iceberg.AlertsIcebergHelper::handleRequest",
      timeout: cdk.Duration.minutes(3),
      environment: {
        DUPLICATES_DDB_TABLE_NAME: duplicatesTable.tableName,
        MATANO_ICEBERG_BUCKET: props.lakeStorageBucket.bucket.bucketName,
      },
      code: getLocalAsset("iceberg_metadata"),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: ["glue:*", "s3:*"],
          resources: ["*"],
        }),
      ],
    });

    this.alertsHelperFunction.addAlias("current");

    duplicatesTable.grantReadWriteData(this.alertsHelperFunction);
    [this.metadataWriterFunction, this.alertsHelperFunction].map(makeLambdaSnapstart);
  }
}
