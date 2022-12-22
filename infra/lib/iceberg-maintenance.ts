import { Construct, Node } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import { SfnStateMachine } from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import { getLocalAsset, makeLambdaSnapstart } from "./utils";

const helperFunctionCode = `
from datetime import datetime, timedelta
import dateutil.parser

def handler(event, context):
    if time := event.get("time"):
        dt = dateutil.parser.parse(time)
    else:
        dt = datetime.now()
    dt_hour_ago = dt - timedelta(hours=1)
    return {
      "current_hour_ts": dt.strftime("%Y-%m-%d %H:00:00"),
      "prev_hour_ts": dt_hour_ago.strftime("%Y-%m-%d %H:00:00"),
    }
`;

interface IcebergCompactionProps {
  tableNames: string[];
}

class IcebergCompaction extends Construct {
  constructor(scope: Construct, id: string, props: IcebergCompactionProps) {
    super(scope, id);

    const compactionHelperFunction = new lambda.Function(this, "Helper", {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: "index.handler",
      code: lambda.Code.fromInline(helperFunctionCode),
    });

    const getPartitionHour = new tasks.LambdaInvoke(this, "Get Partition Hour", {
      lambdaFunction: compactionHelperFunction,
      resultPath: "$.partition",
      payloadResponseOnly: true,
    });

    const inputPass = new sfn.Pass(this, "Add Table Names", {
      result: sfn.Result.fromArray(props.tableNames),
      resultPath: "$.table_names",
    });

    const queryMap = new sfn.Map(this, "Query Map", {
      itemsPath: sfn.JsonPath.stringAt("$.table_names"),
      parameters: {
        table_name: sfn.JsonPath.stringAt("$$.Map.Item.Value"),
        partition: sfn.JsonPath.objectAt("$.partition"),
      },
    });

    const optimizeQueryString =
      "OPTIMIZE matano.{} REWRITE DATA USING BIN_PACK WHERE timestamp \\'{}\\' > ts and ts >= timestamp \\'{}\\';";
    const optimizeQueryFormatStr = `States.Format('${optimizeQueryString}', $.table_name, $.partition.current_hour_ts, $.partition.prev_hour_ts)`;

    const compactionQuery = new tasks.AthenaStartQueryExecution(this, "Compaction Query", {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      queryString: sfn.JsonPath.stringAt(optimizeQueryFormatStr),
      workGroup: "matano_system",
    });

    queryMap.iterator(compactionQuery);

    const chain = inputPass.next(getPartitionHour).next(queryMap);

    const stateMachine = new sfn.StateMachine(this, "Default", {
      definition: chain,
    });

    stateMachine.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["glue:*"],
        resources: ["*"],
      })
    );

    const smScheduleRule = new events.Rule(this, "Rule", {
      schedule: events.Schedule.rate(cdk.Duration.hours(1)),
    });

    smScheduleRule.addTarget(
      new SfnStateMachine(stateMachine, {
        input: events.RuleTargetInput.fromObject({
          time: events.EventField.time,
        }),
      })
    );
  }
}

class IcebergExpireSnapshots extends Construct {
  constructor(scope: Construct, id: string, props: IcebergMaintenanceProps) {
    super(scope, id);

    const inputPass = new sfn.Pass(this, "Add Table Names", {
      result: sfn.Result.fromArray(props.tableNames),
      resultPath: "$.table_names",
    });

    const queryMap = new sfn.Map(this, "Expire Snapshots Map", {
      itemsPath: sfn.JsonPath.stringAt("$.table_names"),
      parameters: {
        time: sfn.JsonPath.stringAt("$.time"),
        table_name: sfn.JsonPath.stringAt("$$.Map.Item.Value"),
      },
      maxConcurrency: 25,
    });

    const expireSnapshotsFunc = new lambda.Function(this, "Function", {
      description: "[Matano] Expires Iceberg snapshots for an iceberg table.",
      runtime: lambda.Runtime.JAVA_11,
      memorySize: 650,
      timeout: cdk.Duration.minutes(14),
      handler: "com.matano.iceberg.ExpireSnapshotsHandler::handleRequest",
      code: getLocalAsset("iceberg_metadata"),
    });
    makeLambdaSnapstart(expireSnapshotsFunc);

    // TODO: scope down
    expireSnapshotsFunc.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["glue:*", "s3:*"],
        resources: ["*"],
      })
    );

    const expireSnapshotsInvoke = new tasks.LambdaInvoke(this, "Expire Table Snapshots", {
      lambdaFunction: expireSnapshotsFunc.currentVersion,
    });

    queryMap.iterator(expireSnapshotsInvoke);

    const chain = inputPass.next(queryMap);

    const stateMachine = new sfn.StateMachine(this, "Default", {
      definition: chain,
    });

    const smScheduleRule = new events.Rule(this, "Rule", {
      description: "[Matano] Schedules the Iceberg expire snapshots workflow.",
      schedule: events.Schedule.rate(cdk.Duration.days(1)),
    });

    smScheduleRule.addTarget(
      new SfnStateMachine(stateMachine, {
        input: events.RuleTargetInput.fromObject({
          time: events.EventField.time,
        }),
      })
    );
  }
}

class IcebergRewriteManifests extends Construct {
  constructor(scope: Construct, id: string, props: IcebergMaintenanceProps) {
    super(scope, id);

    const inputPass = new sfn.Pass(this, "Add Table Names", {
      result: sfn.Result.fromArray(props.tableNames),
      resultPath: "$.table_names",
    });

    const queryMap = new sfn.Map(this, "Rewrite Manifests Map", {
      itemsPath: sfn.JsonPath.stringAt("$.table_names"),
      parameters: {
        table_name: sfn.JsonPath.stringAt("$$.Map.Item.Value"),
      },
      maxConcurrency: 25,
    });

    const rewriteManifestsFunc = new lambda.Function(this, "Function", {
      description: "[Matano] Rewrites Iceberg manifests for an iceberg table.",
      runtime: lambda.Runtime.JAVA_11,
      memorySize: 1024,
      timeout: cdk.Duration.minutes(14),
      handler: "com.matano.iceberg.RewriteManifestsHandler::handleRequest",
      code: getLocalAsset("iceberg_metadata"),
    });
    makeLambdaSnapstart(rewriteManifestsFunc);

    // TODO: scope down
    rewriteManifestsFunc.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["glue:*", "s3:*"],
        resources: ["*"],
      })
    );

    const rewriteManifestsInvoke = new tasks.LambdaInvoke(this, "Rewrite Manifests", {
      lambdaFunction: rewriteManifestsFunc.currentVersion,
    });

    queryMap.iterator(rewriteManifestsInvoke);

    const chain = inputPass.next(queryMap);

    const stateMachine = new sfn.StateMachine(this, "Default", {
      definition: chain,
    });

    const smScheduleRule = new events.Rule(this, "Rule", {
      description: "[Matano] Schedules the Iceberg rewrite manifests workflow.",
      schedule: events.Schedule.rate(cdk.Duration.hours(1)),
    });

    smScheduleRule.addTarget(
      new SfnStateMachine(stateMachine, {
        input: events.RuleTargetInput.fromObject({}),
      })
    );
  }
}

interface IcebergMaintenanceProps {
  tableNames: string[];
}

export class IcebergMaintenance extends Construct {
  constructor(scope: Construct, id: string, props: IcebergMaintenanceProps) {
    super(scope, id);

    new IcebergCompaction(this, "Compaction", {
      ...props,
      tableNames: props.tableNames.filter((n) => !n.startsWith("enrich_")),
    });

    new IcebergExpireSnapshots(this, "ExpireSnapshots", {
      ...props,
    });

    new IcebergRewriteManifests(this, "RewriteManifests", {
      ...props,
    });
  }
}
