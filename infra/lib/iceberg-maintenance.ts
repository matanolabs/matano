import { Construct, Node } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import { SfnStateMachine } from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import { getLocalAsset, getStandardGlueResourceArns, makeLambdaSnapstart } from "./utils";

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
  lakeStorageBucket: s3.IBucket;
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
      workGroup: "matano_system_v3",
    });

    queryMap.iterator(compactionQuery);

    const chain = inputPass.next(getPartitionHour).next(queryMap);

    const stateMachine = new sfn.StateMachine(this, "Default", {
      definition: chain,
    });

    stateMachine.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["glue:Get*", "glue:UpdateDatabase", "glue:UpdateTable"],
        resources: [...getStandardGlueResourceArns(this), "*"],
      })
    );
    stateMachine.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["athena:StopQueryExecution"],
        resources: [
          `arn:${cdk.Stack.of(this).partition}:athena:*:${cdk.Stack.of(this).account}:workgroup/matano_system_v3`,
        ],
      })
    );
    props.lakeStorageBucket.grantReadWrite(stateMachine.role!);

    const smScheduleRule = new events.Rule(this, "Rule", {
      description: "[Matano] Schedules the Iceberg compaction workflow.",
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

interface IcebergAthenaExpireSnapshotsProps {
  tableNames: string[];
  lakeStorageBucket: s3.IBucket;
}

class IcebergAthenaExpireSnapshots extends Construct {
  constructor(scope: Construct, id: string, props: IcebergAthenaExpireSnapshotsProps) {
    super(scope, id);

    const inputPass = new sfn.Pass(this, "Add Table Names", {
      result: sfn.Result.fromArray(props.tableNames),
      resultPath: "$.table_names",
    });

    const queryMap = new sfn.Map(this, "Query Map", {
      itemsPath: sfn.JsonPath.stringAt("$.table_names"),
      parameters: {
        "query_statement.$": "States.Format('VACUUM matano.{};', $$.Map.Item.Value)",
      },
    });

    const vacuumQuery = new tasks.AthenaStartQueryExecution(this, "Vacuum Query", {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      queryString: sfn.JsonPath.stringAt("$.query_statement"),
      workGroup: "matano_system_v3",
    });

    const parseErrorJson = new sfn.Pass(this, "Parse Error JSON", {
      parameters: {
        "Cause.$": "States.StringToJson($.Cause)",
      },
    });

    const postQueryError = new sfn.Pass(this, "Post Query Error", {
      parameters: {
        "Cause.$": "$.Cause",
        "query_statement.$": "$.Cause.QueryExecution.Query",
      },
    });

    const markError = new sfn.Pass(this, "Mark Error", {
      result: sfn.Result.fromObject({
        error: true,
      }),
    });

    const completeRun = new sfn.Pass(this, "Complete Run", {});

    const checkError = new sfn.Choice(this, "Check if error or needs more vacuums");
    checkError.when(
      sfn.Condition.stringMatches(
        "$.Cause.QueryExecution.Status.StateChangeReason",
        "ICEBERG_VACUUM_MORE_RUNS_NEEDED*"
      ),
      vacuumQuery
    );
    checkError.otherwise(markError);
    markError.next(completeRun);

    const catchChain = parseErrorJson.next(postQueryError).next(checkError);

    vacuumQuery.addCatch(catchChain, {
      errors: ["States.TaskFailed"],
    });

    queryMap.iterator(vacuumQuery.next(completeRun));

    const collectErrors = new sfn.Pass(this, "Collect Errors", {
      parameters: {
        "errors.$": "$.[*].error",
      },
    });

    const checkIfErrored = new sfn.Pass(this, "Check If Errored", {
      parameters: {
        "error.$": "States.ArrayContains($.errors, true)",
      },
    });

    const failIfErrored = new sfn.Choice(this, "Fail if errored");

    const fail = new sfn.Fail(this, "Fail", {
      cause: "One or more queries failed.",
    });
    const succeed = new sfn.Succeed(this, "Succeed");
    failIfErrored.when(sfn.Condition.booleanEquals("$.error", true), fail);
    failIfErrored.otherwise(succeed);

    const chain = inputPass.next(queryMap).next(collectErrors).next(checkIfErrored).next(failIfErrored);

    const stateMachine = new sfn.StateMachine(this, "Default", {
      definition: chain,
      timeout: cdk.Duration.hours(1),
    });

    stateMachine.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["glue:Get*", "glue:UpdateDatabase", "glue:UpdateTable"],
        resources: [...getStandardGlueResourceArns(this), "*"],
      })
    );
    stateMachine.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["athena:StopQueryExecution"],
        resources: [
          `arn:${cdk.Stack.of(this).partition}:athena:*:${cdk.Stack.of(this).account}:workgroup/matano_system_v3`,
        ],
      })
    );
    props.lakeStorageBucket.grantDelete(stateMachine.role!);
    props.lakeStorageBucket.grantReadWrite(stateMachine.role!);

    const smScheduleRule = new events.Rule(this, "Rule", {
      description: "[Matano] Schedules the Iceberg expire snapshots workflow.",
      schedule: events.Schedule.rate(cdk.Duration.hours(1)),
    });

    smScheduleRule.addTarget(
      new SfnStateMachine(stateMachine, {
        input: events.RuleTargetInput.fromObject({}),
      })
    );
  }
}

// We use Athena vacuum command to expire snapshots for now.
// Retaining this since we might want a custom snapshot expiry in future.
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
      code: getLocalAsset("iceberg_main"),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: [
            "glue:GetDatabases",
            "glue:GetDatabase",
            "glue:UpdateDatabase",
            "glue:GetTable",
            "glue:GetTables",
            "glue:UpdateTable",
          ],
          resources: getStandardGlueResourceArns(this),
        }),
      ],
    });
    props.lakeStorageBucket.grantReadWrite(expireSnapshotsFunc);
    makeLambdaSnapstart(expireSnapshotsFunc);

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

// TODO: probably remove this, Athena OPTIMIZE actually rewrites manifests.
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
      code: getLocalAsset("iceberg_main"),
      initialPolicy: [
        new iam.PolicyStatement({
          actions: [
            "glue:GetDatabases",
            "glue:GetDatabase",
            "glue:UpdateDatabase",
            "glue:GetTable",
            "glue:GetTables",
            "glue:UpdateTable",
          ],
          resources: getStandardGlueResourceArns(this),
        }),
      ],
    });
    props.lakeStorageBucket.grantReadWrite(rewriteManifestsFunc);
    makeLambdaSnapstart(rewriteManifestsFunc);

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
  lakeStorageBucket: s3.IBucket;
}

export class IcebergMaintenance extends Construct {
  constructor(scope: Construct, id: string, props: IcebergMaintenanceProps) {
    super(scope, id);

    const tableNames = [...new Set(props.tableNames)];
    const nonTempTableNames = tableNames.filter((n) => !n.endsWith("_temp"));

    new IcebergCompaction(this, "Compaction", {
      tableNames: nonTempTableNames.filter((n) => !n.startsWith("enrich_")),
      lakeStorageBucket: props.lakeStorageBucket,
    });

    new IcebergAthenaExpireSnapshots(this, "ExpireSnapshotsAthena", {
      tableNames,
      lakeStorageBucket: props.lakeStorageBucket,
    });
  }
}
