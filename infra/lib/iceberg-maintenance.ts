import { Construct, Node } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import { SfnStateMachine } from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";

const helperFunctionCode = `
from datetime import datetime, timedelta
def handler(event, context):
    if time := event.get("time"):
        dt = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        dt = datetime.now()
    dt_hour_ago = dt - timedelta(hours=1)
    ret = dt_hour_ago.strftime("%Y-%m-%d-%H")
    return f"'{ret}'"
`;

interface IcebergCompactionProps {
  tableNames: string[];
}

class IcebergCompaction extends Construct {
  constructor(scope: Construct, id: string, props: IcebergCompactionProps) {
    super(scope, id);

    const compactionHelperFunction = new lambda.Function(this, "IcebergCompactionHelper", {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: "index.handler",
      code: lambda.Code.fromInline(helperFunctionCode),
    });

    const getPartitionHour = new tasks.LambdaInvoke(this, "Get Partition Hour", {
      lambdaFunction: compactionHelperFunction,
      resultPath: "$.partition_hour",
      resultSelector: {
        value: sfn.JsonPath.stringAt("$.Payload"),
      },
    });

    const inputPass = new sfn.Pass(this, "Add Table Names", {
      result: sfn.Result.fromArray(props.tableNames),
      resultPath: "$.table_names",
    });

    const queryMap = new sfn.Map(this, "Query Map", {
      itemsPath: sfn.JsonPath.stringAt("$.table_names"),
      parameters: {
        table_name: sfn.JsonPath.stringAt("$$.Map.Item.Value"),
        partition_hour: sfn.JsonPath.stringAt("$.partition_hour"),
      },
    });

    const optimizeQueryString = "OPTIMIZE matano.{} REWRITE DATA USING BIN_PACK WHERE partition_hour={};";
    const optimizeQueryFormatStr = `States.Format('${optimizeQueryString}', $.table_name, $.partition_hour.value)`;

    const compactionQuery = new tasks.AthenaStartQueryExecution(this, "Compaction Query", {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      queryString: sfn.JsonPath.stringAt(optimizeQueryFormatStr),
      workGroup: "primary",
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

    const smScheduleRule = new events.Rule(this, "MatanoIcebergCompactionRule", {
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

interface IcebergMaintenanceProps {
  tableNames: string[];
}

export class IcebergMaintenance extends Construct {
  constructor(scope: Construct, id: string, props: IcebergMaintenanceProps) {
    super(scope, id);

    new IcebergCompaction(this, "Compaction", {
      ...props,
    });
  }
}
