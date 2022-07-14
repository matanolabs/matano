import { Construct } from "constructs";
import { MatanoStack } from "./MatanoStack";
import * as fs from "fs";
import * as path from "path";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";

export const getDirectories = (source: string) =>
  fs.readdirSync(source, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => dirent.name)

export const getCdkRoot = (startPath = __dirname): any => {
  const isRoot = fs.existsSync(path.join(startPath, "cdk.json"));

  if (isRoot) {
    return startPath;
  }
  //avoid loop if reached root path
  else if (startPath === path.parse(startPath).root) {
    throw new Error("FAIL getCdkRoot");
  }

  return getCdkRoot(path.dirname(startPath));
};

export const getPackageDir = (pkg: string) => path.join(getCdkRoot(), "..", pkg);

export function fixCrossAccountDelegation(stack: cdk.Stack, roleArns: any[]) {
  // https://github.com/aws/aws-cdk/issues/17836
  const cfnProviderRole = stack.node
    .findChild("Custom::CrossAccountZoneDelegationCustomResourceProvider")
    .node.findChild("Role") as iam.CfnRole;
  cfnProviderRole.addPropertyOverride("Policies.1", {
    PolicyName: "FixCrossAccountAdditionalDelegationPolicy",
    PolicyDocument: {
      Version: "2012-10-17",
      Statement: [
        {
          Effect: "Allow",
          Action: "sts:AssumeRole",
          Resource: roleArns,
        },
      ],
    },
  });
}

export function getDomainNameForStage(baseDomain: string, stage: string) {
  return stage === "prod" ? baseDomain : stage + "." + baseDomain;
}

// cdk formatArn is confusing af
export function makeRoleArn(account: string, roleName: string) {
  return `arn:aws:iam::${account}:role/${roleName}`;
}

export const stackSuffix = (scope: Construct) => {
  const stack = (scope instanceof cdk.Stack ? scope : cdk.Stack.of(scope)) as MatanoStack;
  return (strings: TemplateStringsArray, ...args: string[]) => {
    const joined = args.reduce((soFar, s, i) => soFar + s + strings[i + 1], strings[0]);
    return [joined, "", stack.region].join("-");
  };
};

export const getQueueUrl = (stack: MatanoStack, queueName: string) =>
  `https://sqs.${stack.region}.amazonaws.com/${stack.account}/${queueName}`;
