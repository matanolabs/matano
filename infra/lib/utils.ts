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

// cdk formatArn is confusing af
export function makeRoleArn(account: string, roleName: string) {
  return `arn:aws:iam::${account}:role/${roleName}`;
}

export const getQueueUrl = (stack: MatanoStack, queueName: string) =>
  `https://sqs.${stack.region}.amazonaws.com/${stack.account}/${queueName}`;
