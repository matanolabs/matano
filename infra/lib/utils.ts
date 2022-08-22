import * as fs from "fs";
import * as path from "path";
import * as YAML from 'yaml';
import * as lambda from "aws-cdk-lib/aws-lambda";

export const getDirectories = (source: string) =>
  fs.readdirSync(source, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => dirent.name)

// cdk formatArn is confusing af
export function makeRoleArn(account: string, roleName: string) {
  return `arn:aws:iam::${account}:role/${roleName}`;
}

export function readConfig(directory: string, filename: string): Record<string, any> {
  return YAML.parse(fs.readFileSync(path.join(directory, filename), "utf8"));
}

export function readDetectionConfig(detectionDirectory: string): Record<string, any> {
  return readConfig(detectionDirectory, "detection.yml");
}

export const MATANO_USED_RUNTIMES = [
  lambda.Runtime.JAVA_11,
  lambda.Runtime.PROVIDED,
  lambda.Runtime.PROVIDED_AL2,
  lambda.Runtime.PYTHON_3_9,
  lambda.Runtime.NODEJS_16_X,
]
