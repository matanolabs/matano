import * as fs from "fs";
import * as path from "path";
import * as YAML from 'yaml';

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
