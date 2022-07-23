import * as fs from "fs";

export const getDirectories = (source: string) =>
  fs.readdirSync(source, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => dirent.name)

// cdk formatArn is confusing af
export function makeRoleArn(account: string, roleName: string) {
  return `arn:aws:iam::${account}:role/${roleName}`;
}
