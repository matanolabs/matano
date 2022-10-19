import * as fs from "fs";
import * as path from "path";
import * as YAML from "yaml";

export function readConfig(directory: string, filename: string): Record<string, any> {
  return YAML.parse(fs.readFileSync(path.join(directory, filename), "utf8"));
}

export function isMatanoDirectory(dirpath: string) {
  return fs.existsSync(dirpath) && fs.existsSync(path.join(dirpath, "matano.config.yml"));
}

export function promiseTimeout<T>(promiseFunc: () => Promise<T>, ms = 30000): Promise<T> {
  let id: any;
  let timeout = new Promise((_, reject) => {
    id = setTimeout(() => {
      reject("Timed out.");
    }, ms);
  });

  return Promise.race([promiseFunc(), timeout]).then((result) => {
    clearTimeout(id);
    return result;
  }) as Promise<T>;
}

export const AWS_REGIONS = [
  "af-south-1",
  "eu-north-1",
  "ap-south-1",
  "eu-west-3",
  "eu-west-2",
  "eu-south-1",
  "eu-west-1",
  "ap-northeast-3",
  "ap-northeast-2",
  "me-south-1",
  "ap-northeast-1",
  "me-central-1",
  "sa-east-1",
  "ca-central-1",
  "ap-east-1",
  "ap-southeast-1",
  "ap-southeast-2",
  "ap-southeast-3",
  "eu-central-1",
  "us-east-1",
  "us-east-2",
  "us-west-1",
  "us-west-2",
];
