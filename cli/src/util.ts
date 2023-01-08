import * as fs from "fs-extra";
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

// TODO(shaeq): don't duplicate code between infra and cli?
const isLower = (c: string | undefined) => c && c == c.toLowerCase();
export function validateProjectLabel(projectLabel: string) {
  const KEBAB_CASE_REGEX = /^([a-z](?![\d])|[\d](?![a-z]))+(-?([a-z](?![\d])|[\d](?![a-z])))*$|^$/;

  if (projectLabel.includes("matano")) {
    throw new Error(
      `Project label contains a reserved keyword, try renaming to: ${projectLabel
        .split("-")
        .filter((p) => !p.includes("matano"))
        .join("-")}`
    );
  }
  if (
    !(
      projectLabel.match(KEBAB_CASE_REGEX) &&
      projectLabel.length >= 5 &&
      projectLabel.length <= 15 &&
      isLower(projectLabel)
    )
  ) {
    throw new Error(
      `Invalid project_label: ${projectLabel}, must be kebab-cased and only contain lowercase letters and numbers, and be 5-15 characters long (e.g. my-org-${Math.random()
        .toString()
        .slice(2, 5)})`
    );
  }
}

export const stackNameWithLabel = (name: string, projectLabel: string) => {
  if (projectLabel != null) validateProjectLabel(projectLabel);
  return `${name}${projectLabel ? `-${projectLabel}` : ""}`;
};

export const fileExists = (path: string) =>
  fs
    .stat(path)
    .then(() => true)
    .catch(() => false);
