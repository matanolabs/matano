import * as fs from "fs";
import * as os from "os";
import * as crypto from "crypto";
import * as path from "path";
import * as YAML from "yaml";
import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";

export const getDirectories = (source: string) =>
  fs
    .readdirSync(source, { withFileTypes: true })
    .filter((dirent) => dirent.isDirectory())
    .map((dirent) => dirent.name);

export function walkdirSync(dir: string): string[] {
  return fs.readdirSync(dir).reduce(function (result: string[], file) {
    const filePath = path.join(dir, file);
    const isDir = fs.statSync(filePath).isDirectory();
    return result.concat(isDir ? walkdirSync(filePath) : [filePath]);
  }, []);
}

// cdk formatArn is confusing af
export function makeRoleArn(account: string, roleName: string) {
  return `arn:aws:iam::${account}:role/${roleName}`;
}

export function readConfig(directory: string, filename: string): Record<string, any> {
  return YAML.parse(fs.readFileSync(path.join(directory, filename), "utf8"));
}

export function readConfigPath(filepath: string): Record<string, any> {
  return YAML.parse(fs.readFileSync(path.join(filepath), "utf8"));
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
];

export const isPkg = () => !!(process as any).pkg;
export const getPkgRoot = () => path.dirname(process.execPath);

export const getLocalAssetsDir = () => {
  return isPkg() ? path.resolve(getPkgRoot(), "local-assets") : path.resolve(__dirname, "../../../local-assets");
};

export function getLocalAssetPath(assetName: string) {
  const ret = path.join(getLocalAssetsDir(), assetName);
  return isPkg() ? ret + ".zip" : ret;
}

export function getLocalAsset(assetName: string) {
  return lambda.AssetCode.fromAsset(getLocalAssetPath(assetName));
}

export const randStr = (n = 20) => crypto.randomBytes(n).toString("hex");
export function makeTempDir(prefix?: string) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix ?? randStr()));
}

export const dataDirPath = path.join(__dirname, "../../../data");

export const md5Hash = (s: string) => crypto.createHash("md5").update(s).digest("hex");

export function mergeDeep(target: any, source: any) {
  let output = Object.assign({}, target);
  if (isObject(target) && isObject(source)) {
    Object.keys(source).forEach((key) => {
      if (isObject(source[key])) {
        if (!(key in target)) Object.assign(output, { [key]: source[key] });
        else output[key] = mergeDeep(target[key], source[key]);
      } else {
        Object.assign(output, { [key]: source[key] });
      }
    });
  }
  return output;
}

export function isObject(item: any): boolean {
  return item && typeof item === "object" && !Array.isArray(item);
}

const ERROR_PREFIX = "MATANO_ERROR: ";
export function fail(e: string): never {
  const msg = ERROR_PREFIX + e;
  console.error(msg);
  process.exit(1);
}

export function commonPathPrefix(paths: string[], sep = "/") {
  const [first = "", ...remaining] = paths;
  if (first === "" || remaining.length === 0) return "";

  const parts = first.split(sep);

  let endOfPrefix = parts.length;
  for (const path of remaining) {
    const compare = path.split(sep);
    for (let i = 0; i < endOfPrefix; i++) {
      if (compare[i] !== parts[i]) {
        endOfPrefix = i;
      }
    }

    if (endOfPrefix === 0) return "";
  }

  const prefix = parts.slice(0, endOfPrefix).join(sep);
  return prefix;
}

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

export const stackNameWithLabel = (name: string) => {
  const projectLabel = process.env.MATANO_PROJECT_LABEL;
  if (projectLabel != null) validateProjectLabel(projectLabel);
  return `${name}${projectLabel ? `-${projectLabel}` : ""}`;
};

export function validateMatanoResourceName(name: string) {
  const SNAKE_CASE_REGEX = /^([a-z](?![\d])|[\d](?![a-z]))+(_?([a-z](?![\d])|[\d](?![a-z])))*$|^$/;

  if (!(name.match(SNAKE_CASE_REGEX) && name.length >= 5 && name.length <= 64 && isLower(name))) {
    throw new Error(
      `Invalid resource name: ${name}, must be snake_cased and only contain lowercase letters and numbers, and be 5-64 characters long.`
    );
  }
}

export function matanoResourceToCdkName(name: string) {
  return name
    .split("_")
    .map((substr) => substr.charAt(0).toUpperCase() + substr.slice(1))
    .join("");
}
