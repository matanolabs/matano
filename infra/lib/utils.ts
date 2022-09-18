import * as fs from "fs";
import * as os from "os";
import * as crypto from "crypto";
import * as path from "path";
import * as YAML from 'yaml';
import * as cdk from "aws-cdk-lib";
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

export const isPkg = () => !!((process as any).pkg);
export const getPkgRoot = () => path.dirname(process.execPath);

export const getLocalAssetsDir = () => {
  return isPkg() ? path.resolve(getPkgRoot(), "local-assets") : path.resolve(__dirname, "../../../local-assets");
}

export function getLocalAssetPath(assetName: string) {
  const ret = path.join(getLocalAssetsDir(), assetName);
  return isPkg() ? ret + ".zip" : ret;
}

export function getLocalAsset(assetName: string) {
  return lambda.AssetCode.fromAsset(getLocalAssetPath(assetName));
};

export const randStr = (n=20) => crypto.randomBytes(n).toString('hex');
export function makeTempDir(prefix?: string) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix ?? randStr()));
}

export const dataDirPath = path.join(__dirname, "../../../data");

export const md5Hash = (s: string) => crypto.createHash('md5').update(s).digest('hex');

export function mergeDeep(target: any, source: any) {
  let output = Object.assign({}, target);
  if (isObject(target) && isObject(source)) {
    Object.keys(source).forEach(key => {
      if (isObject(source[key])) {
        if (!(key in target))
          Object.assign(output, { [key]: source[key] });
        else
          output[key] = mergeDeep(target[key], source[key]);
      } else {
        Object.assign(output, { [key]: source[key] });
      }
    });
  }
  return output;
}

export function isObject(item: any): boolean {
  return (item && typeof item === 'object' && !Array.isArray(item));
}
