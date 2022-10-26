import { IConstruct } from "constructs";
import * as cdk from "aws-cdk-lib";

import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as ddb from "aws-cdk-lib/aws-dynamodb";

import { md5Hash, validateProjectLabel } from "../utils";

import { Token } from "aws-cdk-lib";
import { MatanoStack } from "../MatanoStack";

const removeSuffix = (s: string, suffix: string) => {
  if (s.endsWith(suffix)) {
    return s.slice(0, -suffix.length);
  }
  return s;
};

function isNumeric(str: string) {
  if (typeof str != "string") return false; // we only process strings!
  return (
    !isNaN(str as any) && // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
    !isNaN(parseFloat(str))
  ); // ...and ensure strings of whitespace fail
}

const isUpper = (c: string | undefined) => c && c == c.toUpperCase();
const isLower = (c: string | undefined) => c && c == c.toLowerCase();

function cdkPathPascalToKebabSmart(str: string) {
  str = str
    .split("/")
    .filter((s) => s !== "Default")
    .join("");

  let buff: string[] = [];
  let words: string[] = [];
  let prev: string | null = null;

  Array.from(str).forEach((c, i) => {
    if (prev != null && !isNumeric(c)) {
      if (isLower(prev) && isUpper(c)) {
        words.push(buff.join(""));
        buff = [];
      } else if (i > 1 && isLower(c) && isUpper(str[i - 2]) && isUpper(str[i - 1])) {
        let startChar = buff.pop()!;
        words.push(buff.join(""));
        buff = [startChar];
      }
    }

    buff.push(c);
    prev = c;
  });

  if (buff.length) {
    words.push(buff.join(""));
    buff = [];
  }

  let name = words
    .map((w) => w.replace(/^[^a-z\d]*|[^a-z\d]*$/gi, "").toLowerCase())
    .filter((w) => !!w)
    .join("-");

  name = removeSuffix(name, "-resource");
  name = name.replace(/dp-.*-stack-/, "").replace("bucket-bucket", "bucket");

  if (!name.startsWith("matano-")) {
    name = "matano-" + name;
  }

  return name;
}

const enabledResources = [
  lambda.CfnFunction,
  ddb.CfnTable,
  sqs.CfnQueue,
  sns.CfnTopic,
  s3.CfnBucket, // unique
];

const defaultNameField = "name";
const maxResourceNameLengths = {
  "AWS::Lambda::Function": 64,
  "AWS::DynamoDB::Table": 255,
  "AWS::SQS::Queue": 80,
  "AWS::SNS::Topic": 256,
  "AWS::S3::Bucket": 63,
} as Record<string, number>;

export class FriendlyNamingAspect implements cdk.IAspect {
  public visit(node: IConstruct): void {
    let matchedResourceType = enabledResources.find((r) => node instanceof r);
    if (matchedResourceType) {
      const resourceTypeFullName = matchedResourceType?.CFN_RESOURCE_TYPE_NAME!;
      const resourceTypeName = matchedResourceType?.CFN_RESOURCE_TYPE_NAME!.split("::").pop()!;
      const nameField = `${resourceTypeName.charAt(0).toLowerCase() + resourceTypeName.slice(1)}Name`;
      const privateNameField = `_${nameField}`; //some names (eg. stackName, exportName) only has getter, need to access protected fields starting with underscore

      // rename
      const nodeRef = node as any;
      const isMatch = (fieldName: string) =>
        nodeRef[fieldName] != null && isWritable(nodeRef, fieldName) && isTarget(nodeRef[fieldName]);
      const targetField = [nameField, privateNameField, defaultNameField].find(isMatch);
      if (targetField == null) {
        throw new Error(`Couldn't find writable name field for ${node.node.path} (${resourceTypeFullName})`);
      }
      nodeRef[targetField] = rename(node, nodeRef[targetField], resourceTypeFullName);
    }
  }
}

function rename(node: IConstruct, resourceName: string, resourceTypeFullName: string): string {
  const projectLabel = (cdk.Stack.of(node) as MatanoStack).matanoConfig.project_label;

  if (!projectLabel) {
    // console.log(resourceName);
    return resourceName;
  }

  validateProjectLabel(projectLabel);

  const accountId = node.node.tryGetContext("matanoAwsAccountId");
  const regionName = node.node.tryGetContext("matanoAwsRegion");
  if (accountId == null || regionName == null) {
    throw new Error("matanoAwsAccountId and/or matanoAwsRegion missing in context.");
  }

  const normalizedResourceName = cdkPathPascalToKebabSmart(node.node.path);
  if (!normalizedResourceName.startsWith("matano-")) {
    throw new Error(
      `Resource resolved to an improper name: ${normalizedResourceName}, construct path: ${node.node.path} should begin with <Matano...>`
    );
  }

  const uniqueNecessary = ["AWS::S3::Bucket"].includes(resourceTypeFullName);
  const uniqueSuffix = uniqueNecessary
    ? [`${md5Hash(`${accountId}#${regionName}#${node.node.addr}`).slice(0, 6)}`]
    : [];
  const uniqueSuffixStr = uniqueSuffix.join("-");

  const joined = (...parts: string[]) => parts.filter((p) => !!p).join("-");

  const renamedName = projectLabel
    ? joined(`matano`, projectLabel, normalizedResourceName.slice("matano-".length), uniqueSuffixStr)
    : joined(normalizedResourceName, uniqueSuffixStr, projectLabel);

  let renamedResourceName = `${renamedName}`.toLowerCase();

  const maxLength = maxResourceNameLengths[resourceTypeFullName as any]!!;
  if (renamedResourceName.length > maxLength) {
    const extraPart = renamedResourceName.slice(maxLength);
    renamedResourceName = `${removeSuffix(renamedResourceName.slice(0, maxLength - 7), "-")}-${md5Hash(extraPart).slice(
      0,
      6
    )}`;
  }

  // should be other aspect, but enforce deletion policy's based on production flag for stateful resources
  const isProd = (cdk.Stack.of(node) as MatanoStack).matanoConfig.is_production ?? false;
  const statefulResourceTypes = ["AWS::S3::Bucket", "AWS::DynamoDB::Table", "AWS::SQS::Queue"];
  if (statefulResourceTypes.includes(resourceTypeFullName)) {
    (node as cdk.CfnResource).applyRemovalPolicy(isProd ? cdk.RemovalPolicy.RETAIN : cdk.RemovalPolicy.DESTROY);
  }

  // console.log(`Renaming ${resourceTypeFullName} ${resourceName} to ${renamedResourceName}`); // from path ${node.node.path}`);
  return renamedResourceName;
}

function isTarget(resName: any): boolean {
  return Token.isUnresolved(resName); // only rename auto-generated names
}

function isWritable<T extends Object>(obj: T, key: keyof T): boolean {
  let desc: PropertyDescriptor | undefined;
  for (let o = obj; o != Object.prototype; o = Object.getPrototypeOf(o)) {
    desc = Object.getOwnPropertyDescriptor(o, key);
    if (desc !== undefined) {
      break;
    }
  }
  if (desc === undefined) {
    desc = {};
  }
  return Boolean(desc.writable);
}
