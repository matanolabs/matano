import { IConstruct } from "constructs";
import * as cdk from "aws-cdk-lib";

import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as ddb from "aws-cdk-lib/aws-dynamodb";

import { MatanoStack } from "../MatanoStack";

const statefulResourceTypes = [s3.CfnBucket, ddb.CfnTable, sqs.CfnQueue];

export class RetentionPolicyAspect implements cdk.IAspect {
  public visit(node: IConstruct): void {
    const isProd = () => (cdk.Stack.of(node) as MatanoStack).matanoConfig.is_production ?? true;

    let matchedResourceType = statefulResourceTypes.find((r) => node instanceof r);
    if (matchedResourceType) {
      // enforce deletion policy's based on is_production flag in Matano config for stateful resources
      (node as cdk.CfnResource).applyRemovalPolicy(isProd() ? cdk.RemovalPolicy.RETAIN : cdk.RemovalPolicy.DESTROY);
    }

    if (node instanceof s3.Bucket && !isProd()) {
      (node as any).enableAutoDeleteObjects();
    }
  }
}
