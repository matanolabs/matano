import { IConstruct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import { MatanoStack } from "../MatanoStack";

export class S3BlockPublicAccessAspect implements cdk.IAspect {
  public visit(node: IConstruct): void {
    const doSetBlockPublicAccess = (cdk.Stack.of(node) as MatanoStack).matanoConfig?.aws?.set_block_public_access !== false;

    if (node instanceof s3.CfnBucket) {
      if (node.publicAccessBlockConfiguration === undefined && doSetBlockPublicAccess) {
        node.publicAccessBlockConfiguration = {
          blockPublicAcls: true,
          blockPublicPolicy: true,
          ignorePublicAcls: true,
          restrictPublicBuckets: true,
        }
      }
    }
  }
}
