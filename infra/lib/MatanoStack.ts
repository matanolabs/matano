import * as fs from "fs";
import * as path from "path";
import * as YAML from 'yaml';
import { IConstruct, Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ec2 from "aws-cdk-lib/aws-ec2";

// from https://github.com/capralifecycle/liflig-cdk/blob/master/src/tags.ts
export function tagResources(
  scope: Construct,
  tags: (stack: cdk.Stack) => Record<string, string>,
): void {
  cdk.Aspects.of(scope).add({
    visit(construct: IConstruct) {
      if (cdk.TagManager.isTaggable(construct)) {
        // We pick the last stack in chain to support stages where
        // there are multiple stacks.
        const allStacks = construct.node.scopes.filter((it): it is cdk.Stack =>
          cdk.Stack.isStack(it),
        )

        const stack = allStacks.length > 0 ? allStacks[allStacks.length - 1] : undefined
        if (stack != null) {
          for (const [key, value] of Object.entries(tags(stack))) {
            construct.tags.setTag(key, value);
          }
        }
      }
    },
  })
}

export interface MatanoConfiguration {
}

export type MatanoConfig = MatanoConfiguration;
export class MatanoConfiguration {
  static of(scope: Construct) {
    const stack = (scope instanceof cdk.Stack ? scope : cdk.Stack.of(scope)) as MatanoStack;
    return stack.matanoConfig; 
  } 
}

export interface MatanoStackProps extends cdk.StackProps {}

export class MatanoStack extends cdk.Stack {
  matanoConfig: MatanoConfig;
  matanoVpc: ec2.IVpc
  cdkAssetsBucketName: string;
  cdkAssetsBucket: s3.IBucket;
  constructor(scope: Construct, id: string, props: MatanoStackProps) {
    super(scope, id, props);
    this.matanoConfig = YAML.parse(fs.readFileSync(path.resolve(this.matanoUserDirectory, "matano.config.yml"), "utf8"));
    this.matanoVpc = ec2.Vpc.fromVpcAttributes(this, "MATANO_VPC", this.matanoContext["vpc"]);
    this.cdkAssetsBucketName = cdk.Fn.sub(cdk.DefaultStackSynthesizer.DEFAULT_FILE_ASSETS_BUCKET_NAME, {
      Qualifier: (this.node.tryGetContext(cdk.BOOTSTRAP_QUALIFIER_CONTEXT) as any) ?? cdk.DefaultStackSynthesizer.DEFAULT_QUALIFIER,
    });
    this.cdkAssetsBucket = s3.Bucket.fromBucketName(this, "cdkAssetsBucket", this.cdkAssetsBucketName);
  }


  get matanoUserDirectory(): string {
    return this.node.tryGetContext("matanoUserDirectory");
  }

  get matanoAwsAccountId() {
    return this.node.tryGetContext("matanoAwsAccountId");
  }

  get matanoAwsRegion() {
    return this.node.tryGetContext("matanoAwsRegion");
  }

  get matanoContext() {
    return JSON.parse(this.node.tryGetContext("matanoContext"));
  }
}
