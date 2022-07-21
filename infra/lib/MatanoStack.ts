import * as fs from "fs";
import * as path from "path";
import * as YAML from 'yaml';
import { IConstruct, Construct } from "constructs";
import * as cdk from "aws-cdk-lib";
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

export class MatanoBaseStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: cdk.StackProps) {
    super(scope, id, props);
  }
}

interface MatanoConfig {
  kafka_cluster_type: "msk" | "msk-serverless";
}

export interface MatanoStackProps extends cdk.StackProps {}

export class MatanoStack extends MatanoBaseStack {
  matanoConfig: MatanoConfig;
  constructor(scope: Construct, id: string, props: MatanoStackProps) {
    super(scope, id, props);
    this.matanoConfig = YAML.parse(fs.readFileSync(path.resolve(this.matanoUserDirectory, "matano.config.yml"), "utf8"));
  }

  get matanoUserDirectory(): string {
    return this.node.tryGetContext("matanoUserDirectory");
  }

  get matanoVpc(): ec2.IVpc {
    return ec2.Vpc.fromVpcAttributes(this, "MATANO_VPC", this.matanoContext["vpc"]);
  }

  get matanoAwsAccountId() {
    return this.node.tryGetContext("matanoAwsAccountId");
  }

  get matanoAwsRegion() {
    return this.node.tryGetContext("matanoAwsRegion");
  }

  get matanoContext() {
    return this.node.tryGetContext("matanoContext");
  }
}
