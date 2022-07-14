import { IConstruct, Construct } from "constructs";
import * as cdk from "aws-cdk-lib";

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

export interface MatanoStackProps extends cdk.StackProps {}

export class MatanoStack extends MatanoBaseStack {
  constructor(scope: Construct, id: string, props: MatanoStackProps) {
    super(scope, id, props);
  }

  get matanoUserDirectory(): string {
    return this.node.tryGetContext("matanoUserDirectory");
  }
}
