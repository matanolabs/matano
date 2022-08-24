#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";

import { DPCommonStack } from "../src/DPCommonStack";
import { DPMainStack } from "../src/DPMainStack";
import { tagResources } from "../lib/MatanoStack";

const app = new cdk.App();

const env = {
  account: process.env.MATANO_CDK_ACCOUNT,
  region: process.env.MATANO_CDK_REGION,
};

const dpCommonStack = new DPCommonStack(app, "DPCommonStack", {
  stackName: "MatanoDPCommonStack",
  env,
});

const dpMainStack = new DPMainStack(app, "DPMainStack", {
  stackName: "MatanoDPMainStack",
  env,
  matanoSourcesBucket: dpCommonStack.matanoIngestionBucket,
  lakeStorageBucket: dpCommonStack.matanoLakeStorageBucket,
});

tagResources(app, () => ({
  "matano:managed": "true",
}));
