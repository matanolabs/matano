#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";

import { DPCommonStack } from "../src/DPCommonStack";
import { DPMainStack } from "../src/DPMainStack";
import { tagResources } from "../lib/MatanoStack";
import { stackNameWithLabel, MATANO_MANAGED_AWS_TAG_KEY } from "../lib/utils";

const app = new cdk.App();

const env = {
  account: process.env.MATANO_CDK_ACCOUNT,
  region: process.env.MATANO_CDK_REGION,
};

const dpCommonStack = new DPCommonStack(app, "DPCommonStack", {
  stackName: stackNameWithLabel("MatanoDPCommonStack"),
  env,
});

const dpMainStack = new DPMainStack(app, "DPMainStack", {
  stackName: stackNameWithLabel("MatanoDPMainStack"),
  env,
  matanoSourcesBucket: dpCommonStack.matanoIngestionBucket,
  lakeStorageBucket: dpCommonStack.matanoLakeStorageBucket,
  realtimeBucket: dpCommonStack.realtimeBucket,
  realtimeBucketTopic: dpCommonStack.realtimeBucketTopic,
  matanoAthenaResultsBucket: dpCommonStack.matanoAthenaResultsBucket,
  transformerSidelineBucket: dpCommonStack.transformerSidelineBucket,
  integrationsStore: dpCommonStack.integrationsStore,
  alertTrackerTable: dpCommonStack.alertTrackerTable,
});

let userAwsTags = dpMainStack.userAwsTags ?? {};
if (userAwsTags.constructor !== Object || !Object.values(userAwsTags).every((x) => typeof x === "string")) {
  throw new Error("Custom AWS tags must be key value object of strings.");
}

userAwsTags = Object.fromEntries(Object.entries(userAwsTags).map(([k, v]) => [k, v.toString()]));

tagResources(app, () => ({
  ...userAwsTags,
  [MATANO_MANAGED_AWS_TAG_KEY]: "true",
}));
