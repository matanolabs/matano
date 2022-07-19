#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";

import { DPCommonStack } from "../src/DPCommonStack";
import { DPStorageStack } from "../src/DPStorageStack";
import { IcebergMetadataStack } from "../src/IcebergMetadataStack";
import { DetectionsStack } from "../src/DetectionsStack";
import { tagResources } from "../lib/MatanoStack";

const app = new cdk.App();

const env = {
  account: process.env.MATANO_CDK_ACCOUNT,
  region: process.env.MATANO_CDK_REGION,
};

const dpStorageStack = new DPStorageStack(app, "DPStorageStack", {
  stackName: "MatanoDPStorageStack",
  env,
});

new IcebergMetadataStack(app, "IcebergMetadataStack", {
  stackName: "MatanoIcebergMetadataStack",
  env,
  outputBucket: dpStorageStack.outputEventsBucketWithNotifications,
});

new DPCommonStack(app, "DPCommonStack", {
  stackName: "MatanoDPCommonStack",
  env,
  rawEventsBucketWithNotifications: dpStorageStack.rawEventsBucketWithNotifications,
  outputEventsBucketWithNotifications: dpStorageStack.outputEventsBucketWithNotifications,
});

new DetectionsStack(app, "DetectionsStack", {
  stackName: "MatanoDetectionsStack",
  env,
  rawEventsBucket: dpStorageStack.rawEventsBucketWithNotifications,
});


tagResources(app, () => ({
  "matano:managed": "true",
}));
