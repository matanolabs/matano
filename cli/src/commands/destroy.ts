import { CliUx, Command, Flags } from "@oclif/core";
import ora from "ora";
import BaseCommand, { BaseCLIError } from "../base";
import chalk from "chalk";
import { SdkProvider, SdkForEnvironment } from "aws-cdk/lib/api/aws-auth/sdk-provider";
import { Mode } from "aws-cdk/lib/api/plugin/credential-provider-source";
import * as cxapi from "@aws-cdk/cx-api";
import { CloudFormation } from "aws-sdk";
import Table from "tty-table";
import { promiseTimeout, readConfig, stackNameWithLabel } from "../util";
import { waitForStackDelete } from "aws-cdk/lib/api/util/cloudformation";
import * as AWS from "aws-sdk";

const getAllSubsets = (theArray: string[]) =>
  theArray.reduce((subsets, value) => subsets.concat(subsets.map((set) => [value, ...set])), [[]] as string[][]);

const emptyBucket = async (
  Bucket: string,
  NextContinuationToken: string | undefined,
  s3: AWS.S3,
  list: string[] = []
): Promise<void | string[]> => {
  if (NextContinuationToken || list.length === 0) {
    return await s3
      .listObjectsV2({ Bucket, ContinuationToken: NextContinuationToken })
      .promise()
      .then(async ({ Contents, NextContinuationToken }) => {
        if (Contents?.length) {
          await s3
            .deleteObjects({
              Bucket,
              Delete: {
                Objects: Contents.map((item) => ({ Key: item.Key!! })),
              },
            })
            .promise();
          if (NextContinuationToken) {
            // pass
          }
          return await emptyBucket(Bucket, NextContinuationToken, s3, [...list, ...Contents.map((item) => item.Key!!)]);
        }
        return list;
      });
  }
  return list;
};

const deleteVersionMarkers = async (
  Bucket: string,
  NextKeyMarker: string | undefined,
  s3: AWS.S3,
  list: string[] = []
): Promise<void | string[]> => {
  if (NextKeyMarker || list.length === 0) {
    return await s3
      .listObjectVersions({ Bucket, KeyMarker: NextKeyMarker })
      .promise()
      .then(async ({ DeleteMarkers, Versions, NextKeyMarker }) => {
        if (DeleteMarkers?.length) {
          await s3
            .deleteObjects({
              Bucket,
              Delete: {
                Objects: DeleteMarkers.map((item) => ({
                  Key: item.Key!!,
                  VersionId: item.VersionId,
                })),
              },
            })
            .promise();
          if (NextKeyMarker) {
            // pass
          }
          return await deleteVersionMarkers(Bucket, NextKeyMarker, s3, [
            ...list,
            ...DeleteMarkers.map((item) => item.Key!!),
          ]);
        }
        if (Versions?.length) {
          await s3
            .deleteObjects({
              Bucket,
              Delete: {
                Objects: Versions.map((item) => ({
                  Key: item.Key!!,
                  VersionId: item.VersionId,
                })),
              },
            })
            .promise();
          if (NextKeyMarker) {
            // pass
          }
          return await deleteVersionMarkers(Bucket, NextKeyMarker, s3, [
            ...list,
            ...Versions.map((item) => item.Key!!),
          ]);
        }
        return list;
      });
  }
  return list;
};

export default class Destroy extends BaseCommand {
  static description = "Deletes and cleans up all resources that are a part of your Matano deployment.";

  static examples = ["matano destroy", "matano destroy --profile prod", "matano destroy --output json"];

  static flags = {
    ...BaseCommand.flags,
    profile: Flags.string({
      char: "p",
      description: "AWS Profile to use for credentials.",
    }),
    "user-directory": Flags.string({
      required: false,
      description: "Matano user directory to use.",
    }),
    output: CliUx.ux.table.flags().output,
  };

  private static async deleteIfExists(cfn: CloudFormation, stackName: string) {
    // check if exists first, or else no-op
    let describeResult;
    // TODO: there's probably a better way to do this, shouldn't hang on invalid creds...
    try {
      describeResult = await promiseTimeout(() => cfn.describeStacks({ StackName: stackName }).promise());
    } catch (error: any) {
      if (error === "Timed out.") {
        throw new BaseCLIError("Failed to retrieve values. Your AWS credentials are likely misconfigured.");
      } else if (error.code === "ValidationError" && error.message === `Stack with id ${stackName} does not exist`) {
        console.log(`Stack ${stackName} does not exist, skipping...`);
        return;
      } else {
        throw error;
      }
    }

    const resourcesToRetainOnFailed = ["MatanoDefault", "MatanoSystem", "MatanoSystemV3"];
    const subsetsToTry = getAllSubsets(resourcesToRetainOnFailed).slice(1);

    const spinner = ora(`Deleting stack ${stackName}...`).start();
    const stack = describeResult.Stacks?.[0];

    if (stack?.StackStatus === "DELETE_FAILED") {
      for (const possibleFailedResources of subsetsToTry) {
        try {
          await cfn.deleteStack({ StackName: stackName, RetainResources: possibleFailedResources }).promise();
          break;
        } catch (error: any) {
          if (error.message.includes("resources to retain must be in a valid state")) {
            continue;
          } else {
            throw error;
          }
        }
      }
    } else {
      await cfn.deleteStack({ StackName: stackName }).promise();
    }

    let deleteFailed = false;
    let deletedStack;
    let failedResources = [];
    try {
      deletedStack = await waitForStackDelete(cfn, stackName);
    } catch (error: any) {
      if (error.message.includes("DELETE_FAILED")) {
        deleteFailed = true;
        const failedResourceListRegex = /\[(.*?)\]/;
        failedResources = error.message.match(failedResourceListRegex)[1].replace(" ", "").split(",");
      } else {
        throw error;
      }
    }
    if ((deletedStack && deletedStack.stackStatus.name !== "DELETE_COMPLETE") || deleteFailed) {
      if (deletedStack?.stackStatus.name === "DELETE_FAILED" || deleteFailed) {
        // try to delete again, but retain certain resources
        await cfn.deleteStack({ StackName: stackName, RetainResources: failedResources }).promise();
        const deletedStack2 = await waitForStackDelete(cfn, stackName);
        if (deletedStack2 && deletedStack2.stackStatus.name !== "DELETE_COMPLETE") {
          spinner.fail();
          throw new Error(
            `Failed deleting stack ${stackName} that had previously failed creation (current state: ${deletedStack?.stackStatus})`
          );
        }
      } else {
        spinner.fail();
        throw new Error(
          `Failed deleting stack ${stackName} (current state: ${deletedStack?.stackStatus})`
        );
      }
    }
    spinner.succeed();
  }

  static async destroyStacksAndResources(
    matanoUserDirectory: string,
    awsAccountId: string,
    awsRegion: string,
    awsProfile?: string
  ) {
    const sdkProvider = await SdkProvider.withAwsCliCompatibleDefaults({ profile: awsProfile });
    const cfn = (
      await sdkProvider.forEnvironment(cxapi.EnvironmentUtils.make(awsAccountId, awsRegion), Mode.ForReading, {})
    ).sdk.cloudFormation();

    const matanoConfig = readConfig(matanoUserDirectory, "matano.config.yml");
    const projectLabel = matanoConfig.project_label;

    const mainStackName = stackNameWithLabel("MatanoDPMainStack", projectLabel);
    const commonStackName = stackNameWithLabel("MatanoDPCommonStack", projectLabel);

    // prmpt a soft confirm that user can press enter to continue, this is not sensitive.
    const confirmed = await CliUx.ux.confirm(
      `Are you sure you want to delete the Matano stacks for account ${awsAccountId} and region ${awsRegion}? (y/n)`
    );
    if (!confirmed) {
        return;
    }

    await this.deleteIfExists(cfn, mainStackName);
    await this.deleteIfExists(cfn, commonStackName);

    // cleanup s3, sqs, ddb, and athena workgroup resources that can be left behind due to retention policies

    // First ask user to confirm deletion of all resources by typing in "delete <region> <account id>"
    let confirmationParts: string[] = [];
    const isPassing = () =>
      confirmationParts.length === 3 &&
      confirmationParts[0] === "delete" &&
      confirmationParts[1] === awsRegion &&
      confirmationParts[2] === awsAccountId;
    while (!isPassing()) {
      confirmationParts = (
        await CliUx.ux.prompt(
          `\nType "${chalk.bold(
            `delete ${awsRegion} ${awsAccountId}`
          )}" to force deletion of all remaining stateful matano resources in this account region that may have been retained from this deployment / previous deployments.\n\n${chalk.yellow(
            "WARNING:"
          )} Do not use this command in production or if you would like to keep data from any previous deployments to avoid unintentional data loss.\n`
        )
      ).split(" ");
      if (!isPassing()) {
        console.log(chalk.red("Confirmation failed, try again..."));
      }
    }

    console.log(`⚙️  Cleaning up all matano resources...`);

    // find all s3 buckets with the tag "matano:managed" and delete them
    const s3 = (
      await sdkProvider.forEnvironment(cxapi.EnvironmentUtils.make(awsAccountId, awsRegion), Mode.ForReading, {})
    ).sdk.s3();
    const s3Buckets = await s3.listBuckets().promise();
    const s3BucketNames = s3Buckets.Buckets?.map((bucket) => bucket.Name!!) ?? [];
    for (const bucketName of s3BucketNames) {
      if (!bucketName) continue;
      const bucketLocation =
        (await (await s3.getBucketLocation({ Bucket: bucketName }).promise()).LocationConstraint) || "us-east-1";
      if (bucketLocation !== awsRegion) continue;
      let bucketTags;
      try {
        bucketTags = await s3.getBucketTagging({ Bucket: bucketName }).promise();
      } catch (error: any) {
        if (error.code === "NoSuchTagSet") {
          continue;
        } else {
          throw error;
        }
      }
      if (bucketTags.TagSet?.some((tag) => tag.Key === "matano:managed" && tag.Value === "true")) {
        const spinner = ora(`Deleting S3 bucket ${bucketName}`).start();
        await emptyBucket(bucketName, undefined, s3);
        await deleteVersionMarkers(bucketName, undefined, s3);
        await s3.deleteBucket({ Bucket: bucketName }).promise();
        spinner.succeed();
      }
    }

    // sdk class is missing utility methods for sqs/athena, so we have to type it as any and construct manually
    const sdk = (
      await sdkProvider.forEnvironment(cxapi.EnvironmentUtils.make(awsAccountId, awsRegion), Mode.ForReading, {})
    ).sdk as any;

    const sqs: AWS.SQS = sdk.wrapServiceErrorHandling(new AWS.SQS(sdk.config));
    const sqsQueues = await sqs.listQueues().promise();
    const sqsQueueUrls = sqsQueues.QueueUrls ?? [];
    for (const queueUrl of sqsQueueUrls) {
      const queueTags = (await (await sqs.listQueueTags({ QueueUrl: queueUrl }).promise()).Tags) ?? {};
      if (queueTags["matano:managed"] === "true") {
        const spinner = ora(`Deleting SQS queue ${queueUrl}`).start();
        await sqs.deleteQueue({ QueueUrl: queueUrl }).promise();
        spinner.succeed();
      }
    }

    const ddb: AWS.DynamoDB = sdk.wrapServiceErrorHandling(new AWS.DynamoDB(sdk.config));
    const ddbTables = await ddb.listTables().promise();
    const ddbTableNames = ddbTables.TableNames ?? [];
    for (const tableName of ddbTableNames) {
      const tableTags =
        (await (
          await ddb
            .listTagsOfResource({ ResourceArn: `arn:aws:dynamodb:${awsRegion}:${awsAccountId}:table/${tableName}` })
            .promise()
        ).Tags) ?? [];
      if (tableTags.some((tag) => tag.Key === "matano:managed" && tag.Value === "true")) {
        const spinner = ora(`Deleting DynamoDB table ${tableName}`).start();
        await ddb.deleteTable({ TableName: tableName }).promise();
        spinner.succeed();
      }
    }

    const athena: AWS.Athena = sdk.wrapServiceErrorHandling(new AWS.Athena(sdk.config));
    const athenaWorkgroups = await athena.listWorkGroups().promise();
    const athenaWorkgroupNames = athenaWorkgroups.WorkGroups?.map((workgroup) => workgroup.Name!!) ?? [];
    for (const workgroupName of athenaWorkgroupNames) {
      const workgroupTags =
        (await (
          await athena
            .listTagsForResource({
              ResourceARN: `arn:aws:athena:${awsRegion}:${awsAccountId}:workgroup/${workgroupName}`,
            })
            .promise()
        ).Tags) ?? [];
      if (workgroupTags.some((tag) => tag.Key === "matano:managed" && tag.Value === "true")) {
        const spinner = ora(`Deleting Athena workgroup ${workgroupName}`).start();
        await athena.deleteWorkGroup({ WorkGroup: workgroupName, RecursiveDeleteOption: true }).promise();
        spinner.succeed();
      }
    }

    return;
  }

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Destroy);

    const { profile: awsProfile, output } = flags;
    const matanoUserDirectory = this.validateGetMatanoDir(flags);
    const { awsAccountId, awsRegion } = this.validateGetAwsRegionAccount(flags, matanoUserDirectory);

    console.log(`Destroying and cleaning up all stacks/resources from Matano deployment in ${awsRegion}...`);
    let cfnOutputs;
    try {
      cfnOutputs = await Destroy.destroyStacksAndResources(matanoUserDirectory, awsAccountId, awsRegion, awsProfile);
    } catch (error) {
      throw error;
    }
    console.log(chalk.green("\nSuccessfully destroyed and cleaned up all stacks/resources from Matano deployment"));
  }
}
