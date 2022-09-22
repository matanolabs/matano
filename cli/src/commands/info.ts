import { CliUx, Command, Flags } from "@oclif/core";
import ora from "ora";
import BaseCommand, { BaseCLIError } from "../base";
import chalk from "chalk";
import { SdkProvider, SdkForEnvironment } from "aws-cdk/lib/api/aws-auth/sdk-provider";
import { Mode } from "aws-cdk/lib/api/plugin/credential-provider-source";
import * as cxapi from "@aws-cdk/cx-api";
import { CloudFormation } from "aws-sdk";
import Table from 'cli-table3';
import { promiseTimeout } from "../util";

function isInteractive({stream = process.stdout} = {}) {
	return Boolean(
		stream && stream.isTTY &&
		process.env.TERM !== 'dumb' &&
		!('CI' in process.env)
	);
}

export default class Info extends BaseCommand {
  static description = "Retrieves information about your Matano deployment in structured format.";

  static examples = [
    "matano info",
    "matano info --profile prod",
    "matano info --output json",
  ];

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

  private static async getCfnOutputs(cfn: CloudFormation, stackName: string) {
    let describeResult;
    // TODO: there's probably a better way to do this, shouldn't hang on invalic creds...
    try {
      describeResult = await promiseTimeout(() => cfn.describeStacks({StackName: stackName, }).promise());
    } catch (error) {
      if (error  === "Timed out.") {
        throw new BaseCLIError("Failed to retrieve values. Your AWS credentials are likely misconfigured.");
      } else {
        throw error;
      }
    }
    const outputs = describeResult.Stacks!![0].Outputs!!;
    return outputs
        .filter(obj => !!obj.Description)
        .map(obj => ({
            name: obj.OutputKey!!,
            value: obj.OutputValue!!,
            description: obj.Description!!,
        }));
  }

  static async retrieveCfnOutputs(awsAccountId: string, awsRegion: string, awsProfile?: string) {
    const sdkProvider = await SdkProvider.withAwsCliCompatibleDefaults({ profile: awsProfile, });
    const cfn = (await sdkProvider.forEnvironment(cxapi.EnvironmentUtils.make(awsAccountId, awsRegion), Mode.ForReading, {})).sdk.cloudFormation();

    const [o1, o2] = await Promise.all([
        this.getCfnOutputs(cfn, "MatanoDPCommonStack"),
        this.getCfnOutputs(cfn, "MatanoDPMainStack"),
    ]);
    const outputs = [...o1, ...o2];
    return outputs;
  }

  static renderOutputsTable(cfnOutputs: any, output?: string) {
    if (output || !isInteractive()) {
        const columns: CliUx.Table.table.Columns<any> = {
            name: {},
            value: {},
            description: {},
        }
        return CliUx.Table.table(cfnOutputs, columns, {
            output: output ?? "json",
        });
    }

    const table = new Table({
        head: ["Name", "Value", "Description",].map(s => chalk.cyanBright.bold(s)),
        wordWrap: true,
        wrapOnWordBoundary: false,
    });

    for (const row of cfnOutputs) {
        table.push([
          row.name,
          row.value,
          { content: row.description, wrapOnWordBoundary: true, }
        ]);
    }
    console.log(table.toString());
  }


  async run(): Promise<void> {
    const { args, flags } = await this.parse(Info);

    const { profile: awsProfile, output } = flags;
    const matanoUserDirectory = this.validateGetMatanoDir(flags);
    const { awsAccountId, awsRegion } = this.validateGetAwsRegionAccount(flags, matanoUserDirectory);

    const spinner = ora("Retrieving Matano deployment information...").start();
    let cfnOutputs;
    try {
      cfnOutputs = await Info.retrieveCfnOutputs(awsAccountId, awsRegion, awsProfile);
    } catch (error) {
      spinner.stop();
      throw error;
    }

    Info.renderOutputsTable(cfnOutputs, output);
  }
}
