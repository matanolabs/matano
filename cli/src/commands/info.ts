import { CliUx, Command, Flags } from "@oclif/core";
import ora from "ora";
import BaseCommand from "../base";
import chalk from "chalk";
import { SdkProvider } from "aws-cdk/lib/api/aws-auth/sdk-provider";
import { Mode } from "aws-cdk/lib/api/plugin/credential-provider-source";
import * as cxapi from "@aws-cdk/cx-api";
import { CloudFormation } from "aws-sdk";
import wrap from "word-wrap";
import Table from 'cli-table3';



const cfnOutputKeyNameMap = {

};
function defaultWrap(s: string, opts?: wrap.IOptions) {
    return wrap(s, { indent: "", ...opts })
}

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
    profile: Flags.string({
      char: "p",
      description: "AWS Profile to use for credentials.",
    }),
    output: CliUx.ux.table.flags().output,
  };

  private static async getCfnOutputs(cfn: CloudFormation, stackName: string) {
    const outputs = (await cfn.describeStacks({StackName: stackName, }).promise()).Stacks!![0].Outputs!!;
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


    // TODO: improve when https://github.com/cli-table/cli-table3/pull/303 merged
    const getColWidths = () => {
        const totalColWidth = process.stdout.columns;
        if (!totalColWidth) return [null, 21, 30] // conservative?
        else if (totalColWidth < 60) return [Math.round(totalColWidth/3), Math.round(totalColWidth/3), Math.round(totalColWidth/3)]
        else if (totalColWidth > 110) return [32, 21, totalColWidth - 53 - 8]
        else return [20, 21, totalColWidth - 41 - 8];
    }
    const table = new Table({
        head: ["Name", "Value", "Description",].map(s => chalk.cyanBright.bold(s)),
        wordWrap: true,
        wrapOnWordBoundary: false,
        colWidths: getColWidths(),
    });
    for (const row of cfnOutputs) {
        table.push(Object.values(row) as any)
    }
    console.log(table.toString());
  }


  async run(): Promise<void> {
    const { args, flags } = await this.parse(Info);

    const { profile: awsProfile, output } = flags;
    const matanoUserDirectory = this.validateGetMatanoDir(flags);
    const { awsAccountId, awsRegion } = this.validateGetAwsRegionAccount(flags, matanoUserDirectory);

    const spinner = ora("Retrieving Matano deployment information...").start();
    const cfnOutputs = await Info.retrieveCfnOutputs(awsAccountId, awsRegion, awsProfile);
    spinner.stop();

    Info.renderOutputsTable(cfnOutputs, output);
  }
}
