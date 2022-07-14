import { Command, Flags } from "@oclif/core";
import { prompt } from "enquirer";
import execa from "execa";

import ora from "ora";

export default class Bootstrap extends Command {
  static description = "Creates initial resources for Matano deployment.";

  static examples = [`matano bootstrap`, "matano bootstrap --profile prod"];

  static flags = {
    profile: Flags.string({
      char: "p",
      description: "AWS Profile to use for credentials.",
    }),
  };

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Bootstrap);

    const { profile: awsProfile } = flags;
    const regionPrompt = prompt<any>({
      type: "input",
      name: "awsRegion",
      message: "AWS Region",
      initial: process.env.AWS_DEFAULT_REGION ?? undefined,
    });

    const getAwsAcctId = async (profile?: string) => {
      try {
        const { stdout: awsStdout } = await execa(
          "aws",
          ["sts", "get-caller-identity"].concat(
            profile ? ["--profile", profile] : []
          )
        );
        return JSON.parse(awsStdout).Account;
      } catch (error) {
        return undefined;
      }
    };

    const [{awsRegion}, maybeDefaultAwsAccountId] = await Promise.all([
      regionPrompt,
      getAwsAcctId(awsProfile),
    ]);

    const { awsAccountId } = await prompt<any>({
      type: "input",
      name: "awsAccountId",
      message: "AWS Account ID",
      initial: maybeDefaultAwsAccountId ?? undefined,
    });

    const cdkEnvironment = `aws://${awsAccountId}/${awsRegion}`;

    const spinner = ora("Bootstrapping AWS environment...").start();

    const cdkArgs = ["bootstrap", cdkEnvironment];
    if (awsProfile) {
      cdkArgs.push("--profile", awsProfile);
    }

    await execa("cdk", cdkArgs);

    spinner.succeed("Successfully bootstrapped.");
  }
}
