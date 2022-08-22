import { Command, Flags } from "@oclif/core";
import { prompt } from "enquirer";
import * as fs from "fs";
import * as fse from "fs-extra"; 
import path from "path";
import execa from "execa";

import ora from "ora";
import BaseCommand from "../base";
import RefreshContext from "./refresh-context";
import { PROJ_ROOT_DIR } from "..";

export default class Bootstrap extends BaseCommand {
  static description = "Creates initial resources for Matano deployment.";

  static examples = [
    `matano bootstrap`,
    "matano bootstrap --profile prod",
    `matano bootstrap --profile prod --user-directory my-matano-directory`,
  ];

  static flags = {
    profile: Flags.string({
      char: "p",
      description: "AWS Profile to use for credentials.",
    }),
    "user-directory": Flags.string({
      required: false,
      description: "Matano user directory to use.",
    }),
  };

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Bootstrap);

    const { profile: awsProfile } = flags;
    const matanoUserDirectory = this.validateGetMatanoDir(flags);
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

    const matanoContext = JSON.parse(fs.readFileSync(path.resolve(matanoUserDirectory, "matano.context.json"), "utf8"));

    const cdkContext: Record<string, any> = {
      matanoUserDirectory,
      matanoAwsAccountId: awsAccountId,
      matanoAwsRegion: awsRegion,
      matanoContext: JSON.stringify(matanoContext),
    };

    for (const [key, value] of Object.entries(cdkContext)) {
      cdkArgs.push(`--context`, `${key}=${value}`);
    }
    if (process.env.DEBUG) cdkArgs.push(`-vvv`);

    const cdkSubprocess = execa(path.resolve(PROJ_ROOT_DIR, "infra", "node_modules/.bin/cdk"), cdkArgs, {
      cwd: path.resolve(PROJ_ROOT_DIR, "infra"),
      env: {
        MATANO_CDK_ACCOUNT: awsAccountId,
        MATANO_CDK_REGION: awsRegion,
      },
    });
    const refreshContextPromise = RefreshContext.refreshMatanoContext(
      matanoUserDirectory, awsAccountId, awsRegion, awsProfile,
    );
    await Promise.all([cdkSubprocess, refreshContextPromise]);

    spinner.succeed("Successfully bootstrapped.");
  }
}
