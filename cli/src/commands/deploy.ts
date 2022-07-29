import { Command, Flags } from "@oclif/core";
import { prompt } from "enquirer";
import execa from "execa";

import ora from "ora";
import * as fs from "fs";
import * as fse from "fs-extra"; 
import path from "path";
import * as YAML from "yaml";
import { PROJ_ROOT_DIR } from "..";
import { readConfig } from "../util";
import BaseCommand from "../base";

export default class Deploy extends BaseCommand {
  static description = "Deploys matano.";

  static examples = [
    "matano deploy",
    "matano deploy --profile prod",
    "matano deploy --profile prod --user-directory matano-directory",
    "matano deploy --profile prod --region eu-central-1 --account 12345678901",
  ];

  static flags = {
    profile: Flags.string({
      char: "p",
      description: "AWS Profile to use for credentials.",
    }),
    account: Flags.string({
      char: "a",
      description: "AWS Account to deploy to.",
    }),
    region: Flags.string({
      char: "r",
      description: "AWS Region to deploy to.",
    }),
    "user-directory": Flags.string({
      required: false,
      description: "Matano user directory to use.",
    }),
  };

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Deploy);

    const { profile: awsProfile } = flags;
    const matanoUserDirectory = this.validateGetMatanoDir(flags);
    const { awsAccountId, awsRegion } = this.validateGetAwsRegionAccount(flags, matanoUserDirectory);
    const spinner = ora("Deploying Matano...").start();

    const cdkArgs = ["deploy", "*", "--require-approval", "never"];
    if (awsProfile) {
      cdkArgs.push("--profile", awsProfile);
    }

    const matanoConf = readConfig(matanoUserDirectory, "matano.config.yml");
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

    this.log(path.resolve(PROJ_ROOT_DIR, "infra"));
    this.log(path.resolve(PROJ_ROOT_DIR, "infra", "node_modules/.bin/cdk"));
    
    fse.removeSync(path.resolve(PROJ_ROOT_DIR, "infra", "cdk.out"));
    const subprocess = execa(path.resolve(PROJ_ROOT_DIR, "infra", "node_modules/.bin/cdk"), cdkArgs, {
      cwd: path.resolve(PROJ_ROOT_DIR, "infra"),
      env: {
        MATANO_CDK_ACCOUNT: awsAccountId,
        MATANO_CDK_REGION: awsRegion,
      },
    });

    subprocess.stdout?.pipe(process.stdout);
    subprocess.stderr?.pipe(process.stdout);

    await subprocess;
    spinner.succeed("Successfully deployed.");
  }
}