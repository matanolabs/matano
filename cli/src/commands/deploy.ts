import { Command, Flags } from "@oclif/core";
import { prompt } from "enquirer";
import execa from "execa";

import ora from "ora";
import * as fs from "fs";
import path from "path";
import { PROJ_ROOT_DIR, } from "..";

export default class Deploy extends Command {
  static description = "Deploys matano.";

  static examples = [
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
      required: true,
    }),
    region: Flags.string({
      char: "r",
      description: "AWS Region to deploy to.",
      required: true,
    }),
    "user-directory": Flags.string({
      required: false,
      description: "Matano user directory to use."
    }),
  };

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Deploy);

    const {
      profile: awsProfile,
      region: awsRegion,
      account: awsAccountId,
    } = flags;

    const defaultUserDir = path.join(process.cwd(), ".matano");
    if (!fs.existsSync(defaultUserDir) && !flags["user-directory"]) {
      this.error("No Matano user directory found.", { suggestions: ["Specify a directory using `--user-directory`.",], });
    }

    const spinner = ora("Deploying Matano...").start();

    const cdkArgs = ["deploy", "*", "--require-approval", "never"];
    if (awsProfile) {
      cdkArgs.push("--profile", awsProfile);
    }
    const cdkContext = {
      matanoUserDirectory: path.resolve(flags["user-directory"]!!) ?? defaultUserDir,
    };

    for (const [key, value] of Object.entries(cdkContext)) {
      cdkArgs.push(`--context`, `${key}=${value}`);
    }

    this.log(path.resolve(PROJ_ROOT_DIR, "infra"))
    this.log(path.resolve(PROJ_ROOT_DIR, "infra", "node_modules/.bin/cdk"))
    await execa(path.resolve(PROJ_ROOT_DIR, "infra", "node_modules/.bin/cdk"), cdkArgs, {
      cwd: path.resolve(PROJ_ROOT_DIR, "infra"),
      env: {
        MATANO_CDK_ACCOUNT: awsAccountId,
        MATANO_CDK_REGION: awsRegion,
      },
    });

    spinner.succeed("Successfully deployed.");
  }
}
