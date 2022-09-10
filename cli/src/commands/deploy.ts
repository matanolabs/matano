import { CliUx, Command, Flags } from "@oclif/core";
import { prompt } from "enquirer";
import execa from "execa";

import ora from "ora";
import * as fs from "fs";
import * as fse from "fs-extra"; 
import path from "path";
import * as YAML from "yaml";
import { getCdkOutputDir, getCfnOutputsPath, getMatanoCdkApp, isPkg, PROJ_ROOT_DIR } from "..";
import { readConfig } from "../util";
import BaseCommand from "../base";
import { getCdkExecutable, } from "..";

export default class Deploy extends BaseCommand {
  static description = "Deploys matano.";

  static examples = [
    "matano deploy",
    "matano deploy --profile prod",
    "matano deploy --profile prod --user-directory matano-directory",
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

  static deployMatano(matanoUserDirectory: string, awsProfile: string | undefined, awsAccountId: string, awsRegion: string) {
    const cdkOutDir = getCdkOutputDir();

    CliUx.ux.debug("Using cdk out directory: " + cdkOutDir);

    const cdkArgs = [
      "deploy",
      "DPMainStack",
      "--require-approval",
      "never",
      "--app",
      getMatanoCdkApp(),
      "--output",
      cdkOutDir,
    ];
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

    const subprocess = execa(getCdkExecutable(), cdkArgs, {
      cwd: isPkg() ? undefined : path.resolve(PROJ_ROOT_DIR, "infra"),
      env: {
        MATANO_CDK_ACCOUNT: awsAccountId,
        MATANO_CDK_REGION: awsRegion,
      },
    });

    return subprocess;
  }

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Deploy);

    const { profile: awsProfile } = flags;
    const matanoUserDirectory = this.validateGetMatanoDir(flags);
    const { awsAccountId, awsRegion } = this.validateGetAwsRegionAccount(flags, matanoUserDirectory);
    const spinner = ora("Deploying Matano...").start();

    const subprocess = Deploy.deployMatano(matanoUserDirectory, awsProfile, awsAccountId, awsRegion)

    subprocess.stdout?.pipe(process.stdout);
    subprocess.stderr?.pipe(process.stdout);

    await subprocess;
    spinner.succeed("Successfully deployed.");
  }
}