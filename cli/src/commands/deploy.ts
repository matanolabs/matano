import { CliUx, Command, Flags } from "@oclif/core";
import { prompt } from "enquirer";
import execa, { ExecaError } from "execa";

import ora from "ora";
import chalk from "chalk";
import * as fs from "fs";
import * as fse from "fs-extra";
import path from "path";
import * as YAML from "yaml";
import { getCdkOutputDir, getCfnOutputsPath, getMatanoCdkApp, isPkg, getLocalProjRootDir } from "..";
import { parseMatanoConfig, readConfig, safeLoadMatanoContext } from "../util";
import BaseCommand from "../base";
import { getCdkExecutable } from "..";

const MATANO_ERROR_PREFIX = "MATANO_ERROR: ";

export default class Deploy extends BaseCommand {
  static description = "Deploys matano.";

  private foundErrorPattern = /Error: (.+)/;

  static examples = [
    "matano deploy",
    "matano deploy --profile prod",
    "matano deploy --profile prod --user-directory matano-directory",
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
  };

  static deployMatano(
    matanoUserDirectory: string,
    awsProfile: string | undefined,
    awsAccountId: string,
    awsRegion: string,
    debug = false
  ) {
    const cdkOutDir = getCdkOutputDir();
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

    const matanoConfig = parseMatanoConfig(matanoUserDirectory);
    const matanoContext = safeLoadMatanoContext(matanoUserDirectory);

    const cdkContext: Record<string, any> = {
      matanoUserDirectory,
      matanoAwsAccountId: awsAccountId,
      matanoAwsRegion: awsRegion,
      matanoContext: JSON.stringify(matanoContext),
      // https://github.com/aws/aws-cdk/issues/19098 TODO: maybe update CDK to remove?
      "@aws-cdk/aws-lambda:recognizeLayerVersion": true,
    };

    for (const [key, value] of Object.entries(cdkContext)) {
      cdkArgs.push(`--context`, `${key}=${value}`);
    }
    if (process.env.DEV) cdkArgs.push(`--hotswap`);
    if (process.env.DEBUG) cdkArgs.push(`-vvv`);

    const subprocess = execa(getCdkExecutable(), cdkArgs, {
      cwd: isPkg() ? undefined : path.resolve(getLocalProjRootDir(), "infra"),
      env: {
        MATANO_CDK_ACCOUNT: awsAccountId,
        MATANO_CDK_REGION: awsRegion,
        MATANO_PROJECT_LABEL: matanoConfig.project_label,
        FORCE_COLOR: "1",
        DEBUG: "-agent-base", // superflous logs
      },
    });

    if (debug) {
      subprocess.stdout?.pipe(process.stdout);
      subprocess.stderr?.pipe(process.stdout);
    }
    return subprocess;
  }

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Deploy);

    const { profile: awsProfile } = flags;
    const matanoUserDirectory = this.validateGetMatanoDir(flags);
    const { awsAccountId, awsRegion } = this.validateGetAwsRegionAccount(flags, matanoUserDirectory);
    const spinner = ora(chalk.dim("Deploying Matano...")).start();

    const subprocess = Deploy.deployMatano(matanoUserDirectory, awsProfile, awsAccountId, awsRegion, flags.debug);

    try {
      await subprocess;
      spinner.succeed("Successfully deployed.");
    } catch (e) {
      spinner.fail("Deployment failed.");
      const err = e as ExecaError;
      const matanoError = err.message.split("\n").find((s) => s.startsWith(MATANO_ERROR_PREFIX));

      const suggestions = [];
      if (matanoError) {
        this.error(matanoError.replace(MATANO_ERROR_PREFIX, ""), {
          exit: 1,
        });
      } else {
        if (flags.debug) {
          console.debug((e as Error).message ?? e);
        }
        const foundError = err.message.split("\n").find((s) => s.match(this.foundErrorPattern));
        let message: string;
        if (foundError) {
          message = foundError.match(this.foundErrorPattern)?.[1]!!;
        } else {
          message = "An error occurred during deployment.";
          suggestions.push("Run with --debug to see debug logs.");
        }

        this.error(message, {
          exit: 1,
          message: err.message,
          suggestions,
        });
      }
    }
  }
}
