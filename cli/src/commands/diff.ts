import { Flags } from "@oclif/core";
import execa, { ExecaError } from "execa";

import ora from "ora";
import chalk from "chalk";
import * as fs from "fs";
import path from "path";
import { getCdkOutputDir, getMatanoCdkApp, isPkg, PROJ_ROOT_DIR } from "..";
import BaseCommand from "../base";
import { getCdkExecutable } from "..";

const MATANO_ERROR_PREFIX = "MATANO_ERROR: ";

export default class Diff extends BaseCommand {
  static description = "Shows differences in your Matano deployment.";

  private foundErrorPattern = /Error: (.+)/;
  private cdkDiffResourcesPattern = /.*Resources.*\n([\s\S]+)\n\n/g;
  private cdkDiffOutputsPattern = /.*Outputs.*\n([\s\S]+)\n\n/g;

  static examples = [
    "matano diff",
    "matano diff --profile prod",
    "matano diff --profile prod --user-directory matano-directory",
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

  static diffMatano(
    matanoUserDirectory: string,
    awsProfile: string | undefined,
    awsAccountId: string,
    awsRegion: string
  ) {
    const cdkOutDir = getCdkOutputDir();
    const cdkArgs = ["diff", "DPMainStack", "--app", getMatanoCdkApp(), "--output", cdkOutDir, "--fail", "false"];
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
        FORCE_COLOR: "1",
        DEBUG: "-agent-base", // superflous logs
      },
    });

    return subprocess;
  }

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Diff);

    const { profile: awsProfile } = flags;
    const matanoUserDirectory = this.validateGetMatanoDir(flags);
    const { awsAccountId, awsRegion } = this.validateGetAwsRegionAccount(flags, matanoUserDirectory);
    const spinner = ora(chalk.dim("Checking Matano diff...")).start();

    const subprocess = Diff.diffMatano(matanoUserDirectory, awsProfile, awsAccountId, awsRegion);

    if (process.env.DEBUG) {
      subprocess.stdout?.pipe(process.stdout);
      subprocess.stderr?.pipe(process.stdout);
    }

    try {
      const result = await subprocess;
      spinner.stop();
      const stderr = result.stderr;
      const outputsMatches = [...stderr.matchAll(this.cdkDiffOutputsPattern)];
      const resourcesMatches = [...stderr.matchAll(this.cdkDiffResourcesPattern)];

      if (outputsMatches.length || resourcesMatches.length) {
        if (outputsMatches.length) {
          this.log(chalk.bold.underline("Outputs"));
          for (const match of outputsMatches) {
            console.log(match?.[1]);
          }
        }
        if (resourcesMatches.length) {
          this.log(chalk.bold.underline("Resources"));
          for (const match of resourcesMatches) {
            console.log(match?.[1]);
          }
        }
      } else {
        this.log(chalk.bold.green("There were no differences."));
      }
    } catch (e) {
      spinner.stop();
      const err = e as ExecaError;
      const matanoError = err.message.split("\n").find((s) => s.startsWith(MATANO_ERROR_PREFIX));

      const suggestions = [];
      if (matanoError) {
        this.error(matanoError.replace(MATANO_ERROR_PREFIX, ""), {
          exit: 1,
        });
      } else {
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
