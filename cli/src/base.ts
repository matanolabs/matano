import path from "path";
import chalk from "chalk";
import { Command, Flags, Errors } from "@oclif/core";
import { isMatanoDirectory, readConfig } from "./util";
import { CLIError } from "@oclif/core/lib/errors";

export class BaseCLIError extends CLIError {
  constructor(error: string | Error, options?: { exit?: number | false; } & Errors.PrettyPrintableError) {
    super(error, options);
    this.name = chalk.bold.red("Error")
  }
}

export default abstract class BaseCommand extends Command {

  static flags = {
    debug: Flags.boolean({
      description: "View debug information.",
    }),
  }

  async init() {
    const { flags } = await this.parse(this.constructor as any) as any;
    if (flags.debug) {
      process.env.DEBUG = "*"
      this.config.debug = 1;
    }
  }

  error(input: string | Error, options: {code?: string; exit: false} & Errors.PrettyPrintableError): void
  error(input: string | Error, options?: {code?: string; exit?: number} & Errors.PrettyPrintableError): never
  error(input: string | Error, options: {code?: string; exit?: number | false} & Errors.PrettyPrintableError = {}): void {
    return Errors.error(new BaseCLIError(input, options) , options as any);
  }

  protected async catch(err: Error & { exitCode?: number | undefined; }): Promise<any> {
    if (err instanceof CLIError) {
      throw err;
    }
    else {
      throw new BaseCLIError(`An error occurred: ${err.message}`);
    }
  }

  validateGetMatanoDir(flags: any): string {
    const defaultUserDir = process.cwd();
    const userDir = flags["user-directory"] as any;
    if (userDir && !isMatanoDirectory(userDir)) {
      this.error(`Invalid matano directory: ${path.resolve(userDir)}.`, {
        suggestions: ["Make sure the directory specified by `--user-directory` is a valid matano directory."],
      });
    }
    if (!userDir && !isMatanoDirectory(defaultUserDir)) {
      this.error("No Matano user directory found.", {
        suggestions: [
          "Specify a directory using `--user-directory`.",
          "Run this command from a valid Matano directory.",
        ],
      });
    }
    return path.resolve(userDir ?? defaultUserDir);
  }

  validateGetAwsRegionAccount(flags: any, userDirectory: string) {
    let { account: awsAccountId, region: awsRegion } = flags;
    if (awsAccountId && awsRegion) {
      return { awsAccountId, awsRegion }
    }
    const matanoConfig = readConfig(userDirectory, "matano.config.yml");
    if (!awsAccountId) awsAccountId = matanoConfig?.["aws_account"];
    if (!awsRegion) awsRegion = matanoConfig?.["aws_region"];
    if (!awsAccountId || !awsRegion) {
      this.error("AWS Account ID and/or AWS region not specified.", {
        suggestions: [
          "Specify AWS account/region in matano.config.yml",
        ]
      })
    } else {
      return { awsAccountId, awsRegion }
    }
  }
}
