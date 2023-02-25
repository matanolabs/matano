import path from "path";
import chalk from "chalk";
import { Command, Flags, Errors } from "@oclif/core";
import { isMatanoDirectory, parseMatanoConfig, readConfig } from "./util";
import { CLIError } from "@oclif/core/lib/errors";
import { execSync } from "child_process";

export class BaseCLIError extends CLIError {
  constructor(error: string | Error, options?: { exit?: number | false } & Errors.PrettyPrintableError) {
    super(error, options);
    this.name = chalk.bold.red("Error");
  }
}

export default abstract class BaseCommand extends Command {
  static flags = {
    debug: Flags.boolean({
      description: "View debug information.",
    }),
  };

  async init() {
    const { flags } = (await this.parse(this.constructor as any)) as any;
    if (flags.debug) {
      process.env.DEBUG = "*";
      this.config.debug = 1;
    }
  }

  error(input: string | Error, options: { code?: string; exit: false } & Errors.PrettyPrintableError): void;
  error(input: string | Error, options?: { code?: string; exit?: number } & Errors.PrettyPrintableError): never;
  error(
    input: string | Error,
    options: { code?: string; exit?: number | false } & Errors.PrettyPrintableError = {}
  ): void {
    return Errors.error(new BaseCLIError(input, options), options as any);
  }

  protected async catch(err: Error & { exitCode?: number | undefined }): Promise<any> {
    if (err instanceof CLIError) {
      throw err;
    } else {
      throw new BaseCLIError(`An error occurred: ${err.message}`);
    }
  }

  validateGetMatanoDir(flags: any): string {
    const defaultUserDir = process.cwd();
    const userDir = flags["user-directory"] as any;

    let selectedMatanoDir = null;

    if (userDir) {
      if (isMatanoDirectory(userDir)) {
        selectedMatanoDir = userDir;
      } else {
        this.error(`Invalid matano directory: ${path.resolve(userDir)}.`, {
          suggestions: ["Make sure the directory specified by `--user-directory` is a valid matano directory."],
        });
      }
    } else if (isMatanoDirectory(defaultUserDir)) {
      selectedMatanoDir = defaultUserDir;
    } else {
      // try to find a matano directory under the current directory
      try {
        const discoveredMatanoConfigPath = execSync(
          `find ${defaultUserDir} -maxdepth 3 -iname "*matano.config.yml" | head -n 1`,
          {
            encoding: "utf-8",
          }
        ).toString();

        if (!!discoveredMatanoConfigPath) {
          selectedMatanoDir = path.dirname(discoveredMatanoConfigPath);
        }
      } catch (err) {
        // do nothing
      }
      // try to find a matano directory under the git repo
      try {
        const gitRoot = execSync("git rev-parse --show-toplevel", { encoding: "utf8" }).toString().trim();
        const discoveredMatanoConfigPath = execSync(
          `find ${gitRoot} -maxdepth 3 -iname "*matano.config.yml" | head -n 1`,
          {
            encoding: "utf-8",
          }
        ).toString();

        if (!!discoveredMatanoConfigPath) {
          selectedMatanoDir = path.dirname(discoveredMatanoConfigPath);
        }
      } catch (err) {
        // do nothing
      }
    }

    if (!selectedMatanoDir) {
      this.error("No Matano user directory found.", {
        suggestions: [
          "Specify a directory using `--user-directory`.",
          "Run this command from a valid Matano directory.",
        ],
      });
    }

    selectedMatanoDir = path.resolve(selectedMatanoDir);
    if (flags.debug) {
      console.debug(`Using Matano directory: ${selectedMatanoDir}\n`);
    }

    return selectedMatanoDir;
  }

  validateGetAwsRegionAccount(flags: any, userDirectory: string) {
    let { account: awsAccountId, region: awsRegion } = flags;
    if (awsAccountId && awsRegion) {
      return { awsAccountId, awsRegion };
    }
    const matanoConfig = parseMatanoConfig(userDirectory);
    if (!awsAccountId) awsAccountId = matanoConfig?.aws?.account;
    if (!awsRegion) awsRegion = matanoConfig?.aws?.region;
    if (!awsAccountId || !awsRegion) {
      this.error("AWS Account ID and/or AWS region not specified.", {
        suggestions: ["Specify AWS account/region in matano.config.yml"],
      });
    } else {
      return { awsAccountId, awsRegion };
    }
  }
}
