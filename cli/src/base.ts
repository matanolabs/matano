import path from "path";
import { Command, Flags } from "@oclif/core";
import { isMatanoDirectory, readConfig } from "./util";

export default abstract class BaseCommand extends Command {
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
          "Pass AWS account/region using CLI flags."
        ]
      })
    } else {
      return { awsAccountId, awsRegion }
    }
  }
}
