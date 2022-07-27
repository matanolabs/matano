import path from "path";
import { Command, Flags } from "@oclif/core";
import { isMatanoDirectory } from "../util";

export default abstract class BaseCommand extends Command {
  validateGetMatanoDir(flags: any): string {
    const defaultUserDir = process.cwd();
    console.log(flags);
    console.log(defaultUserDir);
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
}
