import { Flags } from "@oclif/core";
import * as path from "path";
import * as fs from "fs-extra";
import BaseCommand from "../base";

export default class DisableMetadataReporting extends BaseCommand {
  static description = "Disables metadata reporting. See https://www.matano.dev/docs/reference/metadata-reporting for more on anonymous metadata reporting.";

  static examples = ["matano disable-metadata-reporting"];

  static flags = {
    ...BaseCommand.flags,
  };

  async run() {
    const disabledPath = path.join(this.config.configDir, "no-metadata-reporting");
    await fs.outputFile(disabledPath, "");
  }
}
