import { Hook } from "@oclif/core";
import * as fs from "fs-extra";
import * as path from "path";
import chalk from "chalk";
import Table from "tty-table";
import { fileExists } from "../util";

// See https://www.matano.dev/docs/reference/metadata-reporting for more on anonymous metadata reporting.

// Show a message about metadata reporting.
const hook: Hook<"init"> = async function (opts) {
  try {
    const infoPath = path.join(this.config.configDir, "info-metadata-reporting");
    if (!(await fileExists(infoPath))) {
      const msg = chalk.greenBright(
        `Matano collects anonymous metadata to help us improve the project. See https://www.matano.dev/docs/reference/metadata-reporting for more information. To disable metadata reporting, run \`matano disable-metadata-reporting\`.`
      );
      const header: Table.Header[] = [{ value: "text", width: "100%" }];
      const table = Table(header, [{ text: msg }], { showHeader: false } as any);
      this.log(table.render());

      await fs.outputFile(infoPath, "");
    }
  } catch {}
};

export default hook;
