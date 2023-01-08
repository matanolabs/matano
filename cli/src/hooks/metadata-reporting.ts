import * as path from "path";
import * as fs from "fs-extra";
import * as crypto from "crypto";
import { Hook } from "@oclif/core";
import { spawn } from "child_process";
import { getCliExecutable } from "..";
import { fileExists } from "../util";

// See https://www.matano.dev/docs/reference/metadata-reporting for more on anonymous metadata reporting.

const getAnonymousId = async (configDir: string) => {
  const idPath = path.join(configDir, "anon-id");
  try {
    return (await fs.readFile(idPath)).toString();
  } catch (error) {
    const id = crypto.randomUUID();
    await fs.outputFile(idPath, id);
    return id;
  }
};

const isDisabled = async (configDir: string) => {
  if (process.env.DO_NOT_TRACK || process.env.MATANO_NO_METADATA_REPORTING) {
    return true;
  }
  return fileExists(path.join(configDir, "no-metadata-reporting"));
};

const hook: Hook<"prerun"> = async function (opts) {
  try {
    // Avoid infinite loop.
    if (opts.Command.name === "InternalReportMetadata") {
      return;
    }
    if (await isDisabled(this.config.configDir)) {
      return;
    }
    const anonymousId = await getAnonymousId(this.config.configDir);
    const data = {
      id: anonymousId,
      command: opts.Command.name,
      arch: this.config.arch,
      version: this.config.version,
      platform: this.config.platform,
      ua: this.config.userAgent,
    };

    // Spawn a child process so that we don't block the main process.
    const process = spawn(getCliExecutable(), ["_internal_report_metadata", JSON.stringify(data)], {
      detached: true,
      stdio: "ignore",
    });
    process.on("error", (e) => {});
    process.unref();
  } catch (error) {}
};

export default hook;
