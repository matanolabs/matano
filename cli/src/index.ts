import os from "os";
import fs from "fs";
import crypto from "crypto";
import path from "path";

export { run } from "@oclif/core";
export const SRC_ROOT_DIR = __dirname;
export const CLI_ROOT_DIR = path.join(SRC_ROOT_DIR, "../");
export const PROJ_ROOT_DIR = path.join(CLI_ROOT_DIR, "../");

export function getCfnOutputsPath() {
  return isPkg()
    ? path.join(makeTempDir("mtncdkoutput"), "outputs.json")
    : path.resolve(PROJ_ROOT_DIR, "infra", "outputs.json");
}

export const isPkg = () => !!(process as any).pkg;
export const getPkgRoot = () => path.dirname(process.execPath);

export function getCdkExecutable() {
  if (isPkg()) {
    return path.resolve(path.dirname(process.execPath), "cdk");
  } else {
    return path.resolve(CLI_ROOT_DIR, "node_modules/aws-cdk/bin/cdk");
  }
}

export function getMatanoCdkApp() {
  if (isPkg()) {
    return path.resolve(path.dirname(process.execPath), "matano-cdk");
  } else {
    return "node " + path.resolve(PROJ_ROOT_DIR, "infra/dist/bin/app.js");
  }
}

export function getCliExecutable() {
  if (isPkg()) {
    return path.resolve(path.dirname(process.execPath), "matano");
  } else {
    return path.resolve(PROJ_ROOT_DIR, "cli/bin/run");
  }
}

export function getCdkOutputDir() {
  return makeTempDir("matanocdkout");
}

export const randStr = (n = 20) => crypto.randomBytes(n).toString("hex");
export function makeTempDir(prefix?: string) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix ?? randStr()));
}
