import os from "os";
import fs from "fs-extra";
import crypto from "crypto";
import path from "path";

export { run } from "@oclif/core";
const SRC_ROOT_DIR = __dirname;
const CLI_ROOT_DIR = path.join(SRC_ROOT_DIR, "../");
export const PROJ_ROOT_DIR = path.join(CLI_ROOT_DIR, "../");

export const getLocalProjRootDir = () => {
  if (process.env.MATANO_REPO_DIR) {
    return process.env.MATANO_REPO_DIR;
  } else {
    try {
      fs.statSync(path.resolve(PROJ_ROOT_DIR, ".git"));
      return PROJ_ROOT_DIR;
    } catch {
      throw new Error(
        "Please set the `MATANO_REPO_DIR` env var to the root of the Matano project checkout. You are likely developing locally and using nvm."
      );
    }
  }
};

export function getCfnOutputsPath() {
  return isPkg()
    ? path.join(makeTempDir("mtncdkoutput"), "outputs.json")
    : path.resolve(getLocalProjRootDir(), "infra", "outputs.json");
}

export const isPkg = () => !!(process as any).pkg;
export const getPkgRoot = () => path.dirname(process.execPath);

export function getCdkExecutable() {
  if (isPkg()) {
    return path.resolve(getPkgRoot(), "cdk");
  } else {
    return path.resolve(getLocalProjRootDir(), "cli", "node_modules/aws-cdk/bin/cdk");
  }
}

export function getMatanoCdkApp() {
  if (isPkg()) {
    return path.resolve(path.dirname(process.execPath), "matano-cdk");
  } else {
    return "node " + path.resolve(getLocalProjRootDir(), "infra/dist/bin/app.js");
  }
}

export function getCliExecutable() {
  if (isPkg()) {
    return path.resolve(path.dirname(process.execPath), "matano");
  } else {
    return path.resolve(getLocalProjRootDir(), "cli/bin/run");
  }
}

export function getCdkOutputDir() {
  return makeTempDir("matanocdkout");
}

export const randStr = (n = 20) => crypto.randomBytes(n).toString("hex");
export function makeTempDir(prefix?: string) {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix ?? randStr()));
}
