import * as fs from "fs";
import * as path from "path";
import * as YAML from "yaml";

export function readConfig(directory: string, filename: string): Record<string, any> {
  return YAML.parse(fs.readFileSync(path.join(directory, filename), "utf8"));
}

export function isMatanoDirectory(dirpath: string) {
  return fs.existsSync(dirpath) && fs.existsSync(path.join(dirpath, "matano.config.yml"));
}
