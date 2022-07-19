import * as fs from "fs";
import * as path from "path";
import * as YAML from 'yaml';

export function readConfig(directory: string, filename: string): Record<string, any> {
  return YAML.parse(fs.readFileSync(path.join(directory, filename), "utf8"));
}