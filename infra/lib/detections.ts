import * as fs from "fs";
import * as path from "path";
import * as TOML from "@iarna/toml";

export function readConfig(directory: string, filename: string): Record<string, any> {
  return TOML.parse(fs.readFileSync(path.join(directory, filename), "utf8"));
}

export function readDetectionConfig(detectionDirectory: string): Record<string, any> {
  return readConfig(detectionDirectory, "detection.toml");
}
