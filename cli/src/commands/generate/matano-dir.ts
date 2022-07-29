import * as fs from "fs";
import * as path from "path";
import { Command, Flags } from "@oclif/core";
import { prompt } from "enquirer";
import execa from "execa";
import ora from "ora";

const autogenerateNote = ``;
const sampleDetectionPy = `# ${autogenerateNote}

def detect(record):
    return True
`;

const sampleDetectionYml = `# ${autogenerateNote}

name: "cheese"
log_sources:
  - "me_source"
`;

const sampleLogSourceYml = `# ${autogenerateNote}

name: "me_source"
`;

const sampleDetectionReqs = `# ${autogenerateNote}
`;

export default class GenerateMatanoDir extends Command {
  static description = "Generates a sample Matano directory to get started.";

  static examples = [`matano generate:matano-dir`, ];

  static args = [
    {
      name: 'directory-name',
      required: true,
      description: 'The name of the directory to create',
    }
  ];

  async run(): Promise<void> {
    const { args, flags } = await this.parse(GenerateMatanoDir);
    const dirName = args["directory-name"];

    fs.mkdirSync(path.join(dirName, "detections/my_detection"), { recursive: true });
    fs.mkdirSync(path.join(dirName, "log_sources/my_log_source"), { recursive: true });
    fs.writeFileSync(path.join(dirName, "detections/my_detection/detect.py"), sampleDetectionPy);
    fs.writeFileSync(path.join(dirName, "detections/my_detection/detection.yml"), sampleDetectionYml);
    fs.writeFileSync(path.join(dirName, "detections/my_detection/requirements.txt"), sampleDetectionReqs);
    fs.writeFileSync(path.join(dirName, "log_sources/my_log_source/log_source.yml"), sampleLogSourceYml);

    this.log(`Generated sample matano directory in ${dirName}.`);
  }
}
