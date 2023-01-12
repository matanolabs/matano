import * as fs from "fs";
import * as path from "path";
import { Command, Flags } from "@oclif/core";
import { prompt } from "enquirer";
import execa from "execa";
import ora from "ora";

const autogenerateNote = ``;
const sampleMatanoConfigYml = (awsAccountId?: string, awsRegion?: string) => `# Use this file for Matano configuration.

aws_account: "${awsAccountId ?? "123456789012"}" # Specify the AWS account to deploy to.
aws_region: "${awsRegion ?? "us-east-1"}" # Specify the AWS region to deploy to.

# Optionally, specify any tags to apply to the deployed resources.
# aws_tags:
#  key: "my_tag"

`;
const sampleDetectionPy = `# ${autogenerateNote}

def detect(record):
    return False
`;

const sampleDetectionYml = `# ${autogenerateNote}

name: "my_detection"
tables:
  - "test_log_source"
`;

const sampleLogSourceYml = `# ${autogenerateNote}
name: "test_log_source"

schema:
  fields: []
  ecs_field_names: []
`;

const sampleDetectionReqs = `# ${autogenerateNote}
`;

export default class GenerateMatanoDir extends Command {
  static description = "Generates a sample Matano directory to get started.";

  static examples = [`matano generate:matano-dir`];

  static args = [
    {
      name: "directory-name",
      required: true,
      description: "The name of the directory to create",
    },
  ];

  static generateMatanoDirectory(dirName: string, awsAccountId?: string, awsRegion?: string) {
    fs.mkdirSync(path.resolve(dirName), { recursive: true });
    fs.writeFileSync(path.join(dirName, "matano.config.yml"), sampleMatanoConfigYml(awsAccountId, awsRegion));
    fs.mkdirSync(path.join(dirName, "detections/my_detection"), { recursive: true });
    fs.mkdirSync(path.join(dirName, "log_sources/test_log_source"), { recursive: true });
    fs.writeFileSync(path.join(dirName, "detections/my_detection/detect.py"), sampleDetectionPy);
    fs.writeFileSync(path.join(dirName, "detections/my_detection/detection.yml"), sampleDetectionYml);
    fs.writeFileSync(path.join(dirName, "detections/my_detection/requirements.txt"), sampleDetectionReqs);
    fs.writeFileSync(path.join(dirName, "log_sources/test_log_source/log_source.yml"), sampleLogSourceYml);
  }

  async run(): Promise<void> {
    const { args, flags } = await this.parse(GenerateMatanoDir);
    const dirName = args["directory-name"];

    GenerateMatanoDir.generateMatanoDirectory(dirName);

    this.log(`Generated sample matano directory in ${dirName}.`);
  }
}
