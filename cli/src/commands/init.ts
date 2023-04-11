import { CliUx, Command, Flags } from "@oclif/core";

const { Confirm, AutoComplete } = require("enquirer");
import { prompt } from "enquirer";
import * as fs from "fs-extra";
import path from "path";
import execa from "execa";
import chalk from "chalk";
import styles from "ansi-styles";
import ora from "ora";
import BaseCommand from "../base";
import RefreshContext from "./refresh-context";
import { getCdkExecutable, getCdkOutputDir, getMatanoCdkApp, isPkg, getLocalProjRootDir } from "..";
import GenerateMatanoDir from "./generate/matano-dir";
import Deploy from "./deploy";
import { AWS_REGIONS, fileExists, isInteractive, parseMatanoConfig, readConfig } from "../util";
import Info from "./info";

const getAwsAcctId = async (profile?: string) => {
  try {
    const { stdout: awsStdout } = await execa(
      "aws",
      ["sts", "get-caller-identity"].concat(profile ? ["--profile", profile] : [])
    );
    return JSON.parse(awsStdout).Account;
  } catch (error) {
    return undefined;
  }
};

export default class Init extends BaseCommand {
  static description =
    "Wizard to get started with Matano. Creates resources, initializes your account, and deploys Matano.";

  static examples = [`matano init`, "matano init --profile prod"];

  static flags = {
    ...BaseCommand.flags,
    profile: Flags.string({
      char: "p",
      description: "AWS Profile to use for credentials.",
    }),
    "user-directory": Flags.string({
      required: false,
      description: "Matano user directory to use.",
    }),
  };

  async run(): Promise<void> {
    const { args, flags } = await this.parse(Init);
    const { profile: awsProfile } = flags;

    let resolvedArgs: any = {};
    if (flags["user-directory"]) {
      const matanoUserDirectory = path.resolve(flags["user-directory"]);
      const config = parseMatanoConfig(matanoUserDirectory);
      const awsAccountId = config?.aws?.account;
      const awsRegion = config?.aws?.region;
      resolvedArgs = {
        matanoUserDirectory,
        awsRegion,
        awsAccountId,
      };
    } else if (isInteractive()) {
      resolvedArgs = await this.getInteractiveArgs(awsProfile);
    } else {
      this.error("Must provide all required flags in non-interactive mode.");
    }
    const { matanoUserDirectory, awsRegion, awsAccountId } = resolvedArgs;

    const spinner1 = ora("Initializing AWS environment... (1/3)").start();

    // Dont use VPC for now.
    // const matanoContext = await RefreshContext.refreshMatanoContext(
    //   matanoUserDirectory,
    //   awsAccountId,
    //   awsRegion,
    //   awsProfile
    // );
    spinner1.text = "Initializing AWS environment... (2/3)";

    if (!(await fileExists(matanoUserDirectory))) {
      this.error(`Matano directory does not exist at ${matanoUserDirectory}.`);
    }

    await Init.bootstrapMatano(awsAccountId, awsRegion, matanoUserDirectory, awsProfile, flags.debug);

    spinner1.succeed("Successfully initialized your account.");

    const spinner2 = ora("Now deploying Matano to your AWS account...").start();
    await Deploy.deployMatano(matanoUserDirectory, awsProfile, awsAccountId, awsRegion, flags.debug);
    spinner2.succeed(chalk.bold.greenBright("Successfully deployed Matano."));

    this.log("\n" + chalk.yellowBright("· Here are some useful values to get you started:") + "");
    const cfnOutputs = await Info.retrieveCfnOutputs(matanoUserDirectory, awsAccountId, awsRegion, awsProfile);
    Info.renderOutputsTable(cfnOutputs);
  }

  static bootstrapMatano(
    awsAccountId: string,
    awsRegion: string,
    matanoUserDirectory: string,
    awsProfile?: string,
    debug = false
  ) {
    const cdkEnvironment = `aws://${awsAccountId}/${awsRegion}`;

    const cdkArgs = ["bootstrap", cdkEnvironment, "--app", getMatanoCdkApp(), "--output", getCdkOutputDir()];
    if (awsProfile) {
      cdkArgs.push("--profile", awsProfile);
    }
    const matanoConfig = parseMatanoConfig(matanoUserDirectory);
    if (matanoConfig?.aws?.set_block_public_access === false) {
      cdkArgs.push("--public-access-block-configuration", "false");
    }

    const cdkContext: Record<string, any> = {
      matanoUserDirectory,
      matanoAwsAccountId: awsAccountId,
      matanoAwsRegion: awsRegion,
      matanoContext: JSON.stringify({}),
    };

    for (const [key, value] of Object.entries(cdkContext)) {
      cdkArgs.push(`--context`, `${key}=${value}`);
    }
    if (debug) cdkArgs.push(`-vvv`);

    const subprocess = execa(getCdkExecutable(), cdkArgs, {
      cwd: isPkg() ? undefined : path.resolve(getLocalProjRootDir(), "infra"),
      env: {
        MATANO_CDK_ACCOUNT: awsAccountId,
        MATANO_CDK_REGION: awsRegion,
        FORCE_COLOR: "1",
        DEBUG: "-agent-base", // superfluous logs
      },
    });
    if (debug) {
      subprocess.stdout?.pipe(process.stdout);
      subprocess.stderr?.pipe(process.stdout);
    }
    return subprocess;
  }

  async getInteractiveArgs(awsProfile?: string) {
    this.log(chalk.dim("━━━ ") + chalk.bold.whiteBright("Matano: Get started Wizard") + chalk.dim(" ━━━") + "\n");

    this.log(chalk.bold.cyanBright("Welcome to the Matano init wizard. This will get you started with Matano."));
    this.log(chalk.dim("Follow the prompts to get started. You can always change these values later."));
    this.log("");

    // const prog = CliUx.ux.progress({
    //   format: styles.cyan.open + '{bar}\u001b[0m',
    //   barCompleteChar: '━', //'\u2588',
    //   barIncompleteChar: '━',
    //   //barGlue: '╸\u001b[33m',
    //   barGlue: '╸' + styles.gray.open

    //   // barGlue: '╸\u001b[30m'
    //   // barCompleteChar: chalk.cyan('╸'), //'\u2588',
    //   // barIncompleteChar: chalk.gray('━'),
    // });
    // prog.start(3, 1);

    const regionPrompt = new AutoComplete({
      name: "awsRegion",
      message: "Which AWS Region to deploy to?",
      limit: 4,
      initial: process.env.AWS_DEFAULT_REGION ?? undefined,
      choices: AWS_REGIONS,
    }).run();
    const getAwsAcctIdPromise = getAwsAcctId(awsProfile);
    // CliUx.ux.url("Feel free to read about the Matano directory here.", "https://www.matano.dev/docs");

    const [awsRegion, maybeDefaultAwsAccountId] = await Promise.all([regionPrompt, getAwsAcctIdPromise]);

    const { awsAccountId } = await prompt<any>({
      type: "input",
      name: "awsAccountId",
      validate(value) {
        return (value.length == 12 && !!+value) || "Invalid AWS account ID.";
      },
      message: "What is the AWS Account ID to deploy to?",
      initial: maybeDefaultAwsAccountId ?? undefined,
    });

    const hasExistingMatanoDirectory = await new Confirm({
      name: "shouldCreateMatanoDirectory",
      message: "Do you have an existing matano directory?",
      initial: false,
    }).run();

    let matanoUserDirectory: string;

    if (!hasExistingMatanoDirectory) {
      this.log(chalk.dim("  I will generate a Matano directory in the current directory."));
      const { directoryName } = await prompt<any>({
        type: "input",
        name: "directoryName",
        message: "What is the name of the directory to generate?" + chalk.gray("(use . for current directory)"),
        initial: ".",
      });
      GenerateMatanoDir.generateMatanoDirectory(directoryName, awsAccountId, awsRegion);
      matanoUserDirectory = path.resolve(directoryName);
      this.log(chalk.green("✔") + ` Generated Matano directory at ${matanoUserDirectory}.`);
    } else {
      const { directoryPath } = await prompt<any>({
        type: "input",
        name: "directoryPath",
        message: "What is the path to your existing Matano directory?",
      });
      matanoUserDirectory = path.resolve(directoryPath);
      this.log(chalk.dim("✔") + chalk.dim(` Using Matano directory at ${matanoUserDirectory}.`));
    }
    return { matanoUserDirectory, awsRegion, awsAccountId };
  }
}
