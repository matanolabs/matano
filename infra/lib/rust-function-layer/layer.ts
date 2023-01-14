import { AssetHashType, DockerImage, FileSystem } from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { FunctionOptions, ILayerVersion, LayerVersion, Runtime } from "aws-cdk-lib/aws-lambda";
import { execSync } from "child_process";
import { Construct } from "constructs";
import * as crypto from "crypto";
import * as path from "path";
import { Settings } from ".";
import { MatanoConfiguration } from "../MatanoStack";
import { getLocalAsset } from "../utils";
import { BaseBuildProps, build, BuildOptions } from "./build";
import { LAMBDA_TARGETS } from "./build";
import { getPackageName, lambdaArchitecture } from "./utils";

/**
 * Properties for a RustFunction
 */
export interface RustFunctionProps extends Partial<lambda.LayerVersionProps>, BaseBuildProps {
  /**
   * Path to directory with Cargo.toml
   *
   * @default - Directory from where cdk binary is invoked
   */
  readonly directory?: string;

  /**
   * The build directory
   *
   * @default - `.build` in the entry file directory
   */
  readonly buildDir?: string;

  /**
   * The cache directory
   *
   * Parcel uses a filesystem cache for fast rebuilds.
   *
   * @default - `.cache` in the root directory
   */
  readonly cacheDir?: string;

  /**
   * Determines whether we want to set up library logging - i.e. set the
   * `RUST_LOG` environment variable - for the lambda function.
   */
  readonly setupLogging?: boolean;
}

export class RustFunctionCode {
  static assetCode(props: RustFunctionProps) {
    const entry = props.directory || Settings.ENTRY;
    const target = <LAMBDA_TARGETS>props.target || Settings.TARGET;
    const arch = lambdaArchitecture(target);

    const packageName = props.package;

    const baseBuildProps: BuildOptions = {
      ...props,
      entry,
      target: target,
      outDir: "/asset-output",
    };
    return getLocalAsset(packageName);
  }
}

/**
 * A Rust Lambda function built using cross
 */
export class RustFunctionLayer extends Construct {
  layer: ILayerVersion;
  arch: lambda.Architecture;
  environmentVariables: FunctionOptions["environment"];

  constructor(scope: Construct, id: string, props: RustFunctionProps) {
    super(scope, id);
    this.environmentVariables ??= {};
    const entry = props.directory || Settings.ENTRY;
    const target = <LAMBDA_TARGETS>props.target || Settings.TARGET;
    this.arch = lambdaArchitecture(target);

    const packageName = props.package;

    if (props.setupLogging) {
      // Need to use the *underscore*- separated variant, which is
      // coincidentally how Rust imports are done.
      let underscoredName = packageName.split("-").join("_");
      // Set the `RUST_LOG` environment variable.
      this.environmentVariables.RUST_LOG = `${Settings.DEFAULT_LOG_LEVEL},${underscoredName}=${Settings.MODULE_LOG_LEVEL}`;
    }

    this.layer = new LayerVersion(this, id, {
      ...props,
      // compatibleArchitectures: [this.arch], TODO(shaeq): disabled for now not supported by in govcloud
      code: RustFunctionCode.assetCode(props),
    });
  }
}
