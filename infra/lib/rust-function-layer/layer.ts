import { AssetHashType, DockerImage, FileSystem } from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { FunctionOptions, ILayerVersion, LayerVersion, Runtime } from 'aws-cdk-lib/aws-lambda';
import { execSync } from 'child_process';
import { Construct } from 'constructs';
import * as crypto from 'crypto';
import * as path from 'path';
import { Settings } from '.';
import { MatanoConfiguration } from '../MatanoStack';
import { BaseBuildProps, build, BuildOptions } from './build';
import { LAMBDA_TARGETS } from './build';
import {
    createDirectory,
    getPackageName,
    lambdaArchitecture,
} from './utils';

/**
 * Properties for a RustFunction
 */
export interface RustFunctionProps
    extends Partial<lambda.LayerVersionProps>,
    BaseBuildProps {
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

/**
 * A Rust Lambda function built using cross
 */
export class RustFunctionLayer extends Construct {
    layer: ILayerVersion;
    environmentVariables: FunctionOptions["environment"];
    image: DockerImage;

    constructor(
        scope: Construct,
        id: string,
        props: RustFunctionProps
    ) {
        super(scope, id);
        this.environmentVariables ??= {}
        const entry = props.directory || Settings.ENTRY;
        const target =
            <LAMBDA_TARGETS>props.target || Settings.TARGET;
        const arch = lambdaArchitecture(target);

        const binName = undefined;
        const packageName = props.package;

        if (props.setupLogging) {
            // Need to use the *underscore*- separated variant, which is
            // coincidentally how Rust imports are done.
            let underscoredName = packageName.split('-').join('_');
            // Set the `RUST_LOG` environment variable.
            this.environmentVariables.RUST_LOG = `${Settings.DEFAULT_LOG_LEVEL},${underscoredName}=${Settings.MODULE_LOG_LEVEL}`;
        }

        const baseBuildProps: BuildOptions = {
            ...props,
            entry,
            target: target,
            outDir: '/asset-output',
        }

        const rustFunctionLayerScriptsPath = path.resolve(path.join('./lib/rust-function-layer'));
        const lambdasDir = path.resolve(path.join("../lib/rust"));
        this.image = DockerImage.fromBuild(rustFunctionLayerScriptsPath);
        this.layer = new LayerVersion(this, id, {
                ...props,
                compatibleArchitectures: [arch],
                code: lambda.Code.fromAsset(lambdasDir, {
                    assetHashType: AssetHashType.SOURCE,
                    bundling: {
                        image: this.image,
                        volumes: [
                            {

                                hostPath: path.resolve(path.join("../lib/rust")),
                                containerPath: "/.cache"
                            },
                            {
                                hostPath: rustFunctionLayerScriptsPath,
                                containerPath: "/asset-input/scripts",
                            },
                        ],
                        command: [
                            "bash",
                            "-c",
                            [
                                // `mkdir -p /.cache`,
                                `ts-node -T -e "import { build } from './scripts/build'; build(JSON.parse(Buffer.from('${Buffer.from(JSON.stringify(baseBuildProps)).toString('base64')}', 'base64').toString('ascii')));"`,
                                `cp -a /asset-output/${packageName}/* /asset-output`,
                                `rm -rf /asset-output/${packageName}`
                            ].join(" && "),
                        ],
                        local: {
                            tryBundle(outputDir, options) {
                                if (!process.env.MATANO_LOCAL_DEV) return false;
                                execSync(`bash -c "cargo lambda build --release --target x86_64-unknown-linux-gnu --quiet --color always --package ${packageName}" && cp target/lambda/${packageName}/bootstrap ${outputDir}`, {
                                    cwd: lambdasDir,
                                });
                                return true;
                            },
                        }
                    },
                }),
            });
    }
}