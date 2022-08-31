#!/usr/bin/env ts-node

import { spawnSync } from 'child_process';
import * as fs from 'fs';
import { performance } from 'perf_hooks';

import * as path from 'path';

export function logTime(start: number, message: string) {
    const elapsedSec = ((performance.now() - start) / 1000).toFixed(
        2
    );
    console.log(`${message}: ${elapsedSec}s`);
}

export function deleteFile(filePath: string, force = true) {
    if (fs.existsSync(filePath)) {
        // @ts-ignore: TS2339
        fs.rmSync(filePath, {
            force: force,
        });
    }
}


// These are the valid cross-compile targets for AWS Lambda.
//
// See also: <https://github.com/awslabs/aws-lambda-rust-runtime/discussions/306#discussioncomment-485478>.
export type LAMBDA_TARGETS =
    // For Arm64 Lambda functions
    | 'aarch64-unknown-linux-gnu'
    // For x86_64 Lambda functions
    | 'x86_64-unknown-linux-gnu'
    | 'x86_64-unknown-linux-musl';

/**
 * Contains common settings and default values.
 */
export const Settings = {
    /**
     * Entry point where the project's main `Cargo.toml` is located. By
     * default, the construct will use directory where `cdk` was invoked as
     * the directory where Cargo files are located.
     */
    ENTRY: '/asset-input',

    /**
     * Default Build directory, which defaults to a `.build` folder under the
     * project's root directory.
     */
    BUILD_DIR: path.join('/tmp', '.build'),

    /**
     * Build target to cross-compile to. Defaults to the target for Amazon
     * Linux 2, as recommended in the [official AWS documentation].
     *
     * [official AWS documentation]: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/lambda.html
     */
    TARGET: 'x86_64-unknown-linux-gnu' as LAMBDA_TARGETS,

    /**
     * Whether to build each executable individually, either via `--bin` or
     * `--package`.
     *
     * If not specified, the default behavior is to build all executables at
     * once, via `--bins` or `--workspace`, as generally this is the more
     * efficient approach.
     */
    BUILD_INDIVIDUALLY: true,

    /**
     * Whether to run `cargo check` to validate Rust code before building it with `cross`.
     *
     * Defaults to true.
     */
    RUN_CARGO_CHECK: true,

    /**
     * Default Log Level, for non-module libraries.
     *
     * Note: this value is only used when `RustFunctionProps.setupLogging`
     * is enabled.
     */
    DEFAULT_LOG_LEVEL: 'warn',

    /**
     * Default Log Level for a module (i.e. the executable)
     *
     * Note: this value is only used when `RustFunctionProps.setupLogging`
     * is enabled.
     */
    MODULE_LOG_LEVEL: 'info',

    /**
     * A list of features to activate when compiling Rust code.
     *
     * @default - No enabled features.
     */
    FEATURES: undefined as string[] | undefined,

    /**
     * Key-value pairs that are passed in at compile time, i.e. to `cargo
     * build` or `cross build`.
     *
     * Use environment variables to apply configuration changes, such
     * as test and production environment configurations, without changing your
     * Lambda function source code.
     *
     * @default - No environment variables.
     */
    BUILD_ENVIRONMENT: undefined as NodeJS.ProcessEnv | undefined,

    /**
     * Additional arguments that are passed in at build time to both
     * `cargo check` and `cross build`.
     *
     * ## Examples
     *
     * - `--all-features`
     * - `--no-default-features`
     */
    EXTRA_BUILD_ARGS: undefined as string[] | undefined,

    /**
     * Sets the root workspace directory. By default, the workspace directory
     * is assumed to be the directory where `cdk` was invoked.
     *
     * This directory should contain at the minimum a `Cargo.toml` file which
     * defines the workspace members. Sample contents of this file are shown:
     *
     *   ```toml
     *   [workspace]
     *   members = ["lambda1", "lambda2"]
     *   ```
     *
     */
    set WORKSPACE_DIR(folder: string) {
        this.ENTRY = path.join(this.ENTRY, folder);
    },
};


// Set the base Cargo workspace directory
Settings.WORKSPACE_DIR = 'my_lambdas';

Settings.BUILD_ENVIRONMENT = {
};

// Uncomment if you want to build (e.g. cross-compile) each target, or
// workspace member, individually.
Settings.BUILD_INDIVIDUALLY = true;

// Uncomment to cross-compile Rust code to a different Lambda architecture.
// Settings.TARGET = 'x86_64-unknown-linux-gnu';

let _builtWorkspaces = false,
    _builtBinaries = false,
    _ranCargoCheck = false;

export interface BaseBuildProps {
    /**
     * Workspace package name to pass to `--package`
     */
    readonly package: string;

    /**
     * The target to use `cross` to compile to.
     *
     * Normally you'll need to first add the target to your toolchain:
     *    $ rustup target add <target>
     *
     * The target defaults to `x86_64-unknown-linux-gnu` if not passed.
     */
    readonly target?: LAMBDA_TARGETS | undefined;

    /**
     * A list of features to activate when compiling Rust code.
     */
    readonly features?: string[];

    /**
     * Key-value pairs that are passed in at compile time, i.e. to `cargo
     * build` or `cross build`.
     *
     * Use environment variables to apply configuration changes, such
     * as test and production environment configurations, without changing your
     * Lambda function source code.
     *
     * @default - No environment variables.
     */
    readonly buildEnvironment?: NodeJS.ProcessEnv;

    /**
     * Additional arguments that are passed in at build time to both
     * `cargo check` and `cross build`.
     *
     * ## Examples
     *
     * - `--all-features`
     * - `--no-default-features`
     */
    readonly extraBuildArgs?: string[];
}

/**
 * Build options
 */
export interface BuildOptions extends BaseBuildProps {
    /**
     * Entry file
     */
    readonly entry: string;

    /**
     * The output directory
     */
    readonly outDir: string;

    // Makes this property *required*
    readonly target: LAMBDA_TARGETS;
}

/**
 * Build with Cross
 */
export function build(options: BuildOptions): void {
    // try {
        let outputName: string;
        let shouldCompile: boolean;
        let extra_args: string[] | undefined;


        // Build package - i.e. workspace member
        outputName = options.package;
        if (_builtWorkspaces) {
            shouldCompile = false;
            extra_args = undefined;
        } else if (Settings.BUILD_INDIVIDUALLY) {
            shouldCompile = true;
            extra_args = ['--package', outputName];
        } else {
            _builtWorkspaces = true;
            shouldCompile = true;
            extra_args = ['--workspace'];
        }

        if (shouldCompile) {
            // Check if directory `./target/{{target}}/release` exists
            const releaseDirExists = fs.existsSync(options.outDir);

            // Base arguments for `cargo lambda`

            const buildArgs = ['--quiet', '--color', 'always'];

            let extraBuildArgs =
                options.extraBuildArgs || Settings.EXTRA_BUILD_ARGS;
            let features = options.features || Settings.FEATURES;

            if (extraBuildArgs) {
                buildArgs.push(...extraBuildArgs);
            }
            if (features) {
                buildArgs.push('--features', features.join(','));
            }

            // Set process environment (optional)
            let inputEnv =
                options.buildEnvironment ||
                Settings.BUILD_ENVIRONMENT;
            const buildEnv = inputEnv
                ? {
                      ...process.env,
                      ...inputEnv,
                  }
                : undefined;

            if (releaseDirExists) {
                console.log(`🍺  Building Rust code...`);
            } else {
                // The `release` directory doesn't exist for the specified
                // target. This is most likely an initial run, so `cargo lambda`
                // will take much longer than usual to cross-compile the code.
                //
                // Print out an informative message that the `build` step is
                // expected to take longer than usual.
                console.log(
                    `🍺  Building Rust code with \`cargo lambda\`. This may take a few minutes...`
                );
            }

            const args: string[] = [
                'lambda',
                'build',
                '--lambda-dir',
                options.outDir,
                '--release',
                '--target',
                options.target,
                ...buildArgs,
                ...extra_args!,
            ];

            console.log('cargo '+args.join(' '))

            const cargo = spawnSync('cargo', args, {
                // cwd: options.entry,
                // env: buildEnv,
                shell: true
            });

            if (cargo.status !== 0) {
                console.error(cargo.stderr?.toString().trim());
                console.error(`💥  Run \`cargo lambda\` errored.`);
                process.exit(1);
                // Note: I don't want to raise an error here, as that will clutter the
                // output with the stack trace here. But maybe, there's a way to
                // suppress that?
                // throw new Error(cargo.stderr.toString().trim());
            }
        }
    // } catch (err) {
    //     throw new Error(
    //         `Failed to build file at ${options.entry}: ${err}`
    //     );
    // }
}