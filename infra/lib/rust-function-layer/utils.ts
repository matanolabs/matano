import { Architecture } from 'aws-cdk-lib/aws-lambda';
import * as fs from 'fs';
import * as path from 'path';
import { performance } from 'perf_hooks';
import * as toml from 'toml';
import { LAMBDA_TARGETS } from './build';

/**
 * Base layout of a `Cargo.toml` file in a Rust project
 *
 * Note: This is only used when `RustFunctionProps.bin` is not defined.
 */
export interface CargoTomlProps {
    readonly package: {
        name: string;
    };
}



export function createDirectory(dir: string) {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }
}

export function createFile(filePath: string, data: string) {
    if (!fs.existsSync(filePath)) {
        fs.writeFileSync(filePath, data);
    }
}

export function getPackageName(entry: string) {
    const tomlFilePath = path.join(entry, 'Cargo.toml');
    // console.trace(`Parsing TOML file at ${tomlFilePath}`);

    try {
        const contents = fs.readFileSync(tomlFilePath, 'utf8');
        let data: CargoTomlProps = toml.parse(contents);
        return data.package.name;
    } catch (err) {
        throw new Error(
            `Unable to parse package name from \`${tomlFilePath}\`\n` +
                `  ${err}\n` +
                `  Resolution: Pass the executable as the \`bin\` parameter, ` +
                `or as \`package\` for a workspace.`
        );
    }
}

export function lambdaArchitecture(
    target: LAMBDA_TARGETS
): Architecture {
    return target.startsWith('x86_64')
        ? Architecture.X86_64
        : Architecture.ARM_64;
}