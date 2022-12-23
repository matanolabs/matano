# Development

This guide contains relevant and useful information if you are developing on the Matano project.

## Overview

Matano is a monorepo codebase with code in several languages (Rust, Kotlin, Typescript, Python).

## Prerequisites / Dependencies

Since Matano is regularly distributed as a prebuilt binary, you may need to ensure several dependencies are installed if you are developing on the source code:

- Rust 1.63.0
- Cargo Lambda (Run `pip install cargo-lambda`)
- Java 11
- Python 3.9
- Node JS >14
- Docker

This is the complete set of requirements. Of course, if you are only working on, for example, Rust code you may not need Python installed, or if you are only working on adding a managed log source parser, you may not need any dependencies at all.

## Components

The major components of the codebase are:

- `cli/` contains the codebase for the Matano CLI. The CLI is written in NodeJS. The CLI is distributed as a single binary using `vercel/pkg`, from the script in `scripts/`.
- `data/` contains the ECS Iceberg schema ands the definitions of managed log sources and managed enrichment sources.
- `infra/` contains the CDK project that defines the infrastructure for Matano.
- `lib/` contains the actual application code of Matano. There are subdirectories for each programming language (e.g. `lib/rust`, `lib/java`, `lib/python`, etc.)
- `scripts/` contains relevant scripts for Matano. `scripts/packaging` defines the script to build the Matano CLI into a single distributable binary.

## Developing locally

You can set up the project to be able to develop and test the Matano CLI, infrastucture, and application code locally. To get started run:

```bash
make build-all
make local-install
```

This will build all the Matano packages and install the matano CLI locally using Matano (watch out for path conflicts if you have the Matano CLI installed from the published binary).


### Relevant commands

Subsequently, you can use the Matano CLI as usual. If you are making code changes, run the relevant commands to rebuild based on what you are changing:

#### Infrastructure

```bash
make build-infra
```

#### CLI

```bash
make build-cli
```

#### Rust

```bash
make build-rust
```

#### Python

```bash
make build-python
```

#### JVM/Kotlin

```bash
make build-jvm
```

### IDEs

Of course, you can use any IDE you wish. Here are tips that have worked well in our experience:

- Use Visual Studio Code for Typescript development (CLI, Infrastructure). You can open the root or subdirectories in VS Code.
- Use IntelliJ IDEA for JVM development. Make sure you open the `lib/java/matano` directory in IDEA for the project to be recognized.
- Use Visual Studio Code for Rust development. Make sure you open the `lib/rust` workspace directory in a separate window as Rust language analysis will not work from the root project directory.

## Developing managed log sources

First, follow the instructions above on [developing locally](#developing-locally).

Managed log sources are defined in the `data/managed/log_sources/` subdirectory. You can take a look at existing examples for the structure. In short, you will need to
- Create a new subdirectory under `data/managed/log_sources/` with the name of the managed log source.
- Add the schema and transform definition as if you were building the transform yourself.
- To test, create a log source in your Matano directory with `managed.type` set to your managed log source name.
- Send test data to your log source and see if the output data is working correctly.

You can also test VRL transforms locally to avoid having to deploy. Some resources:

- [VRL Web](https://playground.vrl.dev/)
- [Vector VRL CLI Repl](https://vector.dev/docs/setup/installation/). Install and run `vector vrl`.

TODO: @shaeqahmed add more info.

## Formatting

Since we have many different languages, we use [Trunk](https://docs.trunk.io/docs/overview) to have a single command for formatting. See [here](https://docs.trunk.io/docs/install) for how to install the trunk CLI.

If you are contributing, make sure to run `trunk fmt` before making a PR.

## Anything else

If you discover anything confusing or missing while developing, feel free to:

- Join our [Discord](https://discord.gg/YSYfHMbfZQ) and ask the community/team any questions.
- Create an issue or PR to improve these docs so others can benefit too.
