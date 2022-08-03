<div align="center">
  <a href="https://matano.dev"><img src="website/src/assets/cover.png" width="500"></a>
</div>

<!-- <h1 align="center">Matano</h1> -->

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=03c989f6-90f5-4982-b002-a48635f10b5d" />

<div align="center">
  <h3>The open-source security lake platform for AWS.</h3>
</div>

<div align="center">
    <a href="#"><img src="https://img.shields.io/badge/Deploys%20to-AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white&labelColor=232F3E"/></a>
    <br>
    <a href="/LICENSE" target="_blank"><img src="https://img.shields.io/github/license/matanolabs/matano?style=flat"/></a>
    <!-- <img src="https://img.shields.io/github/v/release/matanolabs/matano?style=flat-square"/> -->
    <a href="https://discord.gg/YSYfHMbfZQ" target="_blank"><img src="https://img.shields.io/discord/996484553290022973?logo=discord&style=flat-square"/></a>
    <a href="https://twitter.com/intent/follow?screen_name=matanolabs" target="_blank"><img src="https://img.shields.io/twitter/follow/matanolabs?style=social" alt="Twitter Follow"/></a>
</div>

<div align="center">
    <h3 align="center">
        <a href="https://www.matano.dev">Website</a>
        <span> | </span>
        <a href="https://matano.dev/docs">Docs</a>
        <span> | </span>
        <a href="https://discord.gg/YSYfHMbfZQ">Community</a>
    </h3>
</div>

## What is Matano?

Matano is an open source security lake platform for AWS. It lets you ingest petabytes of security and log data from various sources, store and query them in a data lake, and create Python detections as code for realtime alerting. Matano is *fully serverless* and designed specifically for AWS and focuses on enabling high scale, low cost, and zero-ops. Matano deploys fully into your AWS account.

<div align="center">
  <br>
  <img src="website/src/assets/diagram.png" width="1000">
</div>

## Features

#### Collect data from all your sources
Matano lets you collect log data from sources using [S3](#) or Kafka based ingestion.

#### Ingest, transform, normalize log data
Matano normalizes and transforms your data using [Vector Remap Language (VRL)](https://vector.dev/docs/reference/vrl/). Matano works with the [Elastic Common Schema (ECS)](https://www.elastic.co/guide/en/ecs/current/index.html) by default and you can define your own schema. 

#### Store data in S3 object storage
Log data is always stored in S3 object storage, for cost effective, long term, durable storage.

#### Apache Iceberg Data lake
All data is ingested into an Apache Iceberg based data lake, allowing you to perform ACID transactions, time travel, and more on all your log data.

#### Serverless
Matano is a fully serverless platform, designed for zero-ops and unlimited elastic horizontal scaling.

#### Detections as code
Write Python detections to implement realtime alerting on your log data.

## Installing

You can install the matano CLI to deploy Matano into your AWS account, and manage your Matano deployment.

### Requirements

- node>=12 and npm
- Docker

<!-- ### Installation script

```bash
curl -sS https://raw.githubusercontent.com/matanolabs/matano/main/install.sh | bash
```

Matano will be installed by default in `"$HOME/.matano"`. You can configure this using the `--install-dir` option. -->

### From source

You can manually install from source.

```bash
git clone https://github.com/matanolabs/matano.git
make install
```

## Getting started
[**Read the complete docs on getting started**](https://matano.dev/docs/getting-started).

### Create a Matano Directory

A [Matano directory](https://matano.dev/docs/matano-directory) lets you specify data and configuration values for Matano. Run the following to generate a sample directory to get started:

```bash
matano generate:matano-dir
```

This will create a directory with sample detections and log sources.

Fill in your AWS account ID and desired AWS region in the generated `matano.config.yml` file.

```yml
# replace these values
aws_account_id: "012345678901"
aws_region: "us-east-1"
```

The following commands requires valid AWS credentials, either from the environment or the `--profile` flag.

### Bootstrap your AWS account

Initialize your AWS environment before deployment by running `matano bootstrap`. This will create the necessary resources in your AWS account before deploying Matano.

```bash
matano bootstrap [--profile AWS_CLI_PROFILE]
```

Follow the CLI prompts to ensure your AWS account is ready for deployment.

### Deploy Matano

You can now deploy Matano to your AWS account. Run the following command from your Matano directory:

```bash
matano deploy [--profile AWS_CLI_PROFILE]
```

Deployment can take up to 15 minutes.

## Documentation

[**View our complete documentation.**](https://matano.dev/docs)

## License

* [Apache-2.0 License](LICENSE)
