<div align="center">
  <a href="https://matano.dev"><img src="website/src/assets/cover.png" width="500"></a>
</div>

<div align="center">
  <h3>The open-source security lake platform for AWS.</h3>
</div>

<div align="center">
    <img src="https://img.shields.io/badge/Deploys%20to-AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white&labelColor=232F3E"/>
    <br>
    <img src="https://img.shields.io/github/license/matanolabs/matano?style=flat"/>
    <!-- <img src="https://img.shields.io/github/v/release/matanolabs/matano?style=flat-square"/> -->
    <a href="https://discord.gg/YSYfHMbfZQ" target="_blank"><img src="https://img.shields.io/discord/996484553290022973?logo=discord&style=flat-square"/></a>
    <a href="http://twitter.com/intent/tweet?url=https%3A%2F%2Fgithub.com%2Fmatanolabs%2Fmatano" target="_blank"><img src="https://img.shields.io/twitter/url?style=social&url=https%3A%2F%2Fgithub.com%2Fmatanolabs%2Fmatano" alt="Twitter"/></a>
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

#### Ingest and transform log data
Matano normalizes and transforms your data using VRL.

#### Store data in S3 object storage
Log data is always stored in S3 object storage, for cost effective, long term, durable storage.

#### Apache Iceberg Data lake
All data is ingested into an Apache Iceberg based data lake, allowing you to perform ACID transactions, time travel, and more on all your log data.

#### Serverless
Matano is a fully serverless platform, designed for zero-ops and unlimited elastic horizontal scaling.

#### Detections as code
Write Python detections to implement realtime alerting on your log data.

#### Modern CDK based deployment
Matano deploys under the hood using the AWS CDK, giving you a modern, extensible Infrastructure as Code deployment experience. 

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

## License

* [Apache-2.0 License](LICENSE)
