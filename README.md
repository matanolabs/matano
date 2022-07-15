<div align="center">
  <a href="https://matano.dev">
    <img src="https://arhivach.ng/storage/0/19/0191ea69f1a425848e8fdf6ac6a72732.jpg" width="100">
  </a>
  <br>
</div>

<h1 align="center">
  <img src="website/src/assets/cover.png" width="500">
</h1>

<p style="font-size:22px;" align="center">
The open-source security lake platform for AWS.
</p>

<div align="center">
    <img src="https://img.shields.io/badge/Deploys%20to-AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white&labelColor=232F3E"/>
    <br>
    <img src="https://img.shields.io/github/license/matanolabs/matano?style=flat"/>
    <!-- <img src="https://img.shields.io/github/v/release/matanolabs/matano?style=flat-square"/> -->
    <a href="https://discord.gg/YSYfHMbfZQ" target="_blank"><img src="https://img.shields.io/discord/996484553290022973?logo=discord&style=flat-square"/></a>
    <a href="http://twitter.com/intent/tweet?url=https%3A%2F%2Fgithub.com%2Fmatanolabs%2Fmatano" target="_blank"><img src="https://img.shields.io/twitter/url?style=social&url=https%3A%2F%2Fgithub.com%2Fmatanolabs%2Fmatano" alt="Twitter"/></a>

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

## Installing

### Requirements

git, node>=12 and npm are required.

### Installation script

```bash
curl -sS https://raw.githubusercontent.com/matanolabs/matano/main/install.sh | bash
```

Matano will be installed by default in `"$HOME/.matano"`. You can configure this using the `--install-dir` option.

### From source

You can also manually install from source.

```bash
git clone https://github.com/matanolabs/matano.git
make install
```

You can update matano using `matano update`.

## License

* [Apache-2.0 License](LICENSE)
