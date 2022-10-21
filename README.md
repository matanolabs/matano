<div align="center">
    <a href="https://www.matano.dev"><img src="website/src/assets/cover.svg" width=500></a>
    <h3>The open-source security lake platform for AWS.</h3>
</div>

<p align="center">
        <a href="#"><img src="https://img.shields.io/badge/Deploys%20to-AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white&labelColor=232F3E"/></a>
        <a href="#"><img src="https://img.shields.io/badge/rust-%233A3B3C.svg?style=for-the-badge&logo=rust&labelColor=B1513E&logoColor=white"/></a>
        <br>
        <a href="/LICENSE" target="_blank"><img src="https://img.shields.io/github/license/matanolabs/matano?style=flat"/></a>
        <a href="https://bestpractices.coreinfrastructure.org/projects/6478"><img src="https://bestpractices.coreinfrastructure.org/projects/6478/badge"></a>
        <a href="https://trunk.io"><img src="https://img.shields.io/badge/trunk.io-enabled-brightgreen?logo=data:image/svg%2bxml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGZpbGw9Im5vbmUiIHN0cm9rZT0iI0ZGRiIgc3Ryb2tlLXdpZHRoPSIxMSIgdmlld0JveD0iMCAwIDEwMSAxMDEiPjxwYXRoIGQ9Ik01MC41IDk1LjVhNDUgNDUgMCAxIDAtNDUtNDVtNDUtMzBhMzAgMzAgMCAwIDAtMzAgMzBtNDUgMGExNSAxNSAwIDAgMC0zMCAwIi8+PC9zdmc+"></a>
        <!-- <img src="https://img.shields.io/github/v/release/matanolabs/matano?style=flat-square"/> -->
 <a href="https://discord.gg/YSYfHMbfZQ" target="_blank"><img src="https://img.shields.io/discord/996484553290022973.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2"/></a>
        <a href="https://twitter.com/intent/follow?screen_name=matanolabs" target="_blank"><img src="https://img.shields.io/twitter/follow/matanolabs?style=social" alt="Twitter Follow"/></a>
</p>

<h3 align="center">
        <a href="https://www.matano.dev">Website</a>
        <span> | </span>
        <a href="https://www.matano.dev/docs">Docs</a>
        <span> | </span>
        <a href="https://discord.gg/YSYfHMbfZQ">Community</a>
</h3>

<div align="center">
   <h3>
     <a href="https://www.matano.dev/blog/2022/08/11/announcing-matano?utm_source=ghr">🔔 Read our announcement blog post 🔔</a>
   </h3>
</div>

## What is Matano?

Matano is an open source security lake platform for AWS. It lets you ingest petabytes of security and log data from various sources, store and query them in an open Apache Iceberg data lake, and create Python detections as code for realtime alerting. Matano is _fully serverless_ and designed specifically for AWS and focuses on enabling high scale, low cost, and zero-ops. Matano deploys fully into your AWS account.

<div align="center">
  <br>
  <img src="website/src/assets/diagram.png" width="1000">
</div>

## Features

#### Collect data from all your sources

Matano lets you collect log data from sources using [S3](#) or SQS based ingestion.

#### Ingest, transform, normalize log data

Matano normalizes and transforms your data using [Vector Remap Language (VRL)](https://vector.dev/docs/reference/vrl/). Matano works with the [Elastic Common Schema (ECS)](https://www.elastic.co/guide/en/ecs/current/index.html) by default and you can define your own schema.

#### Store data in S3 object storage

Log data is always stored in highly optimized Parquet files in S3 object storage, for cost effective, long term, durable storage.

#### Apache Iceberg Data lake

All data is ingested into an Apache Iceberg based data lake. Apache Iceberg is an open table format, so you always **own your own data**, with no vendor lock-in.

#### Serverless

Matano is a fully serverless platform, designed for zero-ops and unlimited elastic horizontal scaling.

#### Detections as code

Write Python detections to implement realtime alerting on your log data.

## Installing

[**View the complete installation instructions.**](https://www.matano.dev/docs/installation)

You can install the matano CLI to deploy Matano into your AWS account, and manage your Matano deployment.

### Requirements

- Docker

### Installation

Matano provides [a nightly release](https://github.com/matanolabs/matano/releases/tag/nightly) with the latest prebuilt files to install the Matano CLI on GitHub. You can download and execute these files to install Matano.

For example, to install the Matano CLI for Linux, run:

```bash
curl -OL https://github.com/matanolabs/matano/releases/download/nightly/matano-linux-x64.sh
chmod +x matano-linux-x64.sh
sudo ./matano-linux-x64.sh
```

## Getting started

[**Read the complete docs on getting started**](https://www.matano.dev/docs/getting-started).

### Deployment

To get started with Matano, run the `matano init` command. Make sure you have AWS credentials in your environment (or in an AWS CLI profile).

The interactive CLI wizard will walk you through getting started by generating an initial [Matano directory](https://www.matano.dev/docs/matano-directory) for you, initializing your AWS account, and deploying Matano into your AWS account.

<div align="center">
  <img src="website/src/assets/matano-init.gif" width="750">
</div>

Initial deployment takes a few minutes.

## Documentation

[**View the complete documentation.**](https://www.matano.dev/docs)

## Ingestion

[**Read the complete docs on ingestion**](https://www.matano.dev/docs/log-sources/ingestion).

You can ingest data from any log source using Matano. To ingest a log source into Matano, you can use either S3 or SQS ingestion.

### S3 Ingestion

When you use S3 ingestion, Matano ingests data from your log source using an S3 bucket. You can either use a Matano provided bucket or bring your own bucket if you have existing data.

#### Using the Matano provided sources bucket

Matano creates a managed S3 bucket for you to use for S3 ingestion. You can use this bucket to ingest data into Matano.

When sending data to the Matano provided sources bucket, upload files to the `/data/<log_source_name>` prefix where `log_source_name` is the name of your log source that you specified in your `log_source.yml` file.

#### Bringing your own bucket

If you have existing data, or need to have your raw data in a specific location, you can configure Matano to ingest data from a provided S3 location.

In your `log_source.yml`, specify the S3 Bucket and object prefix that your data is located at:

```yml
ingest:
  s3_source:
    bucket_name: "my-org-logs-bucket"
    key_prefix: "data/mypath"
```

## Matano managed log sources

[**Read the complete docs on Matano managed log sources**](https://matano.dev/docs/log-sources/managed-log-sources).

Matano managed log sources are common log sources for which Matano provides preconfigured normalizations, transformations, and schemas. This lets you easily ingest logs from a supported log source without having to write a transformation or specify a schema.

### Supported managed log sources

The following are currently supported Matano managed log sources:

- [**AWS CloudTrail**](https://www.matano.dev/docs/log-sources/managed-log-sources/cloudtrail)

### Using managed log sources

To use a Matano managed log source, specify the `managed.type` property in your `log_source.yml` with the corresponding identifier for the managed log source.

For example, to use the CloudTrail managed log source, your `log_source.yml` may look as follows:

```yml
name: "aws_cloudtrail"

managed:
  type: "AWS_CLOUDTRAIL"
```

## Transformation

[**Read the complete docs on transformation**](https://www.matano.dev/docs/log-sources/transformation).

Matano allows you to flexibly transform your data into a normalized form. The transformation is specified using the [Vector ReMap Language (VRL)](https://vector.dev/docs/reference/vrl/).

If you're using a [Matano managed log source](https://matano.dev/docs/log-sources/managed-log-sources), a transformation will be applied for you and you don't need to write a VRL expression to transform your data.

### Writing Transformations

To apply a transformation to your log source, specify a VRL expression to transform your data as a string in the `transform` key in your `log_source.yml` file. Here's a very small example:

```yml
transform: |
  _date, err = to_timestamp(.json.eventTime)
  if err == null {
      .ts = to_unix_timestamp(_date, "milliseconds")
  }
```

VRL contains many functions that allow you to concisely express your transformation logic.

## Schema

[**Read the complete docs on schemas**](https://www.matano.dev/docs/log-sources/schema).

You define the shape of your log source's data by defining a _schema_.

### Common schema

By default, a log source will use the [**Elastic Common Schema (ECS)**](https://www.elastic.co/guide/en/ecs/current/ecs-reference.html). ECS is a popular format that defines a common set of fields to be used when storing log data.

Matano encourages you to normalize your log data to ECS, so you can best analyze, and correlate the data represented in your events.

### Defining a schema

If you aren't using a Matano managed log source, you must provide the schema for your log source.

### Specifying ECS fields

You can specify the subset of ECS fields to include in your schema by using the `ecs_field_names` key in your `log_source.yml` file.

```yml
schema:
  ecs_field_names: ["dns", "agent"]
```

### Extending ECS with custom fields

ECS is a permissive schema, that encourages you to add columns using custom field names for data that doesn't completely map to ECS.

To add custom fields for a Matano log source, you can define the fields in your `log_source.yml` configuration file:

```yml
schema:
  ecs_field_names: ["dns", "agent"]
  fields:
    - name: "my_additional_field"
      type: "string"
    - name: "additional_struct"
      type:
        type: "struct"
        fields:
          - name: "field_one"
            type: "string"
          - name: "field_two"
            type: "int"
```

These fields will be merged with ECS fields in the final log source schema.

The schema configuration follows the [Apache Iceberg schema format](https://iceberg.apache.org/spec/#schemas) in JSON/YAML format.

## Detections

[**Read the complete docs on detections**](https://www.matano.dev/docs/detections).

To react to your security data in realtime in Matano, you work with detections. Matano lets you define _detections as code_ (DaaC). A _detection_ is a Python program that is invoked with data from a log source in realtime and can create an _alert_.

To create a detection, you configure the Matano table(s) that you want the detection to respond to and author Python code that processes records from the log source and, if the data matches whatever condition you express in your Python code, an alert is created in realtime.

### Realtime Python detections

_Detection scripts_ are Python programs containing the logic of your detection. To create a detection script, create a file called `detect.py` in your detection directory.

The `detect` function is the python function that is invoked for your detection. The function will be invoked with a data record.

The function has the following signature:

```python
def detect(record) -> bool | None:
    ...
```

Your `detect` function must return a boolean `True` to signal an alert. A return value of `False` or `None` will be interpreted as no alert for detection on that record.

#### Title function

You can implement a `title` function to format the title if an alert is created using Python.

```python
def title(record) -> str
  user_name = record.get("user", {}).get("name")
  return f"{user_name} - Elevated login failures"
```

#### Dedupe function

You can implement a `dedupe` function to return a _dedupe string_ that will be used to group rule matches into alerts.

```python
def dedupe(record) -> str
  return record.get("user", {}).get("name")
```

#### Example Python detection

Here is a sample Python detection. It runs on AWS CloudTrail logs and detects a failed attempt to export an AWS EC2 instance.

```python
def detect(record):
  return (
    record.get("event", {}).get("action")
    == "CreateInstanceExportTask"
    and record.get("event", {}).get("provider")
    == "ec2.amazonaws.com"
    and event.outcome == "failure
  )
```

#### Remote cache

[**Read the complete docs on the remote cache**](https://www.matano.dev/docs/detections/remote-cache).

You can use a _remote cache_ backed by DynamoDB in your detections to persist and retrieve values by a key. This gives you a powerful way to be able to retain context and perform correlation across your detections.

The remote cache implements `dict`-like methods, so in your Python code, you can treat it like a Python dictionary.

**Example**

The following is an example detection using the remote cache with a string set.

```python
from detection import remotecache

# a weekly cache
users_ips = remotecache("user_ip", ttl=86400 * 7)

def detect(record):
    if record.get('event', {}).get('action') == 'ConsoleLogin' and
        record.get('event', {}).get('outcome', {}) == 'success':
        # A unique key on the user name
        key = record.get("user", {}).get("name")

        # Alert on new IP
        user_ips = users_ips.add_to_string_set(key)
        if len(user_ips) > 1:
            del users_ips[key]
            return True
```

## Alerting

[**Read the complete docs on alerting**](https://www.matano.dev/docs/detections/alerting).

### Deduplicating alerts

Matano lets you deduplicate common alerts to reduce alert fatigue.

#### Dedupe string

You can return a _dedupe string_ from your detection. Rule matches with the same dedupe will be grouped together.

To return a dedupe string from your detection, create a `dedupe` python function and return the dedupe string. The `dedupe` function will be passed the record being detected on, so you can dynamically create the dedupe string.

```python
def dedupe(record) -> str:
    ...
```

#### Deduplication window

You can use a deduplication window to add rule matches to an existing alert within a time duration. During this window, rule matches will not create new alerts but instead be appended to the existing alert for the detection (and dedupe).

You can specify a max deduplication window of **1 day (86400 seconds)**.

You can configure a deduplication window per detection by using the `alert.deduplication_window` key in your `detection.yml`. Specify the value in **seconds**.

```yml
alert:
  deduplication_window: 21600
```

### Alert threshold

The alert threshold specifies how many rule matches are needed to create an alert. For example, if you set the alert threshold to 10, ten rule matches will be required within the deduplication window for an alert to be created.

You can configure an alert threshold per detection by using the `alert.threshold` key in your `detection.yml`.

```yml
alert:
  threshold: 10
```

### Working with alerts

#### Alerts Matano table

All alerts are automatically stored in a Matano table named `matano_alerts`. The alerts and rule matches are normalized to ECS and contain context about the original event that triggered the rule match, along with the alert and rule data.

**Example Queries**

_View alerts that breached threshold_

```sql
select matano.alert.id as alert_id,
    count(matano.alert.rule.match.id) as rule_match_count,
    array_agg(matano.alert.rule.match.id) as rule_matches,
from matano_alerts
    where
        ts < current_timestamp - interval '1' hour
        and matano.alert.breached = true
    group by matano.alert.id, matano.alert.dedupe
```

#### Delivering alerts

Matano allows you to deliver alerts to external systems. You can use the Matano alerting SNS topic to deliver alerts to Email, Slack, and other services.

## License

- [Apache-2.0 License](LICENSE)

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=03c989f6-90f5-4982-b002-a48635f10b5d"/>
