---
title: Announcing Matano
authors: "samrose"
image: /img/cover.png
---

I'm excited to announce [Matano](/), a new open source project that lets you run a security lake platform directly in your AWS account. Using Matano, security teams on AWS can ingest, normalize, query and detect realtime threats on petabytes of security logs directly in S3.

![](../src/assets/cover.png)

<!-- truncate -->

**TL;DR: Matano is a high-scale, low-cost, serverless platform deployed to your AWS account that lets you ingest logs from any source, transform and normalize them according to a standard schema such as the Elastic Common Schema, store logs in S3 object storage, query them from an Apache Iceberg data lake, and create realtime detections as code using Python.**

## Security meets Big Data

The average organization today deals with a large amount of security data from various cloud sources, such as network traffic logs (Zeek, Suricata, Bro), Firewall logs, CloudTrail, SaaS audit logs and more. Most of the current tools (SIEM) used to work with and analyze this data are a poor fit for data at this scale. For example, some sources of data are high volume enough that it is cost prohibitive to store them in a specialized database like a SIEM. Other tools come with high ops burden and are a pain to maintain and operate.

Our backgrounds are in AWS and Big Data, and we think we need a different approach to security data. Big Data has brought an immense amount of powerful tooling, and open software for dealing with large datasets, but little of this is applied to security data. Additionally, the AWS cloud provides powerful cloud native offerings that offer excellent serverless capabilities.

Matano combines these two in a project that leverages modern data lake concepts and technologies, such as Apache Iceberg, and combines them with powerful cloud native primitives like Amazon Athena to offer a high scale, low cost, serverless security lake platform.

## The security lake platform

Matano is a **security lake platform**, a platform with an open cloud data lake storing all an organization's security data at its core combined with realtime detections and streaming data analytics on top of the data lake.

Here's a sample of what you can do with Matano:

#### Collect data from all your sources

Matano lets you collect log data from sources using [S3](#) or SQS based ingestion.

#### Ingest, transform, normalize log data

Matano normalizes and transforms your data using the flexible [Vector Remap Language (VRL)](https://vector.dev/docs/reference/vrl/). Matano works with the [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html) by default and you can extend the schema.

#### Store data in S3 object storage

Log data is always stored in S3 object storage, for cost effective, long term, durable storage.

#### Apache Iceberg Data lake

All data is ingested into an open Apache Iceberg based data lake, allowing you to query and perform ACID transactions, time travel, and more on all your log data. You can interact with security data using your existing tooling or any software that supports Apache Iceberg.

#### Detections as code

Write detections as code using Python to implement realtime alerting on your log data. You can use the full expressiveness and flexibility of Python for your detection engineering instead of relying on limiting rules and configurations.

Matano is completely serverless, meaning no ops or maintenance. It's cloud-native and simple to deploy, and you can get started in just a few minutes.

## Goodbye to lock-in

Traditional security tooling often results in data and vendor lock-in, where your data is stored in database or system that don't work well with and can't easily be queried from other tools. With Matano, all your security data is stored in Apache Iceberg tables and the open Iceberg table format ensures you retain control over your data and can use it with and query it from other tools that support Iceberg like Spark or Flink.

## Free & open source software

Matano is completely free and open source software (with an [Apache-2.0 license][1]). We're built on many other great open source software projects and we believe Matano is best as free software that you can use as you see fit. We aim to foster an open community, supporting all log sources, formats, and technologies without any lock-in (one of our reasons for using an open table format like Apache Iceberg).

## Up next

Matano is a work in progress. You can install and try out the functionality of Matano today, but we will be working hard on improvements and stabilization in the near future.

You can follow our project [**on GitHub**][2] or join our [community on Discord][3].

[1]: https://github.com/matanolabs/matano/blob/main/LICENSE
[2]: https://github.com/matanolabs/matano
[3]: https://discord.com/invite/YSYfHMbfZQ
