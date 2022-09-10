---
sidebar_position: 3
title: Getting started
---

## Deployment

To get started with Matano, run the `matano init` command. Make sure you have AWS credentials in your environment (or in an AWS CLI profile).

The interactive CLI wizard will walk you through getting started by generating an initial [Matano directory](./matano-directory.md) for you, initializing your AWS account, and deploying Matano into your AWS account.

Initial deployment takes a few minutes.

### Retrieving resource values

Matano creates several resources, such as S3 buckets and SQS queues, that you may need to interact with. Use the `matano info` CLI command to retrieve these values.
