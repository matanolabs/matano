---
sidebar_position: 3
title: Getting started
---

## Create a Matano Directory

A [Matano directory](./matano-directory.md) lets you specify data and configuration values for Matano. Run the following to generate a sample directory to get started:

```bash
matano generate:matano-dir my-matano-directory
```

This will create a directory with sample detections and log sources.

Fill in your AWS account ID and desired AWS region in the generated `matano.config.yml` file.

```yml
# replace these values
aws_account_id: "012345678901"
aws_region: "us-east-1"
```

## Bootstrap your AWS account

Initialize your AWS environment before deployment by running `matano bootstrap`. This will create the necessary resources in your AWS account before deploying Matano.

This command requires valid AWS credentials, either from the environment or the `--profile` flag.

```bash
matano bootstrap [--profile AWS_CLI_PROFILE]
```

Follow the CLI prompts to ensure your AWS account is ready for deployment.

## Deploy Matano

You can now deploy Matano to your AWS account. Make sure you have AWS credentials in your environment (or in a an AWS CLI profile) and run the following command from your Matano directory:

```bash
matano deploy [--profile AWS_CLI_PROFILE]
```

Deployment can take up to 15 minutes.

