---
sidebar_position: 3
title: Getting started
---

## Bootstrapping

Initialize your AWS environment before deployment by running `matano bootstrap`. This will create the necessary resources in your AWS account before deploying Matano.

This command requires valid AWS credentials, either from the environment or the `--profile` flag.

## Create a Matano Directory

A [Matano directory](./matano-directory.md) lets you specify data and configuration values for Matano. Run the following to generate a sample directory to get started:

```
matano generate:matano-dir
```

This will create a directory with sample detections and log sources.

## Deployment

To deploy matano, use the `matano deploy` command.

You must specify the AWS account and region you are deploying to, and the Matano directory you are using.

To update your matano deployment, e.g. after adding a detection or log source, re-run `matano deploy`.
