---
sidebar_position: 2
title: Getting started
---

## Installation

### From source

```bash
git clone https://github.com/matanolabs/matano.git
make install
```

## Bootstrapping

Initialize your AWS environment before deployment by running `matano bootstrap`. This will create the necessary resources in your AWS account before deploying Matano.

This command requires valid AWS credentials, either from the environment or the `--profile` flag.

## Matano Directory

To specify configuration and data for Matano, create a 'Matano directory`. This directory has the following structure:

```
my-directory
├── log_sources/
├── detections/
├── matano.config.yml
└── matano.context.json
```

You should persist this in a version control system like Git.

## Matano configuration file (`matano.config.yml`)

Use this file to specify matano configuration.

## Matano context file (`matano.context.json`)

This file stores environment specific values for your Matano deployment (such as VPC details). Don't edit this file by hand, instead use the `matano refresh-context` command to generate or update it.

## Log sources directory (`log_sources`)

The log sources directory contains definitions and configuration for each log source you ingest into Matano.

The directory has the following format:

```
├── log_sources
│   └── first_log_source
│       └── log_source.yml
```

## Detections directory (`detections`)

The log sources directory contains definitions and configuration for each detection you create.

The directory has the following format:

```
├── detections
│   └── first_detection
│       ├── detect.py
│       ├── requirements.txt
│       └── detection.yml
```

## Deployment

To deploy matano, use the `matano deploy` command.

You must specify the AWS account and region you are deploying to, and the Matano directory you are using.

To update your matano deployment, e.g. after adding a detection or log source, re-run `matano deploy`.
