---
sidebar_position: 2
title: Installation
---

Matano always runs in your AWS account. You use the Matano CLI to deploy Matano and manage your installation.

See [Deployment](#) for more on deploying Matano.

## Installing the matano CLI
You can install the Matano CLI from source using the following commands:

```bash
git clone https://github.com/matanolabs/matano.git
cd matano && make install
```

## Updating the matano CLI

To update an existing installation of the Matano CLI, use `git` to pull the desired version and install.

```bash
git pull --rebase origin main
make install
```
