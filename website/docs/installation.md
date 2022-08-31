---
sidebar_position: 2
title: Installation
---

Matano always runs in your AWS account. You use the Matano CLI to deploy Matano and manage your installation.

See [Deployment](#) for more on deploying Matano.

### Requirements

- Docker: The Matano CLI requires Docker to be installed.

## Installing the matano CLI

### Prebuilt installers

Matano provides [a nightly release](https://github.com/matanolabs/matano/releases/tag/nightly) with the latest prebuilt files to install the Matano CLI on GitHub. You can download and execute these files to install Matano.

For example, to install the Matano CLI for Linux, run:

```bash
curl -OL https://github.com/matanolabs/matano/releases/download/nightly/matano-linux-x64.sh
chmod +x matano-linux-x64.sh
sudo ./matano-linux-x64.sh
```

#### Customizing the installation

By default, the installer will install Matano in `/usr/local/matano-cli` and create a symlink in `/usr/local/bin`. Thus by default, the installer will require sudo permissions.

To customize the installation of Matano, use the `--target` and `--bin-dir` options. For a complete details of these options run the installer with the `--help` flag.

### From source

You can build the Matano CLI archive installer from source.

```bash
git clone https://github.com/matanolabs/matano.git
cd matano
make package
```

The installer file (e.g. `matano-linux-x64.sh`) will be generated at the project root. You can then execute this file to install Matano. 

## Updating the matano CLI

To update the Matano CLI, follow the same instructions as above with an updated installation file.
