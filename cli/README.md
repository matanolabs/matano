oclif-hello-world
=================

oclif example Hello World CLI

[![oclif](https://img.shields.io/badge/cli-oclif-brightgreen.svg)](https://oclif.io)
[![Version](https://img.shields.io/npm/v/oclif-hello-world.svg)](https://npmjs.org/package/oclif-hello-world)
[![CircleCI](https://circleci.com/gh/oclif/hello-world/tree/main.svg?style=shield)](https://circleci.com/gh/oclif/hello-world/tree/main)
[![Downloads/week](https://img.shields.io/npm/dw/oclif-hello-world.svg)](https://npmjs.org/package/oclif-hello-world)
[![License](https://img.shields.io/npm/l/oclif-hello-world.svg)](https://github.com/oclif/hello-world/blob/main/package.json)

<!-- toc -->
* [Usage](#usage)
* [Commands](#commands)
<!-- tocstop -->
# Usage
<!-- usage -->
```sh-session
$ npm install -g matano
$ matano COMMAND
running command...
$ matano (--version)
matano/0.0.0 linux-x64 node-v18.4.0
$ matano --help [COMMAND]
USAGE
  $ matano COMMAND
...
```
<!-- usagestop -->
# Commands
<!-- commands -->
* [`matano autocomplete [SHELL]`](#matano-autocomplete-shell)
* [`matano bootstrap`](#matano-bootstrap)
* [`matano deploy`](#matano-deploy)
* [`matano generate matano_dir`](#matano-generate-matano_dir)
* [`matano help [COMMAND]`](#matano-help-command)
* [`matano plugins`](#matano-plugins)
* [`matano plugins:install PLUGIN...`](#matano-pluginsinstall-plugin)
* [`matano plugins:inspect PLUGIN...`](#matano-pluginsinspect-plugin)
* [`matano plugins:install PLUGIN...`](#matano-pluginsinstall-plugin-1)
* [`matano plugins:link PLUGIN`](#matano-pluginslink-plugin)
* [`matano plugins:uninstall PLUGIN...`](#matano-pluginsuninstall-plugin)
* [`matano plugins:uninstall PLUGIN...`](#matano-pluginsuninstall-plugin-1)
* [`matano plugins:uninstall PLUGIN...`](#matano-pluginsuninstall-plugin-2)
* [`matano plugins update`](#matano-plugins-update)

## `matano autocomplete [SHELL]`

display autocomplete installation instructions

```
USAGE
  $ matano autocomplete [SHELL] [-r]

ARGUMENTS
  SHELL  shell type

FLAGS
  -r, --refresh-cache  Refresh cache (ignores displaying instructions)

DESCRIPTION
  display autocomplete installation instructions

EXAMPLES
  $ matano autocomplete

  $ matano autocomplete bash

  $ matano autocomplete zsh

  $ matano autocomplete --refresh-cache
```

_See code: [@oclif/plugin-autocomplete](https://github.com/oclif/plugin-autocomplete/blob/v1.3.0/src/commands/autocomplete/index.ts)_

## `matano bootstrap`

Creates initial resources for Matano deployment.

```
USAGE
  $ matano bootstrap [-p <value>]

FLAGS
  -p, --profile=<value>  AWS Profile to use for credentials.

DESCRIPTION
  Creates initial resources for Matano deployment.

EXAMPLES
  $ matano bootstrap

  $ matano bootstrap --profile prod
```

_See code: [dist/commands/bootstrap.ts](https://github.com/matano/hello-world/blob/v0.0.0/dist/commands/bootstrap.ts)_

## `matano deploy`

Deploys matano.

```
USAGE
  $ matano deploy -a <value> -r <value> [-p <value>] [--user-directory <value>]

FLAGS
  -a, --account=<value>     (required) AWS Account to deploy to.
  -p, --profile=<value>     AWS Profile to use for credentials.
  -r, --region=<value>      (required) AWS Region to deploy to.
  --user-directory=<value>  Matano user directory to use.

DESCRIPTION
  Deploys matano.

EXAMPLES
  $ matano deploy --profile prod --region eu-central-1 --account 12345678901
```

_See code: [dist/commands/deploy.ts](https://github.com/matano/hello-world/blob/v0.0.0/dist/commands/deploy.ts)_

## `matano generate matano_dir`

Generates a sample Matano directory to get started.

```
USAGE
  $ matano generate matano_dir

DESCRIPTION
  Generates a sample Matano directory to get started.

EXAMPLES
  $ matano generate:matano-dir
```

## `matano help [COMMAND]`

Display help for matano.

```
USAGE
  $ matano help [COMMAND] [-n]

ARGUMENTS
  COMMAND  Command to show help for.

FLAGS
  -n, --nested-commands  Include all nested commands in the output.

DESCRIPTION
  Display help for matano.
```

_See code: [@oclif/plugin-help](https://github.com/oclif/plugin-help/blob/v5.1.12/src/commands/help.ts)_

## `matano plugins`

List installed plugins.

```
USAGE
  $ matano plugins [--core]

FLAGS
  --core  Show core plugins.

DESCRIPTION
  List installed plugins.

EXAMPLES
  $ matano plugins
```

_See code: [@oclif/plugin-plugins](https://github.com/oclif/plugin-plugins/blob/v2.1.0/src/commands/plugins/index.ts)_

## `matano plugins:install PLUGIN...`

Installs a plugin into the CLI.

```
USAGE
  $ matano plugins:install PLUGIN...

ARGUMENTS
  PLUGIN  Plugin to install.

FLAGS
  -f, --force    Run yarn install with force flag.
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Installs a plugin into the CLI.

  Can be installed from npm or a git url.

  Installation of a user-installed plugin will override a core plugin.

  e.g. If you have a core plugin that has a 'hello' command, installing a user-installed plugin with a 'hello' command
  will override the core plugin implementation. This is useful if a user needs to update core plugin functionality in
  the CLI without the need to patch and update the whole CLI.

ALIASES
  $ matano plugins add

EXAMPLES
  $ matano plugins:install myplugin 

  $ matano plugins:install https://github.com/someuser/someplugin

  $ matano plugins:install someuser/someplugin
```

## `matano plugins:inspect PLUGIN...`

Displays installation properties of a plugin.

```
USAGE
  $ matano plugins:inspect PLUGIN...

ARGUMENTS
  PLUGIN  [default: .] Plugin to inspect.

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Displays installation properties of a plugin.

EXAMPLES
  $ matano plugins:inspect myplugin
```

## `matano plugins:install PLUGIN...`

Installs a plugin into the CLI.

```
USAGE
  $ matano plugins:install PLUGIN...

ARGUMENTS
  PLUGIN  Plugin to install.

FLAGS
  -f, --force    Run yarn install with force flag.
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Installs a plugin into the CLI.

  Can be installed from npm or a git url.

  Installation of a user-installed plugin will override a core plugin.

  e.g. If you have a core plugin that has a 'hello' command, installing a user-installed plugin with a 'hello' command
  will override the core plugin implementation. This is useful if a user needs to update core plugin functionality in
  the CLI without the need to patch and update the whole CLI.

ALIASES
  $ matano plugins add

EXAMPLES
  $ matano plugins:install myplugin 

  $ matano plugins:install https://github.com/someuser/someplugin

  $ matano plugins:install someuser/someplugin
```

## `matano plugins:link PLUGIN`

Links a plugin into the CLI for development.

```
USAGE
  $ matano plugins:link PLUGIN

ARGUMENTS
  PATH  [default: .] path to plugin

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Links a plugin into the CLI for development.

  Installation of a linked plugin will override a user-installed or core plugin.

  e.g. If you have a user-installed or core plugin that has a 'hello' command, installing a linked plugin with a 'hello'
  command will override the user-installed or core plugin implementation. This is useful for development work.

EXAMPLES
  $ matano plugins:link myplugin
```

## `matano plugins:uninstall PLUGIN...`

Removes a plugin from the CLI.

```
USAGE
  $ matano plugins:uninstall PLUGIN...

ARGUMENTS
  PLUGIN  plugin to uninstall

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Removes a plugin from the CLI.

ALIASES
  $ matano plugins unlink
  $ matano plugins remove
```

## `matano plugins:uninstall PLUGIN...`

Removes a plugin from the CLI.

```
USAGE
  $ matano plugins:uninstall PLUGIN...

ARGUMENTS
  PLUGIN  plugin to uninstall

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Removes a plugin from the CLI.

ALIASES
  $ matano plugins unlink
  $ matano plugins remove
```

## `matano plugins:uninstall PLUGIN...`

Removes a plugin from the CLI.

```
USAGE
  $ matano plugins:uninstall PLUGIN...

ARGUMENTS
  PLUGIN  plugin to uninstall

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Removes a plugin from the CLI.

ALIASES
  $ matano plugins unlink
  $ matano plugins remove
```

## `matano plugins update`

Update installed plugins.

```
USAGE
  $ matano plugins update [-h] [-v]

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Update installed plugins.
```
<!-- commandsstop -->
