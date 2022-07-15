matano
=================

matano CLI

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
* [`matano generate:matano-dir DIRECTORY-NAME`](#matano-generatematano-dir-directory-name)
* [`matano help [COMMAND]`](#matano-help-command)

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

## `matano generate:matano-dir DIRECTORY-NAME`

Generates a sample Matano directory to get started.

```
USAGE
  $ matano generate:matano-dir [DIRECTORY-NAME]

ARGUMENTS
  DIRECTORY-NAME  The name of the directory to create

DESCRIPTION
  Generates a sample Matano directory to get started.

EXAMPLES
  $ matano generate:matano-dir
```

_See code: [dist/commands/generate/matano-dir.ts](https://github.com/matano/hello-world/blob/v0.0.0/dist/commands/generate/matano-dir.ts)_

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
<!-- commandsstop -->
