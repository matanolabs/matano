## Commands

<!-- prettier-ignore-start -->
<!-- commands -->
* [`matano autocomplete [SHELL]`](#matano-autocomplete-shell)
* [`matano deploy`](#matano-deploy)
* [`matano diff`](#matano-diff)
* [`matano disable-metadata-reporting`](#matano-disable-metadata-reporting)
* [`matano generate:matano-dir DIRECTORY-NAME`](#matano-generatematano-dir-directory-name)
* [`matano help [COMMAND]`](#matano-help-command)
* [`matano info`](#matano-info)
* [`matano init`](#matano-init)
* [`matano refresh-context`](#matano-refresh-context)

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

## `matano deploy`

Deploys matano.

```
USAGE
  $ matano deploy [--debug] [-p <value>] [--user-directory <value>]

FLAGS
  -p, --profile=<value>     AWS Profile to use for credentials.
  --debug                   View debug information.
  --user-directory=<value>  Matano user directory to use.

DESCRIPTION
  Deploys matano.

EXAMPLES
  $ matano deploy

  $ matano deploy --profile prod

  $ matano deploy --profile prod --user-directory matano-directory
```

_See code: [dist/commands/deploy.ts](https://github.com/matanolabs/matano/blob/main/cli/src/commands/deploy.ts)_

## `matano diff`

Shows differences in your Matano deployment.

```
USAGE
  $ matano diff [--debug] [-p <value>] [--user-directory <value>]

FLAGS
  -p, --profile=<value>     AWS Profile to use for credentials.
  --debug                   View debug information.
  --user-directory=<value>  Matano user directory to use.

DESCRIPTION
  Shows differences in your Matano deployment.

EXAMPLES
  $ matano diff

  $ matano diff --profile prod

  $ matano diff --profile prod --user-directory matano-directory
```

_See code: [dist/commands/diff.ts](https://github.com/matanolabs/matano/blob/main/cli/src/commands/diff.ts)_

## `matano disable-metadata-reporting`

Disables metadata reporting.

```
USAGE
  $ matano disable-metadata-reporting [--debug]

FLAGS
  --debug  View debug information.

DESCRIPTION
  Disables metadata reporting.

EXAMPLES
  $ matano disable-metadata-reporting
```

_See code: [dist/commands/disable-metadata-reporting.ts](https://github.com/matanolabs/matano/blob/main/cli/src/commands/disable-metadata-reporting.ts)_

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

_See code: [dist/commands/generate/matano-dir.ts](https://github.com/matanolabs/matano/blob/main/cli/src/commands/generate/matano-dir.ts)_

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

## `matano info`

Retrieves information about your Matano deployment in structured format.

```
USAGE
  $ matano info [--debug] [-p <value>] [--user-directory <value>] [--output csv|json|yaml |  | ]

FLAGS
  -p, --profile=<value>     AWS Profile to use for credentials.
  --debug                   View debug information.
  --output=<option>         output in a more machine friendly format
                            <options: csv|json|yaml>
  --user-directory=<value>  Matano user directory to use.

DESCRIPTION
  Retrieves information about your Matano deployment in structured format.

EXAMPLES
  $ matano info

  $ matano info --profile prod

  $ matano info --output json
```

_See code: [dist/commands/info.ts](https://github.com/matanolabs/matano/blob/main/cli/src/commands/info.ts)_

## `matano init`

Wizard to get started with Matano. Creates resources, initializes your account, and deploys Matano.

```
USAGE
  $ matano init [--debug] [-p <value>]

FLAGS
  -p, --profile=<value>  AWS Profile to use for credentials.
  --debug                View debug information.

DESCRIPTION
  Wizard to get started with Matano. Creates resources, initializes your account, and deploys Matano.

EXAMPLES
  $ matano init

  $ matano init --profile prod
```

_See code: [dist/commands/init.ts](https://github.com/matanolabs/matano/blob/main/cli/src/commands/init.ts)_

## `matano refresh-context`

Refreshes Matano context.

```
USAGE
  $ matano refresh-context [--debug] [-p <value>] [-a <value>] [-r <value>] [--user-directory <value>]

FLAGS
  -a, --account=<value>     AWS Account to deploy to.
  -p, --profile=<value>     AWS Profile to use for credentials.
  -r, --region=<value>      AWS Region to deploy to.
  --debug                   View debug information.
  --user-directory=<value>  Matano user directory to use.

DESCRIPTION
  Refreshes Matano context.

EXAMPLES
  $ matano refresh-context

  $ matano refresh-context --profile prod

  $ matano refresh-context --profile prod --user-directory my-matano-directory

  $ matano refresh-context --profile prod --region eu-central-1 --account 12345678901
```

_See code: [dist/commands/refresh-context.ts](https://github.com/matanolabs/matano/blob/main/cli/src/commands/refresh-context.ts)_
<!-- commandsstop -->
<!-- prettier-ignore-end -->
