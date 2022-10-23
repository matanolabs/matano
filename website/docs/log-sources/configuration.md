---
title: Log source configuration
sidebar_position: 4
---

## Configuring log sources

To create a log source, create a directory under the `log_sources` subdirectory in your Matano directory and create a file named `log_source.yml`. The directory structure is as follows:

```
my-matano-dir/
└── log_sources/
    └── aws_cloudtrail/
        └── log_source.yml
```

### Log source configuration file

The configuration for a log source lives in a YAML file named `log_source.yml`. The file has the following fields.

#### Fields

```yml
# The unique name of the log source.
name: "aws_cloudtrail"

# Properties for managed log sources
managed:
  # The identifier of the managed log source
  type: "AWS_CLOUDTRAIL"
  # Map of string values for managed log source configuration
  properties: {}

ingest:
  s3_source:
    # Name of existing S3 Bucket to use as a source
    bucket_name: "my-bucket"
    # Object key prefix for existing S3 source
    key_prefix: "my-prefix"

  # (Multi table log sources only) Used for mapping incoming data
  # to the appropriate table at runtime.
  select_table_from_payload_metadata: |
    if match(.__metadata.s3.key, r'Digest') { "digest" } else { "default" }

# Defines the schema for a log source.
schema:
  ecs_field_names:
    - event
    # use dotted path to select nested fields
    - user.id
  # List of custom schema fields in Apache Iceberg format.
  fields:
    - name: aws
      type:
        type: struct
        fields: []

# The VRL expression to transform your data.
transform: |
  if .json.eventTime != null {
      .ts = to_timestamp!(.json.eventTime, "milliseconds")
  }
```

## Creating multiple tables from a log source

By default, a log source will generate a single table with the same name as the log source.

Matano supports creating multiple Matano tables from a single log source.

To configure multiple tables from a log source, create a `tables/` subdirectory in your log source directory. For example, if you have the log source `aws_cloudtrail`, your directory structure would be as follows:

```
my-matano-dir/
└── log_sources/
    └── aws_cloudtrail/
        └── tables/
            ├── default.yml
            └── digest.yml
```

The files named `default.yml` and `digest.yml` are _table configuration files_.

### Table configuration file

The table configuration file is a YAML file with the following structure:

```yml
# optional, if omitted will use the log source name
name: "dns"

# optional, same as in `log_source.yml`
# will be merged with schema in `log_source.yml`
schema:
  fields:
    - name: custom_field
      type: string

# optional, same as in `log_source.yml
# will be merged with schema in `log_source.yml`
transform: |
  if .ts != null {
      .event.created = .ts
  }
```

#### Sharing log

Table level configurations 'inherit' from log source level configurations defined in the corresponding `log_source.yml` and both log source level and table level configurations will be merged. You can use this to share properties and logic common to all tables within a log source while applying custom properties to each table.

The name defined in a table configuration will be combined with the log source name to form the final Matano table name. For example, a log source named `zeek` with a table `dns` will result in a Matano table named `zeek_dns`.

### Table selection

When Matano ingests data for a log source with multiple tables, it will route the data to the correct table based on the incoming data's metadata. You provide this logic to Matano using a VRL expression that Matano evaluates on incoming data's metadata at runtime.

To define the table selection VRL expression use the `ingest.select_table_from_payload_metadata` key in your **log_source.yml**.

#### Expression input

Your VRL expression is passed a `__metadata` key with the following structure:

```json
{
  "__metadata": {
    "s3": {
      "bucket": "my-bucket",
      "key": "my/key",
      "size": 123456 // integer bytes
    }
  }
}
```

#### Expression output

The expression should return a string containing the table name that the data maps to.

#### Example

For example, the `aws_cloudtrail` log source has 3 tables configured. The following VRL expression is defined to select the appropriate table from the uploaded file:

```yml
# log_source.yml

select_table_from_payload_metadata: |
  if match(.__metadata.s3.key, r'Digest') {
    "digest"
  } else if match(.__metadata.s3.key, r'Insights') {
    "insights"
  } else {
    "default"
  }
```
