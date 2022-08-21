---
title: Log source schema
sidebar_position: 2
---

You define the shape of your log source's data by defining a *schema*.

## Common schema

By default, a log source will use the [**Elastic Common Schema (ECS)**](https://www.elastic.co/guide/en/ecs/current/ecs-reference.html). ECS is a popular format that defines a common set of fields to be used when storing log data. 

Matano encourages you to normalize your log data to ECS, so you can best analyze, and correlate the data represented in your events.

Matano can ingest from sources that use already use ECS, like Beats processors, without requiring transformation. Otherwise, you can use a [pre-built Matano transformation](#) to normalize data from a supported source to ECS or write your [own transformation](#) to normalize log data.

Matano currently supports ECS version **8.3.1**.

## Using a Matano managed log source

If your log source is supported by Matano, you should use a Matano managed log source. Matano will take care of specifying the schema for your log source.

## Defining a custom schema

If you aren't using a Matano [managed log source](#), you must provide the schema for your log source.

## Specifying ECS fields

You can specify the subset of ECS fields to include in your schema by using the `ecs_field_names` key in your `log_source.yml` file.

```yml
schema:
  ecs_field_names: ["dns", "agent"]
```


## Extending ECS with custom fields

ECS is a permissive schema, that encourages you to add columns using custom field names for data that doesn't completely map to ECS. 

To add custom fields to ECS for a Matano log source, you can define the fields in your `log_source.yml` configuration file:

```yml
schema:
  ecs_field_names: ["dns", "agent"]
  fields:
    - name: "my_additional_field"
      type: "string"
    - name: "additional_struct"
      type:
        type: "struct"
        fields:
          - name: "field_one"
            type: "string"
          - name: "field_two"
            type: "int"
```

These fields will be merged with ECS fields in the final log source schema.

The schema configuration follows the [Apache Iceberg schema format](https://iceberg.apache.org/spec/#schemas) in JSON/YAML format. See a [complete specification here](https://iceberg.apache.org/spec/#schemas).
