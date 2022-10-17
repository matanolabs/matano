---
title: Transformation
sidebar_position: 3
---

Matano allows you to transform your data into a normalized form. The transformation is specified using the Vector ReMap Language (VRL).

### Using Matano managed log sources

If you're using a [Matano managed log source](./managed-log-sources/index.mdx), a transformation will be applied for you and you don't need to write a VRL expression to transform your data.

## Transformations

To apply a transformation to your log source, specify a VRL expression to transform your data as a string in the `transform` key in your `log_source.yml` file.

```yml
transform: |
    _date, err = to_timestamp(.json.eventTime)
    if err == null {
        .ts = to_unix_timestamp(_date, "milliseconds")
    }
```

### Writing transformation VRL expressions

The input to your VRL expression is a single record from your data source. The output of the VRL expression is the transformed record.

<!-- ### Examples -->
