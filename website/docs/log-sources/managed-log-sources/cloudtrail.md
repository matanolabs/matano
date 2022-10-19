---
title: CloudTrail
---

The CloudTrail Matano managed log source lets you ingest your AWS CloudTrail logs directly into Matano.

## Usage

Use the managed log source by specifying the `managed.type` property in your `log_source` as `AWS_CLOUDTRAIL`.

```yml
managed:
  type: "AWS_CLOUDTRAIL"
```

## Transformation

CloudTrail data is normalized to standard ECS fields. Custom fields are normalized into the `aws` field. You can view the [complete mapping][1] to see the specific field mappings.

[1]: https://github.com/matanolabs/matano/blob/main/data/managed/aws_cloudtrail/log_source.yml

## Tables

The AWS CloudTrail managed log source creates the following Matano tables:

- **CloudTrail Logs**. A table is created for actual CloudTrail logs.
- **CloudTrail Digest**. Your CloudTrail digest files are transformed into a separate Matano table.
- **CloudTrail Insights**. CloudTrail insights files are processed into a Matano table.

Matano automatically ingests data in your CloudTrail bucket into the corresponding table.
