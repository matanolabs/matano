---
title: Ingestion
sidebar_position: 2
---

You can ingest data from any log source using Matano. To ingest a log source into Matano, you can use either S3 or SQS ingestion.

## S3 Ingestion

When you use S3 ingestion, Matano ingests data from your log source using an S3 bucket. You can either use a Matano provided bucket or bring your own bucket if you have existing data.

### Using the Matano provided sources bucket

Matano creates a managed S3 bucket for you to use for S3 ingestion. You can use this bucket to ingest data into Matano.

To retrieve the value of the Matano sources bucket, use the `matano info` command. See [Retrieving resource values](../getting-started.md#retrieving-resource-values).

When sending data to the Matano provided sources bucket, upload files to the `/data/<log_source_name>` prefix where `log_source_name` is the name of your log source that you specified in your `log_source.yml` file.

For example, to upload data for a log source named `serverlogs`, you would upload data to the following key prefixes:

```
/data/serverlogs/d007cb0d-7c00-43aa-b9a9-f7cc37e780dc.json
/data/serverlogs/80cd10db-7760-4e34-830d-b98342dd180a.json
```

If you are getting started, don't have existing data, or need your raw data to be a specific location, prefer using the Matano provided sources bucket.

### Bringing your own bucket

If you have existing data, or need to have your raw data in a specific location, you can configure Matano to ingest data from a provided S3 location.

In your `log_source.yml`, specify the S3 Bucket and object prefix that your data is located at:

```yml
ingest:
    s3_source:
        bucket_name: "my-org-logs-bucket"
        key_prefix: "data/mypath"
```

If you are bringing your own bucket, you need to ensure that you have correctly set up permissions on the bucket for Matano to be able to access it.

## Expanding records

When you send data to Matano, you need to communicate how Matano should split the data into individual records. Matano assumes your data is line delimited by default so if you are using a line delimited format like JSON Lines or CSV, Matano will automatically split your data and you do not need to provide any additional configuration.

If you data is not line delimited, you must tell Matano how to expand your data into records using a VRL expression. Provide the VRL expression under the `ingest.expand_records_from_payload` key in your `log_source.yml`, as follows:

```yml
ingest:
    expand_records_from_payload: "parse_json!(.__raw).Records"
```

Your VRL expression will receive the raw payload as `__raw` and must return an array of records. 

For example, if your data is a JSON document with following format:

```json
{
    "Data": [
        { "name": "john" }
    ]
}
```
You would use the following VRL expression to expand your data:

```
parse_json!(.__raw).Data
```
