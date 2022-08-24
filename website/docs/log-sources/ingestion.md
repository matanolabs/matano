---
title: Ingestion
sidebar_position: 2
---

You can ingest data from any log source using Matano. To ingest a log source into Matano, you can use either S3 or SQS ingestion.

## S3 Ingestion

When you use S3 ingestion, Matano ingests data from your log source using an S3 bucket. You can either use a Matano provided bucket or bring your own bucket if you have existing.

### Using the Matano provided sources bucket

Matano creates a managed S3 bucket for you to use for S3 ingestion. You can use this bucket to ingest data into Matano.

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
 