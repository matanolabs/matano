---
title: Querying log sources
sidebar_position: 3
---

All Matano log sources are stored as Iceberg tables, with data stored in Parquet files on S3. You can query and interact with these like any other Iceberg table, using Athena, Spark, or any other technology supporting Iceberg. This ensures your data is free from vendor lock-in and open to a variety of data tooling.

## Notes
Matano log sources are stored in AWS Glue database named **matano**, with the Iceberg table name as the log source name specified in your `matano.config.yml`. 

## Querying a log source

**See more on [Querying Iceberg tables](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-table-data.html) in Athena.**

You can query a log source from Athena using the following syntax:

```sql
SELECT * FROM matano.log_source_name [WHERE predicate]
```

## Advanced
### Performing ACID transactions

Iceberg tables support ACID transactions such as deleting, inserting, and updating. You can use this feature if you need to modify your data for compliance, legal, or any other reason, without having to copy and re-load your entire dataset.

See [Updating Iceberg table data](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html) on the syntax to perform Update and Delete transactions on your log sources.
