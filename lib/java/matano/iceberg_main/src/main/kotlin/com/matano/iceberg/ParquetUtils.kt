package com.matano.iceberg

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType

private class EmptyWriteSupport(var schema: MessageType) : WriteSupport<List<String?>?>() {
    var recordConsumer: RecordConsumer? = null

    override fun init(config: Configuration): WriteContext {
        return WriteContext(schema, HashMap())
    }
    override fun prepareForWrite(recordConsumer: RecordConsumer) {
        this.recordConsumer = recordConsumer
    }
    override fun write(values: List<String?>?) {
        recordConsumer!!.startMessage()
        recordConsumer!!.endMessage()
    }
}

// It's weirdly hard to write an empty parquet file in Java.
private class EmptyParquetWriter(file: org.apache.hadoop.fs.Path, schema: MessageType) : ParquetWriter<List<String?>?>(
    file,
    EmptyWriteSupport(schema),
    CompressionCodecName.UNCOMPRESSED,
    DEFAULT_BLOCK_SIZE,
    DEFAULT_PAGE_SIZE,
    true,
    false
)

/** You can't write a parquet schema separately, convention seems to be write empty parquet file with metadata. */
fun writeParquetSchema(path: String, schema: MessageType) {
    EmptyParquetWriter(org.apache.hadoop.fs.Path(path), schema).use { it.write(listOf()) }
}
