package com.matano.iceberg

import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory

fun getNestedAvro(record: GenericRecord, path: String): String {
    val parts = path.split(".")
    var currentRecord: Any = record
    for (part in parts) {
        currentRecord = (currentRecord as GenericRecord).get(part)
    }
    return (currentRecord as org.apache.avro.util.Utf8).toString()
}

fun generateAvroIndices(avroFile: ByteArray, lookupKeys: List<String>): Map<String, Map<String, Array<Long>>> {
    val avroFileLength = avroFile.size

    val bais = SeekableByteArrayInput(avroFile)
    val reader = DataFileReader(bais, GenericDatumReader<GenericRecord>())

    reader.sync(0)
    val allSyncPositions = mutableListOf<Long>()
    val syncLengths = mutableMapOf<Long, Long>()
    while (bais.available() > 0) {
        allSyncPositions.add(reader.tell())
        reader.sync(reader.tell())
    }
    for (i in 0 until allSyncPositions.size - 1) {
        syncLengths[allSyncPositions[i]] = allSyncPositions[i + 1] - allSyncPositions[i]
    }
    syncLengths[allSyncPositions.last()] = avroFileLength - allSyncPositions.last()

    val decoderFactory = DecoderFactory.get()
    var decoder: BinaryDecoder? = null
    val datumReader = GenericDatumReader<GenericRecord>(reader.schema)

    val indices: MutableMap<String, MutableMap<String, Array<Long>>> = mutableMapOf()
    for (syncPos in allSyncPositions) {
        reader.seek(syncPos)

        val block = reader.nextBlock()
        val blockLen = (syncLengths[syncPos] ?: throw RuntimeException("Need sync"))
        val blockInput = SeekableByteArrayInput(block.array())
        decoder = decoderFactory.directBinaryDecoder(blockInput, decoder)

        var record: GenericRecord? = null
        for (i in 0 until reader.blockCount) {
            record = datumReader.read(record, decoder)
            for (lookupKey in lookupKeys) {
                val index = indices.getOrPut(lookupKey) { mutableMapOf() }
                val key = getNestedAvro(record, lookupKey)
                index[key] = arrayOf(syncPos, blockLen)
            }
        }
    }
    return indices
}
