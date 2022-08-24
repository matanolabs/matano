package com.matano.scripts

import org.apache.iceberg.SchemaParser
import org.apache.iceberg.avro.AvroSchemaUtil
import java.io.File
import kotlin.io.path.Path
import kotlin.io.path.readText
import kotlin.io.path.writeText

fun main(args: Array<String>) {
    // args.forEach { println(it) }
    when (args.first()) {
        "gen-schemas" -> generateSchemas(args)
        else -> throw RuntimeException("nonexistent script")
    }
}

fun generateSchemas(args: Array<String>) {
    val dirPath = Path(args[1])

    dirPath.toFile().list()!!.forEach { subDirName ->
        val subpath = dirPath.resolve(subDirName
        val logSourceName = subDirName
        val icebergSchemaPath = subpath.resolve("iceberg_schema.json")
        val icebergSchema = SchemaParser.fromJson(icebergSchemaPath.readText())
        val avroSchema = AvroSchemaUtil.convert(icebergSchema.asStruct(), logSourceName)
        val avroSchemaPath = subpath.resolve("avro_schema.avsc")
        avroSchemaPath.writeText(avroSchema.toString())
    }
    val outf = File("/asset-output")
    if (outf.exists()) {
        val outPath = outf.toPath().resolve("schemas")
        outPath.toFile().mkdirs()
        dirPath.toFile().copyRecursively(outPath.toFile())
    }
}
