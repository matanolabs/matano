package com.matano.iceberg

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.PositionedReadable
import org.apache.hadoop.fs.Seekable
import org.apache.parquet.io.DelegatingPositionOutputStream
import org.apache.parquet.io.DelegatingSeekableInputStream
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream
import org.apache.parquet.io.SeekableInputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream

class InMemoryOutputFile : OutputFile {
    private val baos = ByteArrayOutputStream()

    override fun create(blockSizeHint: Long): PositionOutputStream { // Mode.CREATE calls this method
        return InMemoryPositionOutputStream(baos)
    }

    override fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream {
        return create(blockSizeHint)
    }
    override fun supportsBlockSize(): Boolean = false
    override fun defaultBlockSize(): Long = 0

    fun toArray(): ByteArray = baos.toByteArray()

    private class InMemoryPositionOutputStream(outputStream: OutputStream?) : DelegatingPositionOutputStream(outputStream) {
        override fun getPos(): Long {
            return (stream as ByteArrayOutputStream).size().toLong()
        }
    }
}

class InMemoryInputFile(private val bytes: ByteArray) : InputFile {
    override fun getLength(): Long = bytes.size.toLong()
    override fun newStream(): SeekableInputStream = InMemoryInputStream(FSDataInputStream(SeekableByteArrayInputStream(bytes)))

    private class InMemoryInputStream(inputStream: FSDataInputStream) : DelegatingSeekableInputStream(inputStream) {
        override fun getPos(): Long = (stream as FSDataInputStream).pos
        override fun seek(pos: Long) = (stream as FSDataInputStream).seek(pos)
    }
}

class InMemoryIcebergOutputFile : org.apache.iceberg.io.OutputFile {
    private val baos = ByteArrayOutputStream()
    override fun create(): org.apache.iceberg.io.PositionOutputStream {
        return InMemoryPositionOutputStream(baos)
    }

    override fun createOrOverwrite(): org.apache.iceberg.io.PositionOutputStream {
        return create()
    }

    override fun location(): String? = null

    override fun toInputFile(): org.apache.iceberg.io.InputFile? {
        return null
    }

    fun toArray(): ByteArray = baos.toByteArray()

    private class InMemoryPositionOutputStream(val outputStream: OutputStream) : org.apache.iceberg.io.PositionOutputStream() {
        override fun write(b: Int) {
            outputStream.write(b)
        }

        override fun getPos(): Long {
            return (outputStream as ByteArrayOutputStream).size().toLong()
        }
    }
}

class InMemoryIcebergInputFile(private val bytes: ByteArray) : org.apache.iceberg.io.InputFile {
    override fun getLength() = bytes.size.toLong()

    override fun newStream(): org.apache.iceberg.io.SeekableInputStream {
        return InMemoryInputStream(FSDataInputStream(SeekableByteArrayInputStream(bytes)))
    }

    override fun location() = "<mem>"
    override fun exists() = true

    private class InMemoryInputStream(val stream: FSDataInputStream) : org.apache.iceberg.io.SeekableInputStream() {
        override fun read(): Int = stream.read()
        override fun getPos(): Long = stream.getPos()
        override fun seek(newPos: Long) = stream.seek(newPos)
    }
}

internal class SeekableByteArrayInputStream(buf: ByteArray?) : ByteArrayInputStream(buf), Seekable, PositionedReadable {
    override fun getPos(): Long {
        return pos.toLong()
    }

    override fun seek(pos: Long) {
        check(mark == 0)
        reset()
        val skipped = skip(pos)
        if (skipped != pos) throw IOException()
    }
    override fun seekToNewSource(targetPos: Long): Boolean {
        return false
    }
    override fun read(position: Long, buffer: ByteArray, offset: Int, length: Int): Int {
        require(position < buf.size)
        require(position + length <= buf.size)
        require(length <= buffer.size)
        System.arraycopy(buf, position.toInt(), buffer, offset, length)
        return length
    }
    override fun readFully(position: Long, buffer: ByteArray) {
        read(position, buffer, 0, buffer.size)
    }
    override fun readFully(position: Long, buffer: ByteArray, offset: Int, length: Int) {
        read(position, buffer, offset, length)
    }
}

// ParquetIO is private...
class ParquetInputStreamAdapter constructor(private val delegate: org.apache.iceberg.io.SeekableInputStream) : DelegatingSeekableInputStream(delegate) {
    override fun getPos() = delegate.pos
    override fun seek(newPos: Long) = delegate.seek(newPos)
}
class ParquetIcebergInputFile(private val inputFile: org.apache.iceberg.io.InputFile) : org.apache.parquet.io.InputFile {
    override fun getLength(): Long = inputFile.length
    override fun newStream(): SeekableInputStream = ParquetInputStreamAdapter(inputFile.newStream())
}
class ParquetOutputStreamAdapter(private val delegate: org.apache.iceberg.io.PositionOutputStream) : DelegatingPositionOutputStream(delegate) {
    override fun getPos() = delegate.pos
}
class ParquetIcebergOutputFile(private val file: org.apache.iceberg.io.OutputFile) : OutputFile {
    override fun create(ignored: Long): PositionOutputStream = ParquetOutputStreamAdapter(file.create())
    override fun createOrOverwrite(ignored: Long): PositionOutputStream = ParquetOutputStreamAdapter(file.createOrOverwrite())

    override fun supportsBlockSize() = false
    override fun defaultBlockSize(): Long = 0
}
