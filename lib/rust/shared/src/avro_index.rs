use crate::avro::AvroValueExt;
use crate::ArcMutexExt;
use apache_avro::from_avro_datum;
use apache_avro::AvroResult;
use log::info;
use memmap2::{Mmap, MmapOptions};
use once_cell::sync::OnceCell;
use std::io::{BufRead, Read};
use std::io::{BufReader, Seek};
use std::vec;
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex, Weak},
};

use anyhow::{anyhow, Context, Result};

/// We just need this because the Avro reader takes ownership but we want to be able to use the underlying reader to seek.
struct ReaderHolder {
    reader: Weak<Mutex<Cursor<Mmap>>>,
}

impl std::io::Read for ReaderHolder {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.upgrade().unwrap().lock().unwrap().read(buf)
    }
}

type RecordPosMap = HashMap<String, [i64; 2]>; // [start_pos, block_length]

/// `AvroIndex` represents an Avro file with an associated index.
/// The index is a JSON file that maps keys to byte offsets and block lengths in the Avro file.
/// The keys in the index correspond to the values of the primary key in the Avro file.
/// The values in the index correspond to the start of a block in the Avro file and the length of the block.
/// The Avro file is memory mapped and the index is loaded into memory as a HashMap.
/// On a lookup, the byte offsets and block length are used to seek to the start of the block in the Avro file and read the block.
/// The block is decompressed and the block's datums are then iterated over to find the datum with the matching primary key.
///
/// We additionally support multiple indices for a single Avro file. This is useful for when we want to index on multiple fields.
/// We lazily load the indices into memory as they are requested by a get query.
///
/// Similar to [SortedKeyValueFile](https://github.com/apache/avro/blob/master/lang/java/mapred/src/main/java/org/apache/avro/hadoop/file/SortedKeyValueFile.java) in Java.
pub struct AvroIndex {
    pub schema: apache_avro::Schema,
    indices: HashMap<String, (String, OnceCell<RecordPosMap>)>,
    reader: Arc<Mmap>,
}

impl AvroIndex {
    pub fn new(indices_paths: HashMap<String, String>, avro_file_path: &str) -> Result<Self> {
        let file = std::fs::File::open(avro_file_path)?;
        let mmap = unsafe { MmapOptions::new().populate().map(&file)? };
        mmap.advise(memmap2::Advice::Sequential)?;

        let indices = indices_paths
            .into_iter()
            .map(|(index_key, index_path)| (index_key, (index_path, OnceCell::new())))
            .collect::<HashMap<String, (String, OnceCell<RecordPosMap>)>>();

        let reader = Arc::new(Mutex::new(Cursor::new(mmap)));

        let reader_holder = ReaderHolder {
            reader: Arc::downgrade(&reader.clone()),
        };

        let avro_reader = apache_avro::Reader::new(reader_holder)?;
        let schema = avro_reader.writer_schema().clone();

        let mem_cursor = reader.try_unwrap_arc_mutex()?;

        let mem = mem_cursor.into_inner();

        let reader = Arc::new(mem);

        Ok(Self {
            indices,
            reader,
            schema,
        })
    }

    pub fn get_by_key(
        &self,
        key: &str,
        index_key: Option<&str>,
    ) -> Result<Option<apache_avro::types::Value>> {
        match index_key {
            Some(index_key) => self._get_by_key(index_key, key),
            None => {
                // If only one lookup key, use that one by default.
                if self.indices.len() == 1 {
                    let index_key = self.indices.keys().next().unwrap();
                    self._get_by_key(index_key, key)
                } else {
                    Err(anyhow!(
                        "No lookup key provided and multiple indices found."
                    ))
                }
            }
        }
    }

    fn get_index(&self, index_key: &str) -> Result<Option<&RecordPosMap>> {
        match self.indices.get(index_key) {
            Some((index_path, index)) => {
                let index = index.get_or_try_init(|| {
                    let index_bytes = std::fs::read(index_path)?;
                    let index = serde_json::from_slice(index_bytes.as_slice())?;
                    anyhow::Ok(index)
                })?;
                Ok(Some(index))
            }
            None => Ok(None),
        }
    }

    fn _get_by_key(&self, index_key: &str, key: &str) -> Result<Option<apache_avro::types::Value>> {
        let index = self
            .get_index(index_key)?
            .context(format!("Invalid lookup key: {}", index_key))?;
        match index.get(key) {
            Some([start_pos, block_len]) => {
                let start_pos = *start_pos as usize;
                let block_len = *block_len as usize;

                // Read the whole block since we know the length and avoid multiple reads.
                // The count and size are variable length encoded so we need to read so we can seek to the start of the block data.
                // (The block_len from the input here is the total block len which includes the count and size.)
                let raw_block = {
                    let mut buf = &self.reader[start_pos..start_pos + block_len];
                    let _count = decode_long(&mut buf)?;
                    let _size = decode_long(&mut buf)?;
                    buf
                };

                let mut block = vec![];
                zstd::Decoder::new(raw_block)?
                    .read_to_end(block.as_mut())
                    .map_err(|e| anyhow!("Failed to decompress avro idx block").context(e))?;

                let mut block_reader = block.as_slice();
                while !block_reader.is_empty() {
                    let value = from_avro_datum(&self.schema, &mut block_reader, None)?;
                    let key_value = value.get_nested(index_key).and_then(|v| v.as_str());
                    if key_value.map_or(false, |k| k == key) {
                        return Ok(Some(value));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }
}

// just vendoring read variable length long from avro
pub fn zag_i64<R: Read>(reader: &mut R) -> AvroResult<i64> {
    let z = decode_variable(reader)?;
    Ok(if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    })
}

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> AvroResult<apache_avro::types::Value> {
    zag_i64(reader).map(apache_avro::types::Value::Long)
}

fn decode_variable<R: Read>(reader: &mut R) -> AvroResult<u64> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            return Err(apache_avro::Error::IntegerOverflow);
        }
        reader
            .read_exact(&mut buf[..])
            .map_err(apache_avro::Error::ReadVariableIntegerBytes)?;
        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }

    Ok(i)
}
