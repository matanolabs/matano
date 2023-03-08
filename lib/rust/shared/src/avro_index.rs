use crate::avro::AvroValueExt;
use apache_avro::from_avro_datum;
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
    /// There's an offset of 2 bytes between the start positions written in Index and where raw compressed block starts.
    const OFFSET: u64 = 2;
    /// The length of the sync marker at the end of each block. Remove to get actual data.
    const SYNC_LENGTH: usize = 16;

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

        let mem_cursor = Arc::try_unwrap(reader)
            .map_err(|e| anyhow!("Could not unwrap Arc: {:?}", e))?
            .into_inner()?;

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
                let start_pos = *start_pos as u64 + Self::OFFSET;
                let block_len = *block_len as usize - (Self::OFFSET as usize + Self::SYNC_LENGTH);

                // Read the whole block since we know the length and avoid multiple reads.
                let raw_block: Vec<u8> = {
                    let reader = self.reader.clone();
                    let start_pos = start_pos as usize;
                    let buf = reader[start_pos..start_pos + block_len].to_vec();
                    buf
                };

                let mut block = vec![];
                zstd::Decoder::new(raw_block.as_slice())?
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
