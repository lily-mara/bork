#![allow(dead_code)]

use std::{
    collections::HashMap,
    ffi::OsStr,
    fmt::Debug,
    fs::File,
    io::{BufReader, ErrorKind, Read},
    path::PathBuf,
};

use byteorder::{LittleEndian, ReadBytesExt};
use configparser::ini::Ini;
use eyre::{bail, eyre, Context, Result};
use msgpack::{Bytes, PythonValue};
use serde::Deserialize;

mod msgpack;

const MANIFEST_ID: [u8; 32] = [0; 32];

fn main() -> Result<()> {
    let repository = Repository::load(PathBuf::from(std::env::args().nth(1).unwrap()))?;

    // dbg!(&repository);

    for segment in repository.segments()? {
        println!("SEGMENT {}", segment.id);
        for entry in segment.open()? {
            let entry = entry?;
            println!("{entry:?}");

            if let LogEntry::Put { key, data } = &entry {
                if *key == MANIFEST_ID {
                    let data = unpack_data(data)?;
                    std::fs::write(hex_str(key), &data)?;

                    let manifest = rmp_serde::decode::from_slice::<Manifest>(&data)
                        .wrap_err("decode manifest msgpack")?;
                    dbg!(manifest);
                }
            }
        }

        println!();
    }

    for index in repository.indices()? {
        dbg!(&index, index.open()?);
    }

    for hint in repository.hints()? {
        dbg!(hint);
    }

    Ok(())
}

/// Reads the data segment from a PUT log entry and removes the encryption and
/// compression layers from it, returning a plain view of the data
fn unpack_data(data: &[u8]) -> Result<Vec<u8>> {
    let mut data = std::io::Cursor::new(data);

    if data.read_u8().wrap_err("read encryption")? != 0x02 {
        bail!("only plaintext data is supported");
    }

    if data
        .read_u16::<LittleEndian>()
        .wrap_err("read compression")?
        != 0x00_01
    {
        bail!("only lz4 compression is supported");
    }

    let position = data.position() as usize;
    let sliced_data = &data.into_inner()[position..];

    let mut size = sliced_data.len() * 3;
    loop {
        let mut buffer = vec![0; size];
        match lz4::block::decompress_to_buffer(sliced_data, Some(size as i32), &mut buffer) {
            Ok(bytes) => {
                buffer.resize(bytes, 0);
                return Ok(buffer);
            }
            Err(e) => {
                if e.kind() == ErrorKind::InvalidInput {
                    if size > 2usize.pow(27) {
                        bail!("lz4 decompress failed");
                    }

                    size = (size as f64 * 1.5) as usize;
                } else {
                    return Err(e).wrap_err("lz4 decompress");
                }
            }
        }
    }
}

fn number(o: &OsStr) -> Option<u32> {
    if let Some(s) = o.to_str() {
        return s.parse().ok();
    }

    None
}

#[derive(Debug)]
struct Hint {
    data: HintData,
    id: u32,
}

#[derive(Deserialize, Debug)]
struct HintData {
    version: u8,
    segments: HashMap<PythonValue, PythonValue>,
    compact: HashMap<PythonValue, PythonValue>,
    storage_quota_use: PythonValue,
    shadow_index: HashMap<PythonValue, PythonValue>,
}

#[derive(Deserialize, Debug)]
struct Manifest {
    version: u8,
    timestamp: String,
    item_keys: Vec<String>,
    config: HashMap<String, String>,
    archives: HashMap<String, ManifestArchive>,
    tam: Tam,
}

#[derive(Deserialize, Debug)]
struct Tam {
    #[serde(rename = "type")]
    tipe: String,

    #[serde(flatten)]
    data: HashMap<String, Bytes>,
}

#[derive(Deserialize)]
struct ManifestArchive {
    id: Bytes,
    time: String,
}

impl Debug for ManifestArchive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestArchive")
            .field("id", &hex_str(&self.id.0))
            .field("time", &self.time)
            .finish()
    }
}

#[derive(Debug)]
struct Repository {
    path: PathBuf,
    config: Ini,
    id: String,
}

struct Archive {
    path: PathBuf,
}

#[derive(Debug)]
struct Segment {
    id: u32,
    path: PathBuf,
}

#[derive(Debug)]
struct OpenSegment {
    data: BufReader<File>,
}

#[derive(Debug)]
struct Index {
    transaction_id: u32,
    path: PathBuf,
}

#[derive(Debug)]
struct OpenIndex {
    variant: IndexVariant,
    data: BufReader<File>,
}

#[derive(Debug)]
enum IndexVariant {
    V1,
    V2,
}

enum LogEntry {
    Put { key: [u8; 32], data: Vec<u8> },
    Delete { key: [u8; 32] },
    Commit,
}

fn hex_str(x: &[u8]) -> String {
    let mut s = String::new();

    for byte in x {
        s.push_str(&format!("{byte:02X} "));
    }

    s.pop();

    s
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogEntry::Commit => write!(f, "COMMIT"),
            LogEntry::Delete { key } => write!(f, "DELETE {}", hex_str(key)),
            LogEntry::Put { key, data } => {
                write!(f, "PUT    {} - {} bytes", hex_str(key), data.len())
            }
        }
    }
}

impl Iterator for OpenSegment {
    type Item = Result<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_log_entry() {
            Ok(None) => None,
            Ok(Some(x)) => Some(Ok(x)),
            Err(e) => Some(Err(e)),
        }
    }
}

impl OpenSegment {
    fn next_log_entry(&mut self) -> Result<Option<LogEntry>> {
        // TODO: actually use the CRC?
        let _crc = match self.data.read_u32::<LittleEndian>() {
            Ok(x) => x,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                }

                return Err(e.into());
            }
        };
        let size = self.data.read_u32::<LittleEndian>()?;
        let tag = self.data.read_u8()?;

        match tag {
            0 => {
                let mut key = [0; 32];
                self.data.read_exact(&mut key)?;

                let data_len = (size - 41) as usize;

                let mut data = vec![0; data_len];
                self.data.read_exact(&mut data)?;

                Ok(Some(LogEntry::Put { key, data }))
            }
            1 => {
                let mut key = [0; 32];
                self.data.read_exact(&mut key)?;

                Ok(Some(LogEntry::Delete { key }))
            }
            2 => Ok(Some(LogEntry::Commit)),
            _ => bail!("unknown log entry tag {tag}"),
        }
    }
}

impl Segment {
    fn open(&self) -> Result<OpenSegment> {
        let mut data = BufReader::new(File::open(&self.path)?);

        let mut buf = [0; 8];
        data.read_exact(&mut buf).wrap_err("failed 8 byte read")?;

        if &buf != b"BORG_SEG" {
            bail!("segment does not contain BORG_SEG magic number");
        }

        Ok(OpenSegment { data })
    }

    fn variant(r: &mut impl Read) -> Result<IndexVariant> {
        let mut data = [0; 8];
        r.read_exact(&mut data).wrap_err("failed 8 byte read")?;

        // value 12345678 is used by borg unit tests, we just return the current
        // variant when we see this.

        match &data {
            b"BORG_IDX" => Ok(IndexVariant::V1),
            b"BORG2IDX" | b"12345678" => Ok(IndexVariant::V2),
            _ => bail!("Unknown hashindex magic number: {:?}", data),
        }
    }
}

impl Archive {
    fn items(&self) {}
}

impl Index {
    fn open(&self) -> Result<OpenIndex> {
        let mut data = BufReader::new(File::open(&self.path)?);

        let variant = Self::variant(&mut data).wrap_err_with(|| {
            format!(
                "failed to determine variant of index file {}",
                self.path.display()
            )
        })?;

        Ok(OpenIndex { variant, data })
    }

    fn variant(r: &mut impl Read) -> Result<IndexVariant> {
        let mut data = [0; 8];
        r.read_exact(&mut data).wrap_err("failed 8 byte read")?;

        // value 12345678 is used by borg unit tests, we just return the current
        // variant when we see this.

        match &data {
            b"BORG_IDX" => Ok(IndexVariant::V1),
            b"BORG2IDX" | b"12345678" => Ok(IndexVariant::V2),
            _ => bail!("Unknown hashindex magic number: {:?}", data),
        }
    }
}

impl Repository {
    fn load(path: PathBuf) -> Result<Self> {
        let config_str =
            std::fs::read_to_string(path.join("config")).wrap_err("read config file")?;

        let mut config = configparser::ini::Ini::new();

        config
            .read(config_str)
            .map_err(|e| eyre!(e))
            .wrap_err("parse config ini")?;

        let id = config
            .get("repository", "id")
            .ok_or_else(|| eyre!("config file missing ID key"))?;

        Ok(Self { config, path, id })
    }

    fn hints(&self) -> Result<Vec<Hint>> {
        let mut hints = Vec::new();

        for result in std::fs::read_dir(&self.path)? {
            let dir_entry = result?;

            if let Some(s) = dir_entry.file_name().to_str() {
                if s.starts_with("hints.") {
                    if let Ok(id) = s[6..].parse() {
                        hints.push(Hint {
                            id,
                            data: rmp_serde::from_read(
                                File::open(dir_entry.path())
                                    .wrap_err("failed to read hint file")?,
                            )
                            .wrap_err("failed to parse hint file as msgpack")?,
                        });
                    }
                }
            }
        }

        Ok(hints)
    }

    fn indices(&self) -> Result<Vec<Index>> {
        let mut indices = Vec::new();

        for result in std::fs::read_dir(&self.path)? {
            let dir_entry = result?;

            if let Some(s) = dir_entry.file_name().to_str() {
                if s.starts_with("index.") {
                    if let Ok(id) = s[6..].parse() {
                        indices.push(Index {
                            transaction_id: id,
                            path: dir_entry.path(),
                        });
                    }
                }
            }
        }

        indices.sort_by(|i1, i2| i1.transaction_id.cmp(&i2.transaction_id));

        Ok(indices)
    }

    fn segments(&self) -> Result<Vec<Segment>> {
        let mut dirs = Vec::new();
        for result in std::fs::read_dir(self.path.join("data"))? {
            let dir_entry = result?;

            let metadata = dir_entry.metadata()?;
            if !metadata.is_dir() {
                continue;
            }

            if let Some(dir_num) = number(&dir_entry.file_name()) {
                dirs.push((dir_num, dir_entry.path()));
            }
        }

        dirs.sort_by(|d1, d2| d1.0.cmp(&d2.0));

        let mut segments = Vec::new();

        for (_, dir) in dirs {
            for result in std::fs::read_dir(dir)? {
                let dir_entry = result?;

                if let Some(id) = number(&dir_entry.file_name()) {
                    segments.push(Segment {
                        id,
                        path: dir_entry.path(),
                    });
                }
            }
        }

        segments.sort_by(|s1, s2| s1.id.cmp(&s2.id));

        Ok(segments)
    }
}
