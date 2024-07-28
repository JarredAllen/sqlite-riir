//! Tools for handling records

use std::fmt;

use anyhow::{Context, Result};

use crate::parse_varint;

#[derive(Copy, Clone)]
pub struct Record<'a> {
    /// A header containing schema information
    header: &'a [u8],
    /// The body, containing the raw data
    body: &'a [u8],
}
impl<'a> Record<'a> {
    pub(crate) fn parse(payload: &'a [u8]) -> Result<Self> {
        let header_len = parse_varint(&mut &*payload)?;
        let (header, body) = payload
            .split_at_checked(usize::try_from(header_len).context("Invalid header length")?)
            .context("Unexpected end of payload")?;
        Ok(Self { header, body })
    }

    /// Return an iterator over the [types of values](ColumnType) in `self`.
    pub fn type_iter(&self) -> impl Iterator<Item = ColumnType> + 'a {
        HeaderTypesIter::new(self.header)
    }

    /// Return an iterator over the values contained within.
    pub fn value_iter(&self) -> impl Iterator<Item = Value<&'a [u8]>> + 'a {
        RecordValueIter {
            header: HeaderTypesIter::new(self.header),
            body: self.body,
        }
    }
}

struct RecordValueIter<'a> {
    header: HeaderTypesIter<'a>,
    body: &'a [u8],
}
impl<'a> Iterator for RecordValueIter<'a> {
    type Item = Value<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let ty = self.header.next()?;
        let value = Value::parse_for_ty(ty, &mut self.body).expect("Failed to parse body");
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.header.size_hint()
    }
}

/// Iterator over types in a record.
struct HeaderTypesIter<'a> {
    header: &'a [u8],
}
impl<'a> HeaderTypesIter<'a> {
    fn new(mut header: &'a [u8]) -> Self {
        // The header starts with a varint we can skip
        let _ = parse_varint(&mut header);
        Self { header }
    }
}
impl<'a> Iterator for HeaderTypesIter<'a> {
    type Item = ColumnType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.header.is_empty() {
            return None;
        }
        // TODO Error checking
        let numeric = parse_varint(&mut self.header).unwrap();
        Some(ColumnType::from_numeric(numeric))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Each varint is between 1 and 9 bytes
        (self.header.len() / 9, Some(self.header.len()))
    }
}

/// A value a column of a record can have
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Value<Blob: AsRef<[u8]>> {
    // TODO More efficient storage of `i24` and `i48`
    Null,
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I48(i64),
    I64(i64),
    F64(f64),
    Zero,
    One,
    Blob(Blob),
    String(Blob),
    SQLiteReserved,
}
impl<'a, Blob: From<&'a [u8]> + AsRef<[u8]>> Value<Blob> {
    /// Parse a value for the given type.
    pub fn parse_for_ty(ty: ColumnType, buffer: &mut &'a [u8]) -> Result<Self> {
        Ok(match ty {
            ColumnType::Null => Self::Null,
            ColumnType::I8 => {
                let (head, tail) = buffer
                    .split_first_chunk()
                    .context("End of payload parsing cell values")?;
                *buffer = tail;
                Self::I8(i8::from_be_bytes(*head))
            }
            ColumnType::I16 => {
                let (head, tail) = buffer
                    .split_first_chunk()
                    .context("End of payload parsing cell values")?;
                *buffer = tail;
                Self::I16(i16::from_be_bytes(*head))
            }
            ColumnType::I24 => {
                let (head, tail) = buffer
                    .split_first_chunk::<3>()
                    .context("End of payload parsing cell values")?;
                *buffer = tail;
                Self::I24(i32::from_be_bytes([0, head[0], head[1], head[2]]))
            }
            ColumnType::I32 => {
                let (head, tail) = buffer
                    .split_first_chunk()
                    .context("End of payload parsing cell values")?;
                *buffer = tail;
                Self::I32(i32::from_be_bytes(*head))
            }
            ColumnType::I48 => {
                let (head, tail) = buffer
                    .split_first_chunk::<6>()
                    .context("End of payload parsing cell values")?;
                *buffer = tail;
                Self::I48(i64::from_be_bytes([
                    0, 0, head[0], head[1], head[2], head[3], head[4], head[5],
                ]))
            }
            ColumnType::I64 => {
                let (head, tail) = buffer
                    .split_first_chunk()
                    .context("End of payload parsing cell values")?;
                *buffer = tail;
                Self::I64(i64::from_be_bytes(*head))
            }
            ColumnType::F64 => {
                let (head, tail) = buffer
                    .split_first_chunk()
                    .context("End of payload parsing cell values")?;
                *buffer = tail;
                Self::F64(f64::from_be_bytes(*head))
            }
            ColumnType::Zero => Self::Zero,
            ColumnType::One => Self::One,
            ColumnType::Blob(len) => {
                let (head, tail) = buffer
                    .split_at_checked(len as usize)
                    .context("End of payload while parsing cell values")?;
                *buffer = tail;
                Self::Blob(Blob::from(head))
            }
            ColumnType::String(len) => {
                let (head, tail) = buffer.split_at_checked(len as usize).with_context(|| {
                    format!(
                        "End of payload while parsing cell values:\n{}/{} bytes for string:\n{buffer:X?}",
                        buffer.len(),
                        len
                    )
                })?;
                *buffer = tail;
                Self::String(Blob::from(head))
            }
            ColumnType::SQLiteReserved => Self::SQLiteReserved,
        })
    }
}

impl<Blob: AsRef<[u8]>> Value<Blob> {
    pub fn to_owned(&self) -> OwnedValue {
        match self {
            Self::Null => Value::Null,
            Self::I8(n) => Value::I8(*n),
            Self::I16(n) => Value::I16(*n),
            Self::I24(n) => Value::I24(*n),
            Self::I32(n) => Value::I32(*n),
            Self::I48(n) => Value::I48(*n),
            Self::I64(n) => Value::I64(*n),
            Self::F64(n) => Value::F64(*n),
            Self::Zero => Value::Zero,
            Self::One => Value::One,
            Self::Blob(blob) => Value::Blob(blob.as_ref().to_owned().into_boxed_slice()),
            Self::String(blob) => Value::String(blob.as_ref().to_owned().into_boxed_slice()),
            Self::SQLiteReserved => Value::SQLiteReserved,
        }
    }

    pub fn ty(&self) -> ColumnType {
        match self {
            Self::Null => ColumnType::Null,
            Self::I8(_) => ColumnType::I8,
            Self::I16(_) => ColumnType::I16,
            Self::I24(_) => ColumnType::I24,
            Self::I32(_) => ColumnType::I32,
            Self::I48(_) => ColumnType::I48,
            Self::I64(_) => ColumnType::I64,
            Self::F64(_) => ColumnType::F64,
            Self::Zero => ColumnType::Zero,
            Self::One => ColumnType::One,
            Self::Blob(blob) => ColumnType::Blob(blob.as_ref().len() as u64),
            Self::String(blob) => ColumnType::String(blob.as_ref().len() as u64),
            Self::SQLiteReserved => ColumnType::SQLiteReserved,
        }
    }
}
type OwnedValue = Value<Box<[u8]>>;

impl<Blob: AsRef<[u8]>> fmt::Display for Value<Blob> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("null"),
            Self::I8(n) => n.fmt(f),
            Self::I16(n) => n.fmt(f),
            Self::I24(n) | Self::I32(n) => n.fmt(f),
            Self::I48(n) | Self::I64(n) => n.fmt(f),
            Self::F64(n) => n.fmt(f),
            Self::Zero => f.write_str("0"),
            Self::One => f.write_str("1"),
            Self::Blob(blob) => write!(f, "{:X?}", blob.as_ref()),
            Self::String(blob) => {
                // TODO: Detect string type and pretty-print for real
                if let Ok(utf8) = std::str::from_utf8(blob.as_ref()) {
                    write!(f, "{utf8:?}")
                } else {
                    write!(f, "{:X?}", blob.as_ref())
                }
            }
            Self::SQLiteReserved => f.write_str("_sqlite_reserved"),
        }
    }
}

/// The values that an entry for a column might have.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Zero,
    One,
    Blob(u64),
    String(u64),
    SQLiteReserved,
}
impl ColumnType {
    fn from_numeric(n: i64) -> Self {
        match n {
            0 => Self::Null,
            1 => Self::I8,
            2 => Self::I16,
            3 => Self::I24,
            4 => Self::I32,
            5 => Self::I48,
            6 => Self::I64,
            7 => Self::F64,
            8 => Self::Zero,
            9 => Self::One,
            10 | 11 => Self::SQLiteReserved,
            _ => {
                let rem = n % 2;
                let length = (n as u64 - 12 - rem as u64) / 2;
                if rem == 0 {
                    Self::Blob(length)
                } else {
                    Self::String(length)
                }
            }
        }
    }
}
impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Null => "null",
            Self::I8 => "i8",
            Self::I16 => "i16",
            Self::I24 => "i24",
            Self::I32 => "i32",
            Self::I48 => "i48",
            Self::I64 => "i64",
            Self::F64 => "f64",
            Self::Zero => "zero",
            Self::One => "one",
            Self::Blob(_) => "blob",
            Self::String(_) => "string",
            Self::SQLiteReserved => "_sqlite_reserved",
        })
    }
}
