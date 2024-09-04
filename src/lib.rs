use anyhow::{Context, Result};

// `rustyline` is needed for the CLI interface
use rustyline as _;

mod db;
pub mod page;
pub mod pager;
pub mod record;
pub mod table_iter;

pub use db::Database;

/// Parse a variable-length integer
fn parse_varint(buffer: &mut &[u8]) -> Result<i64> {
    let mut acc = 0;
    let mut length = 0;
    loop {
        let new_byte = buffer
            .get(length)
            .context("Unexpected end of buffer inside varint")?;
        if length == 8 {
            acc |= i64::from(*new_byte) << 56;
            break;
        }
        acc |= i64::from(new_byte & 0x7F) << (length * 7);
        length += 1;
        if new_byte & 0x80 == 0 {
            break;
        }
    }
    *buffer = &buffer[length..];
    Ok(acc)
}
