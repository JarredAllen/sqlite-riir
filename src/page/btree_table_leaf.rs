//! Implementation for btree table leaf pages

use anyhow::{Context, Result};

use crate::page::PageType;

pub struct BTreeTableLeafPage<'a> {
    header: super::BTreePageHeader,
    cell_pointers: &'a [u8],
    cell_contents: &'a [u8],
}

impl<'a> BTreeTableLeafPage<'a> {
    pub(super) fn new(contents: &'a [u8]) -> Result<Self> {
        let (page_type, header, header_len) = super::BTreePageHeader::parse(contents)?;
        let body = &contents[header_len..];
        anyhow::ensure!(page_type == PageType::BTreeTableLeaf, "Wrong page type");
        let cell_pointers = body
            .get(..header.cell_count as usize * 2)
            .context("Unexpected end of page in cell pointer array")?;
        let cell_contents = contents
            .get(header.cell_content_offset as usize..)
            .context("Unexpected end of page in cell contents")?;
        Ok(Self {
            header,
            cell_pointers,
            cell_contents,
        })
    }

    /// Get the number of cells in this page
    pub fn num_cells(&self) -> usize {
        self.header.cell_count as usize
    }

    pub fn cells(&'a self) -> impl Iterator<Item = Cell<'a>> + 'a {
        CellIter { page: self, idx: 0 }
    }
}

#[derive(Debug)]
pub struct Cell<'a> {
    contents: &'a [u8],
}

struct CellIter<'a> {
    page: &'a BTreeTableLeafPage<'a>,
    idx: usize,
}
impl<'a> Iterator for CellIter<'a> {
    type Item = Cell<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx * 2 >= self.page.cell_pointers.len() {
            return None;
        }
        // TODO Error checking
        let pointer_bytes = [
            self.page.cell_pointers[self.idx * 2],
            self.page.cell_pointers[self.idx * 2 + 1],
        ];
        self.idx += 1;
        let pointer =
            u16::from_be_bytes(pointer_bytes) - self.page.header.cell_content_offset as u16;
        Some(parse_cell(&self.page.cell_contents[pointer as usize..]).expect("Failed to parse"))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.page.num_cells().saturating_sub(self.idx);
        (len, Some(len))
    }

    fn count(self) -> usize {
        self.size_hint().0
    }
}

fn parse_cell(mut buffer: &[u8]) -> Result<Cell<'_>> {
    let length = parse_varint(&mut buffer)?;
    Ok(Cell {
        contents: &buffer
            .get(..length as usize)
            .context("Unexpected end of page")?,
    })
}

fn parse_varint(buffer: &mut &[u8]) -> Result<i64> {
    let mut acc = 0;
    let mut length = 0;
    loop {
        let new_byte = buffer.get(length).context("Unexpected end of page")?;
        if length == 8 {
            acc |= i64::from(*new_byte) << 56;
            break;
        } else {
            acc |= i64::from(new_byte & 0x7F) << length * 7;
            length += 1;
            if new_byte & 0x80 == 0 {
                break;
            }
        }
    }
    *buffer = &buffer[length..];
    Ok(acc)
}
