//! Implementation for btree table leaf pages

use anyhow::{Context, Result};

use crate::{page::PageType, parse_varint, record::Record};

/// A parsed leaf in a table's btree
pub struct BTreeTableLeafPage<'a> {
    /// The header for the page
    header: super::BTreePageHeader,
    /// The pointers to cells
    ///
    /// Per SQLite format, you need to subtract the cell content offset in [`Self::header`] first
    /// and then you can index into [`Self::cell_contents`].
    cell_pointers: &'a [u8],
    /// The contents of the cells
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
    #[must_use]
    pub fn num_cells(&self) -> usize {
        self.header.cell_count as usize
    }

    pub fn cells(&'a self) -> impl Iterator<Item = Cell<'a>> + 'a {
        CellIter { page: self, idx: 0 }
    }
}

pub struct Cell<'a> {
    row_id: i64,
    record: Record<'a>,
    // TODO Handle cells too large to fit in a page
}
impl<'a> Cell<'a> {
    fn new(length: usize, mut contents: &'a [u8]) -> Result<Self> {
        let row_id = parse_varint(&mut contents)?;
        let contents = contents
            .get(..length)
            .context("Unexpected end of contents")?;
        Ok(Self {
            row_id,
            record: Record::parse(contents)?,
        })
    }

    /// Get the row ID for this cell
    #[must_use]
    pub fn row_id(&self) -> i64 {
        self.row_id
    }

    /// Get the payload bytes of this cell
    #[must_use]
    pub fn payload(&self) -> Record<'a> {
        self.record
    }
}

/// An iterator over the cells in a page.
struct CellIter<'a> {
    /// The page we're iterating over
    page: &'a BTreeTableLeafPage<'a>,
    /// The index of iteration
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

/// Parse a cell from the given buffer
fn parse_cell(mut buffer: &[u8]) -> Result<Cell<'_>> {
    let length = parse_varint(&mut buffer)? as usize;
    Cell::new(length, buffer)
}
