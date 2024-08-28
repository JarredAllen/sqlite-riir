//! Implementation for btree table internal pages

use anyhow::{Context, Result};

use crate::{page::PageType, parse_varint};

/// A parsed leaf in a table's btree
pub struct BTreeTableInternalPage<'a> {
    /// The header for the page
    header: super::BTreePageHeader,
    /// The page number of the subtree root containing records with greater keys.
    rightmost_pointer: u32,
    /// The pointers to cells
    ///
    /// Per SQLite format, you need to subtract the cell content offset in [`Self::header`] first
    /// and then you can index into [`Self::cell_contents`].
    cell_pointers: &'a [u8],
    /// The contents of the cells
    cell_contents: &'a [u8],
}

impl<'a> BTreeTableInternalPage<'a> {
    pub(super) fn new(contents: &'a [u8]) -> Result<Self> {
        let (page_type, header, header_len) = super::BTreePageHeader::parse(contents)?;
        let rightmost_pointer = u32::from_be_bytes([
            contents[header_len],
            contents[header_len + 1],
            contents[header_len + 2],
            contents[header_len + 3],
        ]);
        let body = &contents[header_len + 4..];
        anyhow::ensure!(page_type == PageType::BTreeTableInternal, "Wrong page type");
        let cell_pointers = body
            .get(..header.cell_count as usize * 2)
            .context("Unexpected end of page in cell pointer array")?;
        let cell_contents = contents
            .get(header.cell_content_offset as usize..)
            .context("Unexpected end of page in cell contents")?;
        Ok(Self {
            header,
            rightmost_pointer,
            cell_pointers,
            cell_contents,
        })
    }

    /// Get the index of the rightmost (greatest) child page.
    #[must_use]
    pub fn rightmost_child_idx(&self) -> u32 {
        self.rightmost_pointer
    }

    /// Get the number of cells in this page
    #[must_use]
    pub fn num_cells(&self) -> usize {
        self.header.cell_count as usize
    }

    pub fn cells(&'a self) -> impl Iterator<Item = Cell> + 'a {
        CellIter { page: self, idx: 0 }
    }
}

pub struct Cell {
    pub left_child_page: u32,
    pub key: i64,
}
impl Cell {
    fn parse(contents: &[u8]) -> Result<Self> {
        let left_child_page = u32::from_be_bytes(
            <[u8; 4]>::try_from(contents.get(..4).context("cell too short")?)
                .context("cell too short")?,
        );
        let key = parse_varint(&mut contents.get(4..).context("cell too short")?)?;
        Ok(Self {
            left_child_page,
            key,
        })
    }
}

/// An iterator over the cells in a page.
struct CellIter<'a> {
    /// The page we're iterating over
    page: &'a BTreeTableInternalPage<'a>,
    /// The index of iteration
    idx: usize,
}
impl<'a> Iterator for CellIter<'a> {
    type Item = Cell;

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
        Some(Cell::parse(&self.page.cell_contents[pointer as usize..]).expect("Failed to parse"))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.page.num_cells().saturating_sub(self.idx);
        (len, Some(len))
    }

    fn count(self) -> usize {
        self.size_hint().0
    }
}
