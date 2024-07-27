//! Implementation of the various page types

pub mod btree_table_leaf;

use std::num::NonZeroU16;

use anyhow::{Context, Result};

use crate::pager::DATABASE_HEADER_SIZE;

/// A validated page
///
/// This can contain any type of page inside.
pub struct Page<'a> {
    /// The byte buffer it points at
    contents: &'a mut [u8],
}

impl<'a> Page<'a> {
    pub(crate) fn new(contents: &'a mut [u8]) -> Result<Self> {
        let maybe_self = Self { contents };
        // Ensure that it parses correctly
        maybe_self.parse_checked()?;
        Ok(maybe_self)
    }

    #[must_use]
    pub fn parse(&self) -> ParsedPage {
        // We ensure the parse succeeds in the type invariants
        self.parse_checked().unwrap()
    }

    /// Parse `self`, returning an error if we fail to parse.
    ///
    /// After constructing a [`Page`], use [`Self::parse`] instead, which leans on type invariants
    /// to ensure that it parses correctly.
    fn parse_checked(&self) -> Result<ParsedPage> {
        // TODO Don't assume all pages are btree pages
        let (page_type, ..) = BTreePageHeader::parse(self.contents)?;
        match page_type {
            PageType::BTreeTableLeaf => btree_table_leaf::BTreeTableLeafPage::new(self.contents)
                .map(ParsedPage::BTreeTableLeaf),
        }
    }
}

/// An enum of all page types.
pub enum ParsedPage<'a> {
    /// A leaf in the table btree.
    BTreeTableLeaf(btree_table_leaf::BTreeTableLeafPage<'a>),
}

/// The page types
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PageType {
    /// A leaf in the table btree.
    BTreeTableLeaf,
}
impl PageType {
    fn from_header_byte(byte: u8) -> Result<Self> {
        Ok(match byte {
            0x0d => Self::BTreeTableLeaf,
            _ => anyhow::bail!("Unrecognized header byte: {byte}"),
        })
    }
}

const BTREE_PAGE_HEADER_SIZE: usize = 8;

/// The header at the start of every btree page
#[derive(Debug)]
struct BTreePageHeader {
    /// The start of the first free block in the page, if we have one
    _first_free_block: Option<NonZeroU16>,
    /// The number of cells in this page
    cell_count: u16,
    /// The offset at which content starts
    cell_content_offset: u32,
    /// The number of fragmented free bytes in the content area
    _fragmented_bytes_count: u8,
}
impl BTreePageHeader {
    fn parse(buffer: &[u8]) -> Result<(PageType, Self, usize)> {
        let (parse_from_arr, total_len): (&[u8; BTREE_PAGE_HEADER_SIZE], usize) =
            if buffer[0] == b'S' {
                // The first page contains the database header, so we cut that off
                (
                    buffer
                        .get(DATABASE_HEADER_SIZE..DATABASE_HEADER_SIZE + BTREE_PAGE_HEADER_SIZE)
                        .context("Unexpected end of page")?
                        .try_into()
                        .unwrap(),
                    DATABASE_HEADER_SIZE + BTREE_PAGE_HEADER_SIZE,
                )
            } else {
                (
                    buffer
                        .get(..BTREE_PAGE_HEADER_SIZE)
                        .context("Unexpected end of page")?
                        .try_into()
                        .unwrap(),
                    BTREE_PAGE_HEADER_SIZE,
                )
            };
        let page_type = PageType::from_header_byte(parse_from_arr[0])?;
        let first_free_block =
            NonZeroU16::new(u16::from_be_bytes([parse_from_arr[1], parse_from_arr[2]]));
        let cell_count = u16::from_be_bytes([parse_from_arr[3], parse_from_arr[4]]);
        let cell_content_offset_raw = u16::from_be_bytes([parse_from_arr[5], parse_from_arr[6]]);
        let cell_content_offset = match cell_content_offset_raw {
            0 => 65536,
            _ => u32::from(cell_content_offset_raw),
        };
        let fragmented_bytes_count = parse_from_arr[7];
        Ok((
            page_type,
            Self {
                _first_free_block: first_free_block,
                cell_count,
                cell_content_offset,
                _fragmented_bytes_count: fragmented_bytes_count,
            },
            total_len,
        ))
    }
}
