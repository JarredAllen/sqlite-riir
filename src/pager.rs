//! A pager to control reading pages from disk and writing them back.

use anyhow::{Context, Result};
use std::{
    collections::{hash_map, HashMap},
    io::{self, Read, Seek},
    ptr::NonNull,
};

use crate::page::Page;

/// The pager itself
pub struct Pager<File> {
    /// The file to read pages from
    file: File,
    /// The header for this database
    header: DatabaseHeader,
    /// The page cache.
    page_cache: PageCache,
}
impl<File: Read> Pager<File> {
    /// Construct a new pager over the given file.
    ///
    /// We assume that the file is currently at the beginning, this function may behave
    /// unexpectedly otherwise.
    pub fn new(mut file: File) -> Result<Self> {
        let header = {
            let mut buf = [0; DATABASE_HEADER_SIZE];
            file.read_exact(&mut buf)
                .context("Error reading database header from file")?;
            DatabaseHeader::parse(&buf)?
        };
        Ok(Self {
            file,
            header,
            page_cache: PageCache::new(header.page_size()),
        })
    }
}
impl<File: Read + Seek> Pager<File> {
    /// Read the given page.
    pub fn read_page(&mut self, page_idx: usize) -> Result<Page> {
        let buffer = self.page_cache.get_or_load(page_idx, |buf, page_idx| {
            self.file
                .seek(io::SeekFrom::Start(
                    (self.header.page_size() * page_idx) as u64,
                ))
                .context("Error seeking in database")?;
            self.file
                .read_exact(buf)
                .context("Error reading from database file")?;
            Ok(())
        })?;
        Page::new(buffer)
    }

    /// Return the number of pages in the database.
    pub fn page_count(&mut self) -> Result<usize> {
        let file_len = self.file.seek(io::SeekFrom::End(0))?;
        anyhow::ensure!(
            (file_len as usize) % self.header.page_size() == 0,
            "Unexpected file length"
        );
        Ok((file_len as usize) / self.header.page_size())
    }
}

/// The size of the database header.
pub const DATABASE_HEADER_SIZE: usize = 100;

#[derive(Copy, Clone, Debug)]
struct DatabaseHeader {
    /// $\log_2$ of the page size.
    page_size_exp: u8,
}
impl DatabaseHeader {
    fn parse(buffer: &[u8; DATABASE_HEADER_SIZE]) -> Result<Self> {
        anyhow::ensure!(
            buffer.starts_with(b"SQLite format 3\0"),
            "File did not begin with header, is it a SQLite database?"
        );
        let page_size_raw = u16::from_be_bytes(buffer[16..18].try_into().unwrap());
        let page_size_exp = match page_size_raw {
            0 => 16,
            n if n.is_power_of_two() => n.ilog2() as u8,
            _ => anyhow::bail!("Invalid page size value in header"),
        };
        Ok(Self { page_size_exp })
    }

    fn page_size(&self) -> usize {
        1 << usize::from(self.page_size_exp)
    }
}

struct PageCache {
    page_size: usize,
    /// The entries in the cache.
    ///
    /// TODO This cache has no eviction policy and will grow without bound.
    ///
    /// # SAFETY
    /// Each entry must always point to an address which starts a byte array of length
    /// `self.page_size`.
    entries: HashMap<usize, NonNull<u8>>,
}
impl PageCache {
    fn new(page_size: usize) -> Self {
        Self {
            page_size,
            entries: HashMap::new(),
        }
    }

    /// Get the page at the given index, loading if required.
    ///
    /// # Arguments
    /// * `page_idx`: The index number of the page being loaded.
    /// * `loader`: A function that reads into the given buffer the given page number.
    fn get_or_load(
        &mut self,
        page_idx: usize,
        loader: impl FnOnce(&mut [u8], usize) -> Result<()>,
    ) -> Result<&mut [u8]> {
        let raw_ptr = match self.entries.entry(page_idx) {
            hash_map::Entry::Occupied(slot) => slot.get().as_ptr(),
            hash_map::Entry::Vacant(slot) => {
                let mut buffer = vec![0; self.page_size].into_boxed_slice();
                loader(&mut buffer, page_idx).context("Failed to read from buffer")?;
                let ptr = Box::leak(buffer);
                slot.insert(NonNull::from(ptr).cast::<u8>()).as_ptr()
            }
        };
        // SAFETY: `self.entries` only contains pointers to pages of `self.page_size` size.
        Ok(unsafe { std::slice::from_raw_parts_mut(raw_ptr, self.page_size) })
    }
}
