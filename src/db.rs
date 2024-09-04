//! Database implementation

use std::fs::File;

use anyhow::{Context, Result};

use crate::{pager::Pager, table_iter::TableIter};

/// A SQLite database
pub struct Database {
    /// Paging on the file
    pub(crate) pager: Pager<File>,
}

impl Database {
    pub fn new(file: File) -> Result<Self> {
        let pager = Pager::new(file).context("Failed to parse file")?;
        Ok(Self { pager })
    }

    pub fn table_names(&mut self) -> Result<impl Iterator<Item = String> + '_> {
        Ok(self
            .table_root_page_indices_by_name()?
            .map(|(name, _)| name))
    }

    pub(crate) fn table_root_page_indices_by_name(
        &mut self,
    ) -> Result<impl Iterator<Item = (String, usize)> + '_> {
        Ok([("sqlite_schema".to_owned(), 1)].into_iter().chain({
            TableIter::new(self, "sqlite_schema")?.filter_map(|cell| {
                if cell.first()?.as_str()? != "table" {
                    return None;
                }
                Some((cell.get(2)?.as_str()?.to_owned(), cell.get(3)?.as_usize()?))
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_table_root_page_indices() {
        let mut db = Database::new(
            File::open("test-data/minimal-test.sqlite").expect("Failed to open test database"),
        )
        .expect("Failed to parse test database");
        assert_eq!(
            db.table_root_page_indices_by_name()
                .expect("failed to read table list")
                .collect::<HashSet<(String, usize)>>(),
            HashSet::from_iter([
                ("sqlite_schema".to_owned(), 1),
                ("t1".to_owned(), 2),
                ("t2".to_owned(), 3),
            ]),
        );
        assert_eq!(
            db.table_names()
                .expect("failed to read table list")
                .collect::<HashSet<String>>(),
            HashSet::from_iter(["sqlite_schema".to_owned(), "t1".to_owned(), "t2".to_owned()]),
        );
    }
}
