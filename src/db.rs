//! Database implementation

use std::fs::File;

use anyhow::{Context, Result};

use crate::{
    page::{Page, ParsedPage},
    pager::Pager,
    record::Value,
};

/// A SQLite database
pub struct Database {
    /// Paging on the file
    pager: Pager<File>,
}

impl Database {
    pub fn new(file: File) -> Result<Self> {
        let pager = Pager::new(file)?;
        Ok(Self { pager })
    }

    pub fn table_names(&mut self) -> Result<impl Iterator<Item = String> + '_> {
        Ok(self
            .table_root_page_indices_by_name()?
            .map(|(name, _)| name))
    }

    fn table_root_page_indices_by_name(
        &mut self,
    ) -> Result<impl Iterator<Item = (String, usize)> + '_> {
        Ok([("sqlite_schema".to_owned(), 1)].into_iter().chain({
            let schema_page = self
                .root_page_for_table("sqlite_schema")
                .context("Error loading `sqlite_schema` table")?
                .context("Missing `sqlite_schema` page")?;
            let schema_page = match schema_page.parse() {
                ParsedPage::BTreeTableLeaf(leaf_page) => leaf_page,
                ParsedPage::BTreeTableInternal(_) => {
                    // TODO implement scanning of trees to list all cells
                    anyhow::bail!("too many tables to fit in one page")
                }
            };
            schema_page
                .cells()
                .filter_map(|cell| {
                    let mut values = cell.payload().value_iter();
                    if values.next()? != Value::String(&b"table"[..]) {
                        return None;
                    }
                    let table_name = match values.next()? {
                        // TODO Support non-UTF8 tables
                        Value::String(table_name) => std::str::from_utf8(table_name).ok()?,
                        _ => return None,
                    };
                    values.next();
                    let table_value = match values.next()? {
                        Value::I8(n) => usize::from(n as u8),
                        _ => return None,
                    };
                    Some((table_name.to_owned(), table_value))
                })
                .collect::<Vec<_>>()
        }))
    }

    /// Get the root page for the given table.
    fn root_page_for_table(&mut self, table_name: &str) -> Result<Option<Page<'_>>> {
        const SCHEMA_TABLE_NAMES: &[&str] = &["sqlite_schema", "sqlite_master"];
        if SCHEMA_TABLE_NAMES.contains(&table_name) {
            // schema table is always rooted at the first page
            return Ok(Some(self.pager.read_page(1)?));
        }
        let Some((_, target_page_num)) = self
            .table_root_page_indices_by_name()?
            .find(|(name, _)| name == table_name)
        else {
            return Ok(None);
        };
        Ok(Some(
            self.pager
                .read_page(target_page_num)
                .context("Error reading requested page")?,
        ))
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
