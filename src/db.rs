//! Database implementation

use std::fs::File;

use anyhow::{Context, Result};

use crate::{pager::Pager, record::OwnedValue, table_iter::TableIter};

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

    /// Execute the given statement.
    ///
    /// For each returned value, `callback` is called.
    pub fn execute_statement(
        &mut self,
        statement: &sqlparser::ast::Statement,
        mut callback: impl FnMut(Vec<OwnedValue>) -> Result<()>,
    ) -> Result<()> {
        match statement {
            sqlparser::ast::Statement::Query(query) => {
                match query.body.as_ref() {
                    sqlparser::ast::SetExpr::Select(select) => {
                        // TODO Loosen these restrictions as I implement more of it.
                        let sqlparser::ast::Select {
                            distinct: None,
                            top: None,
                            projection,
                            into: None,
                            from,
                            lateral_views,
                            prewhere: None,
                            selection: None,
                            group_by: _, // TODO figure out this field
                            cluster_by,
                            distribute_by,
                            sort_by,
                            having: None,
                            named_window,
                            qualify: None,
                            window_before_qualify: _,
                            value_table_mode: None,
                            connect_by: None,
                        } = select.as_ref()
                        else {
                            anyhow::bail!("Unimplemented SELECT arguments");
                        };
                        if !(projection.len() == 1
                            && lateral_views.is_empty()
                            && cluster_by.is_empty()
                            && distribute_by.is_empty()
                            && sort_by.is_empty()
                            && named_window.is_empty())
                        {
                            anyhow::bail!("Unimplemented SELECT arguments 2");
                        }
                        let sqlparser::ast::SelectItem::Wildcard(
                            sqlparser::ast::WildcardAdditionalOptions {
                                opt_ilike: None,
                                opt_except: None,
                                opt_rename: None,
                                opt_exclude: None,
                                opt_replace: None,
                            },
                        ) = projection[0]
                        else {
                            anyhow::bail!("Unimplemented projection");
                        };
                        let Some(sqlparser::ast::TableWithJoins {
                            joins,
                            relation:
                                sqlparser::ast::TableFactor::Table {
                                    name: table_name,
                                    alias: None,
                                    args: None,
                                    with_hints,
                                    version: None,
                                    with_ordinality: false,
                                    partitions,
                                },
                        }) = from.first().take_if(|_| from.len() == 1)
                        else {
                            anyhow::bail!("Unimplemented FROM target");
                        };
                        if !(joins.is_empty() && with_hints.is_empty() && partitions.is_empty()) {
                            anyhow::bail!("Unimplemented FROM target");
                        }
                        let Some(table_name) =
                            table_name.0.first().take_if(|_| table_name.0.len() == 1)
                        else {
                            anyhow::bail!("Unimplemented FROM target");
                        };
                        let table_name = &table_name.value;
                        for row in TableIter::new(self, table_name)? {
                            callback(row)?;
                        }
                    }
                    _ => anyhow::bail!("Unimplemented command"),
                }
            }
            _ => anyhow::bail!("Unimplemented command"),
        }
        Ok(())
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
