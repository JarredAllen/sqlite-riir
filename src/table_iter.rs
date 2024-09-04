//! An iterator over the rows of a table

use crate::{page::ParsedPage, record::Value, Database};

use anyhow::{Context, Result};

pub struct TableIter<'a> {
    db: &'a mut Database,
    stack: Vec<StackFrame>,
}

impl<'a> TableIter<'a> {
    pub fn new(db: &'a mut Database, table_name: &str) -> Result<Self> {
        const SCHEMA_TABLE_NAMES: &[&str] = &["sqlite_schema", "sqlite_master"];
        let root_page_num = if SCHEMA_TABLE_NAMES.contains(&table_name) {
            // schema table is always rooted at the first page
            1
        } else {
            db.table_root_page_indices_by_name()?
                .find(|(name, _)| name == table_name)
                .with_context(|| format!("Failed to find table {table_name}"))?
                .1
        };
        Ok(Self {
            db,
            stack: vec![StackFrame {
                page_num: root_page_num,
                idx_in_page: 0,
            }],
        })
    }
}

impl<'a> Iterator for TableIter<'a> {
    type Item = Vec<Value<Box<[u8]>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let stack_len = self.stack.len();
        let top_frame = self.stack.get_mut(stack_len.checked_sub(1)?)?;
        let page = self
            .db
            .pager
            .read_page(top_frame.page_num)
            .expect("Error reading pages");
        match page.parse() {
            ParsedPage::BTreeTableInternal(internal) => {
                // If the top page is an internal node, we set the top of the stack to the next
                // page to look in, and then recurse.
                if let Some(cell) = internal.cells().nth(top_frame.idx_in_page) {
                    top_frame.idx_in_page = top_frame.idx_in_page.saturating_add(1);
                    self.stack.push(StackFrame {
                        page_num: cell.left_child_page as usize,
                        idx_in_page: 0,
                    });
                } else if top_frame.idx_in_page == internal.num_cells() {
                    top_frame.idx_in_page = top_frame.idx_in_page.saturating_add(1);
                    // Minor optimization: this is the last child, so we can replace the top stack
                    // frame with its child instead of pushing it on top.
                    *top_frame = StackFrame {
                        page_num: internal.rightmost_child_idx() as usize,
                        idx_in_page: 0,
                    };
                } else {
                    // We're past the end of this page, so we remove the top frame and recurse into
                    // the parent (should never be hit because of above optimization).
                    self.stack.pop();
                }
                // We have a different stack frame on top (pointing to a different page), so we can
                // recurse. This should get tail-call optimization, but even if it doesn't, we only
                // have a pointer on the stack at this point, so it shouldn't explode the stack too
                // much.
                self.next()
            }
            ParsedPage::BTreeTableLeaf(leaf) => {
                let Some(cell) = leaf.cells().nth(top_frame.idx_in_page) else {
                    self.stack.pop();
                    return self.next();
                };
                top_frame.idx_in_page = top_frame.idx_in_page.saturating_add(1);
                Some(
                    cell.payload()
                        .value_iter()
                        .map(|value| value.to_owned())
                        .collect(),
                )
            }
        }
    }
}

struct StackFrame {
    page_num: usize,
    idx_in_page: usize,
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[test]
    fn test_with_leaf_root() {
        let mut db = Database::new(
            File::open("./test-data/minimal-test.sqlite").expect("Failed to open database file"),
        )
        .expect("Failed to parse database file as database");
        assert_eq!(
            TableIter::new(&mut db, "sqlite_schema")
                .expect("Failed to make iterator")
                .count(),
            2,
        );
        assert_eq!(
            TableIter::new(&mut db, "t1")
                .expect("Failed to make iterator")
                .count(),
            0,
        );
        assert_eq!(
            TableIter::new(&mut db, "t2")
                .expect("Failed to make iterator")
                .count(),
            0,
        );
    }

    #[test]
    fn test_two_layers_tree() {
        let mut db = Database::new(
            File::open("./test-data/many-tables.sqlite").expect("Failed to open database file"),
        )
        .expect("Failed to parse database file as database");
        assert_eq!(
            TableIter::new(&mut db, "sqlite_schema")
                .expect("Failed to make iterator")
                .count(),
            1024,
        );
    }
}
