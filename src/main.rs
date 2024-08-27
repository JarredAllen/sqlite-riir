#![allow(clippy::print_stdout)]

use std::fs::File;

use anyhow::Context;
use sqlite_rs::{page::ParsedPage, pager::Pager};

/// Print the contents of a database file.
fn display_database(path: impl AsRef<std::path::Path>) -> anyhow::Result<()> {
    let mut pager = Pager::new(File::open(path).context("Failed to open file")?)
        .context("Failed to read database")?;
    let page_count = pager.page_count();
    println!("\n{page_count} pages:\n\n");
    for page_idx in 1..=page_count {
        match pager.read_page(page_idx) {
            Ok(page) => match page.parse() {
                ParsedPage::BTreeTableLeaf(page) => {
                    println!(
                        "Page {page_idx}: Table btree leaf with {} cells",
                        page.num_cells()
                    );
                    for cell in page.cells() {
                        println!("Cell {}:", cell.row_id());
                        for value in cell.payload().value_iter() {
                            println!("{}: {value}", value.ty());
                        }
                        println!();
                    }
                    println!();
                }
            },
            Err(e) => println!("Page {page_idx}: Error while reading:\n{e:?}"),
        }
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let file_path = std::env::args_os()
        .nth(1)
        .unwrap_or(std::ffi::OsString::from("./test-data/minimal-test.sqlite"));
    display_database(file_path)
}
