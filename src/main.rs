#![allow(clippy::print_stdout)]

use std::fs::File;

use anyhow::Context;
use sqlite_rs::{page::ParsedPage, pager::Pager};

fn main() -> anyhow::Result<()> {
    let mut pager =
        Pager::new(File::open("./test-data/minimal-test.sqlite").context("Failed to open file")?)
            .context("Failed to read database")?;
    let page_count = pager.page_count();
    println!("\n{page_count} pages:\n\n");
    for page_idx in 0..page_count {
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
