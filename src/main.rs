use std::fs::File;

use sqlite_rs::{page::ParsedPage, pager::Pager};

fn main() {
    let mut pager =
        Pager::new(File::open("./test-data/minimal-test.sqlite").expect("Failed to open file"))
            .expect("Failed to read database");
    let page_count = pager.page_count().expect("Failed to read page count");
    println!("{page_count} pages:");
    for page_idx in 0..page_count {
        match pager.read_page(page_idx) {
            Ok(page) => match page.parse() {
                ParsedPage::BTreeTableLeaf(page) => {
                    println!(
                        "Page {page_idx}: Table btree leaf with {} cells",
                        page.num_cells()
                    );
                    for (page_idx, page) in page.cells().enumerate() {
                        println!("Cell {page_idx}: {page:?}");
                    }
                    println!();
                }
            },
            Err(e) => println!("Page {page_idx}: Error while reading:\n{e:?}"),
        }
    }
}
