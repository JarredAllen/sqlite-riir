#![allow(clippy::print_stdout)]

use std::fs::File;

use anyhow::Context;
use sqlite_riir::{page::ParsedPage, pager::Pager, Database};

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
                        page.num_cells(),
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
                ParsedPage::BTreeTableInternal(page) => {
                    println!(
                        "Page {page_idx}: Table btree internal with {} cells",
                        page.num_cells(),
                    );
                    for (idx, cell) in page.cells().enumerate() {
                        println!("Cell {idx}: ");
                        println!("Key: {}", cell.key);
                        println!("Left Child Page: {}", cell.left_child_page);
                        println!();
                    }
                    println!("Right-most child Page: {}", page.rightmost_child_idx());
                    println!();
                    println!();
                }
            },
            Err(e) => println!("Page {page_idx}: Error while reading:\n{e:?}"),
        }
    }
    Ok(())
}

fn display_tables(db: &mut Database) -> anyhow::Result<()> {
    let statement = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::SQLiteDialect {},
        "SElECT * FROM sqlite_schema",
    )
    .context("failed to parse command")?;
    anyhow::ensure!(statement.len() == 1, "command parsed unexpectedly");
    db.execute_statement(&statement[0], |table| {
        println!(
            "Table {table_name}: \"{create_command}\" @ {page_num}",
            table_name = table[2].as_str().context("invalid string in table name")?,
            create_command = table[4].as_str().context("invalid string in table name")?,
            page_num = table[3]
                .as_usize()
                .context("invalid number in table root page number")?,
        );
        Ok(())
    })?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let file_path = std::env::args_os()
        .nth(1)
        .unwrap_or(std::ffi::OsString::from("./test-data/minimal-test.sqlite"));
    let mut db = Database::new(File::open(&file_path).context("Failed to open file")?)
        .context("Failed to read database")?;
    let mut readline =
        rustyline::DefaultEditor::new().context("Error setting up readline instance")?;
    loop {
        match readline.readline("sqlite-riir>> ") {
            Ok(line) => {
                if let Some(debug_cmd) = line.strip_prefix('.') {
                    match debug_cmd {
                        "debug" => {
                            if let Err(e) = display_database(&file_path) {
                                println!(
                                    "{:?}",
                                    e.context(format!(
                                        "Error displaying database at {}",
                                        std::path::Path::new(&file_path).display()
                                    ))
                                );
                            }
                        }
                        "tables" => {
                            if let Err(e) = display_tables(&mut db) {
                                println!(
                                    "{:?}",
                                    e.context(format!(
                                        "Error displaying database at {}",
                                        std::path::Path::new(&file_path).display()
                                    ))
                                );
                            }
                        }
                        _ => println!("Unrecognized debug command: {debug_cmd:?}"),
                    }
                } else {
                    let statements = match sqlparser::parser::Parser::parse_sql(
                        &sqlparser::dialect::SQLiteDialect {},
                        &line,
                    ) {
                        Ok(cmd) => cmd,
                        Err(e) => {
                            println!(
                                "{:?}",
                                anyhow::Error::new(e).context("Error parsing command")
                            );
                            continue;
                        }
                    };
                    for statement in statements {
                        if let Err(e) = db.execute_statement(&statement, |row| {
                            println!("{row:?}");
                            Ok(())
                        }) {
                            println!("{:?}", e.context("Error running given command"));
                            break;
                        }
                    }
                }
            }
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C");
                break;
            }
            Err(e) => return Err(e).context("Failed to read command from CLI"),
        }
    }
    Ok(())
}
