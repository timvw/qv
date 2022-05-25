use anyhow::Result;
use camino::Utf8PathBuf;
use clap::Parser;
use datafusion::datafusion_data_access::object_store::local::LocalFileSystem;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::prelude::*;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Location where the data is located
    path: Utf8PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let ctx = SessionContext::new();

    let fs = LocalFileSystem {};
    let fs_arc = Arc::new(fs);
    let config = ListingTableConfig::new(fs_arc, &args.path.into_string())
        .infer()
        .await?;
    let table = ListingTable::try_new(config)?;
    ctx.register_table("tbl", Arc::new(table))?;

    let df = ctx.sql("SELECT * FROM tbl").await?;
    df.show_limit(10).await?;

    Ok(())
}
