use anyhow::Result;
use camino::Utf8PathBuf;
use clap::Parser;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::prelude::*;
use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Location where the data is located
    path: Utf8PathBuf,

    /// Query to execute
    #[clap(short, long, default_value_t = String::from("select * from tbl"), group = "sql")]
    query: String,

    /// When provided the schema is shown
    #[clap(short, long, group = "sql")]
    schema: bool,

    /// Rows to return
    #[clap(short, long, default_value_t = 10)]
    limit: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let s3_fs = Arc::new(S3FileSystem::default().await);
    ctx.runtime_env().register_object_store("s3", s3_fs);

    let (fs, path) = ctx
        .runtime_env()
        .object_store_registry
        .get_by_uri(args.path.as_str())?;

    let config = ListingTableConfig::new(fs, path).infer().await?;
    let table = ListingTable::try_new(config)?;
    ctx.register_table("tbl", Arc::new(table))?;

    let query = if args.schema {
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'tbl'"
    } else {
        args.query.as_str()
    };

    let df = ctx.sql(query).await?;
    df.show_limit(args.limit).await?;

    Ok(())
}
