use clap::Parser;
use datafusion::catalog::TableReference;
use std::sync::Arc;
//use datafusion::catalog::TableReference;

use datafusion::common::Result;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;

mod args;
mod globbing_path;
mod globbing_table;
mod object_store_util;

use crate::args::Args;
//use crate::globbing_path::GlobbingPath;
//use crate::globbing_table::build_table_provider;
//use crate::object_store_util::register_object_store;

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let args: Args = Args::parse();
    //let globbing_path = args.get_globbing_path().await?;
    //register_object_store(&ctx, &globbing_path.object_store_url).await?;

    let table_path = ListingTableUrl::parse(&args.path)?;
    let mut config = ListingTableConfig::new(table_path);
    config = config.infer_options(&ctx.state()).await?;
    config = config.infer_schema(&ctx.state()).await?;

    let table = ListingTable::try_new(config)?;

    ctx.register_table(
        TableReference::from("datafusion.public.tbl"),
        Arc::new(table),
    )?;

    let query = &args.get_query();
    let df = ctx.sql(query).await?;
    if args.schema {
        df.show().await?;
    } else {
        df.show_limit(args.limit).await?;
    }

    Ok(())
}
