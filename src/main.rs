use clap::Parser;
use datafusion::catalog::TableReference;
use std::env;
use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;

mod args;
mod globbing_path;
mod globbing_table;
mod object_store_util;

use crate::args::Args;

use object_store_opendal::OpendalStore;
use opendal::services::{Gcs, S3};
use opendal::Operator;
use url::Url;

fn init_s3_operator_via_builder(url: &Url) -> Result<Operator> {
    let mut builder = S3::default();
    let bucket_name = url.host_str().unwrap();
    builder.bucket(bucket_name);

    //https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html

    if let Ok(aws_endpoint_url) = env::var("AWS_ENDPOINT_URL") {
        builder.endpoint(&aws_endpoint_url);
    }

    let op = Operator::new(builder)
        .map_err(|e| DataFusionError::Execution(format!("Failed to build operator: {e}")))?
        .finish();
    Ok(op)
}

fn init_gcs_operator_via_builder(url: &Url) -> Result<Operator> {
    let mut builder = Gcs::default();

    let op = Operator::new(builder)
        .map_err(|e| DataFusionError::Execution(format!("Failed to build operator: {e}")))?
        .finish();
    Ok(op)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let args: Args = Args::parse();

    let data_path = &args.path.clone();

    if data_path.starts_with("s3://") {
        let s3_url = Url::parse(data_path)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse url, {e}")))?;
        let op = init_s3_operator_via_builder(&s3_url)?;
        ctx.runtime_env()
            .register_object_store(&s3_url, Arc::new(OpendalStore::new(op)));
    }

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
