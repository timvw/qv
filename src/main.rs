use anyhow::Result;
use aws_types::credentials::*;
use aws_types::SdkConfig;
use clap::Parser;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::*;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::env;
use std::sync::Arc;
use url::Url;

mod args;
mod globbing_path;

use crate::args::Args;
use crate::globbing_path::GlobbingPath;

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let args = Args::parse();
    set_aws_profile_when_needed(&args);
    let globbing_path = args.get_globbing_path()?;
    register_object_store(&ctx, &globbing_path.object_store_url).await?;

    let table_arc = globbing_path.build_table_provider(&ctx).await?;
    ctx.register_table("tbl", table_arc)?;

    let query = &args.get_query();
    let df = ctx.sql(query).await?;
    df.show_limit(args.limit).await?;

    Ok(())
}

fn set_aws_profile_when_needed(args: &Args) {
    if let Some(aws_profile) = &args.profile {
        env::set_var("AWS_PROFILE", aws_profile);
    }
}

async fn register_object_store(
    ctx: &SessionContext,
    object_store_url: &ObjectStoreUrl,
) -> Result<()> {
    if object_store_url.as_str().starts_with("s3://") {
        let bucket_name = String::from(
            Url::parse(object_store_url.as_str())
                .expect("failed to parse object_store_url")
                .host_str()
                .expect("failed to extract host/bucket from path"),
        );

        let sdk_config = aws_config::load_from_env().await;
        let s3 = build_s3_from_sdk_config(&bucket_name, &sdk_config).await?;
        ctx.runtime_env()
            .register_object_store("s3", &bucket_name, Arc::new(s3));
    }
    Ok(())
}

async fn build_s3_from_sdk_config(bucket_name: &str, sdk_config: &SdkConfig) -> Result<AmazonS3> {
    let credentials_providder = sdk_config
        .credentials_provider()
        .expect("could not find credentials provider");
    let credentials = credentials_providder
        .provide_credentials()
        .await
        .expect("could not load credentials");

    let s3_builder = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(
            sdk_config
                .region()
                .expect("could not find region")
                .to_string(),
        )
        .with_access_key_id(credentials.access_key_id())
        .with_secret_access_key(credentials.secret_access_key());

    let s3 = match credentials.session_token() {
        Some(session_token) => s3_builder.with_token(session_token),
        None => s3_builder,
    }
    .build()?;

    Ok(s3)
}
