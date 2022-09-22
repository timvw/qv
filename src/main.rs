use anyhow::Result;
use aws_types::credentials::*;
use aws_types::SdkConfig;
use clap::Parser;
use datafusion::prelude::*;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use url::Url;

mod args;
mod globbing_path;

use crate::args::Args;
use crate::globbing_path::GlobbingPath;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let data_location = update_s3_console_url(&args.path);
    let globbing_path = GlobbingPath::parse(&data_location)?;

    if let Some(aws_profile) = &args.profile {
        env::set_var("AWS_PROFILE", aws_profile);
    }

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);
    register_object_store(&ctx, &globbing_path).await?;

    let table_arc = globbing_path.build_table_provider(&ctx).await?;
    ctx.register_table("tbl", table_arc)?;

    let query = &args.get_query();
    let df = ctx.sql(query).await?;
    df.show_limit(args.limit).await?;

    Ok(())
}

/// When the provided s looks like an https url from the amazon webui convert it to an s3:// url
/// When the provided s does not like such url, return it as is.
fn update_s3_console_url(s: &str) -> String {
    if s.starts_with("https://s3.console.aws.amazon.com/s3/buckets/") {
        let parsed_url = Url::parse(s).unwrap_or_else(|_| panic!("Failed to parse {}", s));
        let path_segments = parsed_url
            .path_segments()
            .map(|c| c.collect::<Vec<_>>())
            .unwrap_or_default();
        if path_segments.len() == 3 {
            let bucket_name = path_segments[2];
            let params: HashMap<String, String> = parsed_url
                .query()
                .map(|v| {
                    url::form_urlencoded::parse(v.as_bytes())
                        .into_owned()
                        .collect()
                })
                .unwrap_or_else(HashMap::new);
            params
                .get("prefix")
                .map(|prefix| format!("s3://{}/{}", bucket_name, prefix))
                .unwrap_or_else(|| s.to_string())
        } else {
            s.to_string()
        }
    } else {
        s.to_string()
    }
}

#[test]
fn test_update_s3_console_url() -> Result<()> {
    assert_eq!(
        update_s3_console_url("/Users/timvw/test"),
        "/Users/timvw/test"
    );
    assert_eq!(update_s3_console_url("https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?region=eu-central-1&prefix=COVID-19_NYT/&showversions=false"), "s3://datafusion-delta-testing/COVID-19_NYT/");
    assert_eq!(update_s3_console_url("https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?prefix=COVID-19_NYT/&region=eu-central-1"), "s3://datafusion-delta-testing/COVID-19_NYT/");
    Ok(())
}

async fn register_object_store(ctx: &SessionContext, globbing_path: &GlobbingPath) -> Result<()> {
    if globbing_path.object_store_url.as_str().starts_with("s3://") {
        let bucket_name = String::from(
            Url::parse(globbing_path.object_store_url.as_str())
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
