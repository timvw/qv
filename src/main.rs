mod args;
mod globbing_path;
mod globbing_table;
mod object_store_util;

use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_glue::Client;
use clap::Parser;
use datafusion::catalog::TableReference;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use aws_types::SdkConfig;

use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::ObjectStore;
use regex::Regex;
use url::Url;

use crate::args::Args;

async fn build_s3(url: &Url, sdk_config: &SdkConfig) -> Result<AmazonS3> {
    let cp = sdk_config.credentials_provider().unwrap();
    let creds = cp
        .provide_credentials()
        .await
        .map_err(|e| DataFusionError::Execution(format!("Failed to get credentials: {e}")))?;

    let bucket_name = url.host_str().unwrap();

    let builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
        .with_access_key_id(creds.access_key_id())
        .with_secret_access_key(creds.secret_access_key());

    let builder = if let Some(session_token) = creds.session_token() {
        builder.with_token(session_token)
    } else {
        builder
    };

    //https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
    let builder = if let Ok(aws_endpoint_url) = env::var("AWS_ENDPOINT_URL") {
        builder.with_endpoint(&aws_endpoint_url)
    } else {
        builder
    };

    let s3 = builder.build()?;

    Ok(s3)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let args: Args = Args::parse();

    let (_, data_path) = replace_s3_console_url_with_s3_path(&args.path.clone());

    let sdk_config = get_sdk_config(&args).await;

    let data_path = if data_path.starts_with("s3://") {
        // register s3 object store
        let s3_url = Url::parse(&data_path)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse url, {e}")))?;
        let s3 = build_s3(&s3_url, &sdk_config).await?;
        let s3_arc = Arc::new(s3);
        ctx.runtime_env()
            .register_object_store(&s3_url, s3_arc.clone());

        // add trailing slash to folder
        if !data_path.ends_with('/') {
            let path = Path::parse(s3_url.path())?;
            if s3_arc.head(&path).await.is_err() {
                format!("{data_path}/")
            } else {
                data_path
            }
        } else {
            data_path
        }
    } else {
        data_path
    };

    let table_path = ListingTableUrl::parse(data_path)?;
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

async fn get_sdk_config(args: &Args) -> SdkConfig {
    set_aws_profile_when_needed(args);
    set_aws_region_when_needed();

    aws_config::load_defaults(BehaviorVersion::latest()).await
}

fn set_aws_profile_when_needed(args: &Args) {
    if let Some(aws_profile) = &args.profile {
        env::set_var("AWS_PROFILE", aws_profile);
    }
}

fn set_aws_region_when_needed() {
    match env::var("AWS_DEFAULT_REGION") {
        Ok(_) => {}
        Err(_) => env::set_var("AWS_DEFAULT_REGION", "eu-central-1"),
    }
}

/// When the provided s looks like an https url from the amazon webui convert it to an s3:// url
/// When the provided s does not like such url, return it as is.
fn replace_s3_console_url_with_s3_path(s: &str) -> (bool, String) {
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
                .unwrap_or_default();
            params
                .get("prefix")
                .map(|prefix| format!("s3://{}/{}", bucket_name, prefix))
                .map(|x| (true, x))
                .unwrap_or_else(|| (false, s.to_string()))
        } else {
            (false, s.to_string())
        }
    } else {
        (false, s.to_string())
    }
}

#[test]
fn test_replace_s3_console_url_with_s3_path() -> Result<()> {
    assert_eq!(
        replace_s3_console_url_with_s3_path("/Users/timvw/test"),
        (false, "/Users/timvw/test".to_string())
    );
    assert_eq!(replace_s3_console_url_with_s3_path("https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?region=eu-central-1&prefix=COVID-19_NYT/&showversions=false"), (true, "s3://datafusion-delta-testing/COVID-19_NYT/".to_string()));
    assert_eq!(replace_s3_console_url_with_s3_path("https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?prefix=COVID-19_NYT/&region=eu-central-1"), (true, "s3://datafusion-delta-testing/COVID-19_NYT/".to_string()));
    Ok(())
}

fn parse_glue_url(s: &str) -> Option<(String, String)> {
    let re: Regex = Regex::new(r"^glue://(\w+)\.(\w+)$").unwrap();
    re.captures(s).map(|captures| {
        let database_name = &captures[1];
        let table_name = &captures[2];
        (database_name.to_string(), table_name.to_string())
    })
}

#[test]
fn test_parse_glue_url() {
    assert_eq!(None, parse_glue_url("file:///a"));
    assert_eq!(
        Some(("db".to_string(), "table".to_string())),
        parse_glue_url("glue://db.table")
    );
}

async fn get_storage_location(
    sdk_config: &SdkConfig,
    database_name: &str,
    table_name: &str,
) -> Result<String> {
    let client: Client = Client::new(sdk_config);
    let table = client
        .get_table()
        .set_database_name(Some(database_name.to_string()))
        .set_name(Some(table_name.to_string()))
        .send()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .table
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Could not find {}.{} in glue",
                database_name, table_name
            ))
        })?;
    let location = table
        .storage_descriptor()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Could not find storage descriptor for {}.{} in glue",
                database_name, table_name
            ))
        })?
        .location()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Could not find sd.location for {}.{} in glue",
                database_name, table_name
            ))
        })?;
    Ok(location.to_string())
}
