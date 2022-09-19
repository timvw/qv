use anyhow::Result;
use aws_types::credentials::*;
use aws_types::SdkConfig;
use clap::Parser;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::*;
use deltalake::datafusion::datasource::TableProvider;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaTable, DeltaTableConfig, DeltaTableError, StorageUrl};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use glob::Pattern;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::ObjectMeta;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let data_location = update_s3_console_url(&args.path)?;

    let (object_store_url, _) = extract_object_store_url_and_path(&data_location)?;
    let bucket_name = String::from(
        Url::parse(object_store_url.as_str())
            .expect("failed to parse object_store_url")
            .host_str()
            .expect("failed to extract host/bucket from path"),
    );

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let sdk_config = aws_config::load_from_env().await;

    let s3 = build_s3_from_sdk_config(&bucket_name, &sdk_config).await?;

    ctx.runtime_env()
        .register_object_store("s3", &bucket_name, Arc::new(s3));

    let delta_table_load_result = load_delta_table(&data_location, &object_store_url, &ctx).await;

    let table_arc: Arc<dyn TableProvider> = match delta_table_load_result {
        Ok(delta_table) => Arc::new(delta_table),
        Err(_) => {
            let table = load_listing_table(&data_location, &ctx, &bucket_name).await?;
            Arc::new(table)
        }
    };
    ctx.register_table("tbl", table_arc)?;

    let query = if args.schema {
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'tbl'"
    } else {
        args.query.as_str()
    };

    let df = ctx.sql(query).await?;
    df.show_limit(args.limit).await?;

    Ok(())
}

async fn build_s3_from_sdk_config(
    bucket_name: &String,
    sdk_config: &SdkConfig,
) -> Result<AmazonS3> {
    let credentials_providder = sdk_config
        .credentials_provider()
        .expect("could not find credentials provider");
    let credentials = credentials_providder
        .provide_credentials()
        .await
        .expect("could not load credentials");

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(
            sdk_config
                .region()
                .expect("could not find region")
                .to_string(),
        )
        .with_access_key_id(credentials.access_key_id())
        .with_secret_access_key(credentials.secret_access_key())
        .with_token(
            credentials
                .session_token()
                .expect("could not find session_token")
                .to_string(),
        )
        .build()?;
    Ok(s3)
}

fn update_s3_console_url(path: &str) -> Result<String> {
    if path.starts_with("https://s3.console.aws.amazon.com/s3/buckets/") {
        let parsed_url = Url::parse(path)?;
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
            let maybe_prefix = params.get("prefix");
            let result = maybe_prefix
                .map(|prefix| format!("s3://{}/{}", bucket_name, prefix))
                .unwrap_or_else(|| path.to_string());
            Ok(result)
        } else {
            Ok(path.to_string())
        }
    } else {
        Ok(path.to_string())
    }
}

fn extract_object_store_url_and_path(globbing_path: &str) -> Result<(ObjectStoreUrl, String)> {
    let url = Url::parse(globbing_path).map_err(|_| {
        DataFusionError::Execution(format!("Failed to parse {} as url.", &globbing_path))
    })?;
    let bucket = &url[..url::Position::BeforePath];
    let bucket_url = ObjectStoreUrl::parse(&bucket)?;
    let path = url
        .path()
        .strip_prefix(object_store::path::DELIMITER)
        .unwrap_or_else(|| url.path());
    Ok((bucket_url, String::from(path)))
}

async fn load_delta_table(
    data_location: &String,
    object_store_url: &ObjectStoreUrl,
    ctx: &SessionContext,
) -> Result<DeltaTable, DeltaTableError> {
    let store = ctx.runtime_env().object_store(&object_store_url)?;
    let delta_storage_url = StorageUrl::parse(&data_location).expect("failed to parse storage url");
    let delta_storage = DeltaObjectStore::new(delta_storage_url, store);
    let delta_config = DeltaTableConfig::default();
    let mut delta_table = DeltaTable::new(Arc::new(delta_storage), delta_config);
    let delta_table_load_result = delta_table.load().await;
    delta_table_load_result.map(|_| delta_table)
}

async fn load_listing_table(
    data_location: &str,
    ctx: &SessionContext,
    bucket_name: &str,
) -> Result<ListingTable> {
    let matching_files = list_matching_files(ctx, data_location).await?;

    let matching_file_urls: Vec<_> = matching_files
        .iter()
        .map(|meta| {
            ListingTableUrl::parse(format!("s3://{}/{}", bucket_name, meta.location.as_ref()))
                .expect("failed to create listingtableurl")
        })
        .collect();

    let mut config = ListingTableConfig::new_with_multi_paths(matching_file_urls);
    config = config.infer_options(&ctx.state()).await?;
    config = config.infer_schema(&ctx.state()).await?;
    let table = ListingTable::try_new(config)?;
    Ok(table)
}

async fn list_matching_files(ctx: &SessionContext, globbing_path: &str) -> Result<Vec<ObjectMeta>> {
    let (object_store_url, path) = extract_object_store_url_and_path(globbing_path)?;
    let prefix_path = Path::parse(&path)?;

    let maybe_glob = if contains_glob_start_character(&path) {
        let glob = Pattern::new(&path).map_err(|_| {
            DataFusionError::Execution(format!("Failed to create globbing pattern from {}", &path))
        })?;
        Some(glob)
    } else {
        None
    };

    let store = ctx.runtime_env().object_store(&object_store_url)?;

    let list_result = store.list(Some(&prefix_path)).await?;

    let matching_files_result: BoxStream<Result<ObjectMeta>> = list_result
        .map_err(Into::into)
        .try_filter(move |meta| {
            let glob_ok = maybe_glob.as_ref().map_or_else(|| true, |glob| {
                glob.matches(&meta.location.as_ref())
            });
            futures::future::ready(glob_ok)
        })
        .boxed();

    let matching_files: Vec<ObjectMeta> = matching_files_result.try_collect().await?;

    Ok(matching_files)
}

fn contains_glob_start_character(item: &str) -> bool {
    item.contains('?') || item.contains('*') || item.contains('[')
}

fn extract_leading_path_without_glob_characters(path: &str) -> Path {
    let leading_path_parts_without_glob: Vec<_> = path
        .split(object_store::path::DELIMITER)
        .take_while(|part| !contains_glob_start_character(part))
        .collect();
    Path::from_iter(leading_path_parts_without_glob)
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Location where the data is located
    path: String,

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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_update_s3_console_url() -> Result<()> {
        assert_eq!(
            update_s3_console_url("/Users/timvw/test")?,
            "/Users/timvw/test"
        );
        assert_eq!(update_s3_console_url("https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?region=eu-central-1&prefix=COVID-19_NYT/&showversions=false")?, "s3://datafusion-delta-testing/COVID-19_NYT/");
        assert_eq!(update_s3_console_url("https://s3.console.aws.amazon.com/s3/buckets/datafusion-delta-testing?prefix=COVID-19_NYT/&region=eu-central-1")?, "s3://datafusion-delta-testing/COVID-19_NYT/");
        Ok(())
    }

    #[test]
    fn test_extract_object_store_url_and_path() {
        let actual = extract_object_store_url_and_path("s3://bucket").unwrap();
        assert_eq!(("s3://bucket/", ""), (actual.0.as_str(), actual.1.as_str()));

        let actual = extract_object_store_url_and_path("s3://bucket/").unwrap();
        assert_eq!(("s3://bucket/", ""), (actual.0.as_str(), actual.1.as_str()));

        let actual = extract_object_store_url_and_path("s3://bucket/path").unwrap();
        assert_eq!(
            ("s3://bucket/", "path"),
            (actual.0.as_str(), actual.1.as_str())
        );
    }
}
