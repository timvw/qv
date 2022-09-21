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
use std::env;
use std::sync::Arc;
use url::{ParseError, Url};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let data_location = update_s3_console_url(&args.path)?;

    if let Some(aws_profile) = args.profile {
        env::set_var("AWS_PROFILE", aws_profile);
    }

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let data_location_with_scheme = ensure_scheme(&data_location)?;
    let (object_store_url, prefix, maybe_glob) = extract_path_parts(&data_location_with_scheme)?;

    if data_location.starts_with("s3://") {
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

    let table_arc: Arc<dyn TableProvider> = if let Some(_) = &maybe_glob {
        let table = load_listing_table(&ctx, &object_store_url, &prefix, &maybe_glob).await?;
        Arc::new(table)
    } else {
        let delta_table_load_result =
            load_delta_table(&data_location, &object_store_url, &ctx).await;
        match delta_table_load_result {
            Ok(delta_table) => Arc::new(delta_table),
            Err(_) => {
                let table = load_listing_table(&ctx, &object_store_url, &prefix, &maybe_glob).await?;
                Arc::new(table)
            }
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

fn extract_path_parts(path_with_scheme: &str) -> Result<(ObjectStoreUrl, String, Option<Pattern>)> {
    let (non_globbed_path, maybe_globbed_path) = match split_glob_expression(path_with_scheme) {
        Some((non_globbed, globbed)) => (non_globbed, Some(globbed)),
        None => (path_with_scheme, None),
    };

    let non_globbed_url = Url::parse(non_globbed_path)?;
    let (object_store_url, prefix) = match non_globbed_url.scheme() {
        "file" => ObjectStoreUrl::parse("file://").map(|osu| (osu, non_globbed_url.path())),
        "s3" => ObjectStoreUrl::parse(&non_globbed_url[..url::Position::BeforePath])
            .map(|osu| (osu, non_globbed_url.path())),
        _ => Err(DataFusionError::NotImplemented(format!(
            "no support scheme {}.",
            non_globbed_url.scheme()
        ))),
    }?;

    let prefix_without_leading_delimiter = prefix.strip_prefix("/").unwrap_or(prefix);
    let maybe_glob = maybe_globbed_path
        .map(|globbed_path| format!("{}{}", &prefix_without_leading_delimiter, globbed_path));

    let maybe_result = maybe_glob.map(|glob| {
        Pattern::new(&glob).map_err(|_| {
            DataFusionError::Execution(format!("Failed to parse {} as a globbing pattern", &glob))
        })
    });

    match maybe_result {
        Some(Ok(pattern)) => Ok((
            object_store_url,
            String::from(prefix_without_leading_delimiter),
            Some(pattern),
        )),
        Some(Err(e)) => Err(anyhow::Error::from(e)),
        None => Ok((
            object_store_url,
            String::from(prefix_without_leading_delimiter),
            None,
        )),
    }
}

#[test]
fn test_extract_path_parts() {
    let actual = extract_path_parts("s3://bucket").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("", actual.1);
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("s3://bucket/").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("", actual.1);
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("s3://bucket/a").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("a", actual.1);
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("s3://bucket/a*").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("", actual.1);
    assert_eq!(Some(Pattern::new("a*").unwrap()), actual.2);

    let actual = extract_path_parts("s3://bucket/a/b*").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("a/", actual.1);
    assert_eq!(Some(Pattern::new("a/b*").unwrap()), actual.2);

    let actual = extract_path_parts("s3://bucket/a/b*/c").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("a/", actual.1);
    assert_eq!(Some(Pattern::new("a/b*/c").unwrap()), actual.2);

    let actual = extract_path_parts("file://").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("", actual.1);
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("file:///a").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("a", actual.1);
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("file:///c/b").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("c/b", actual.1);
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("file:///c/b*").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("c/", actual.1);
    assert_eq!(Some(Pattern::new("c/b*").unwrap()), actual.2);

    let actual = extract_path_parts("file://c*").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("", actual.1);
    assert_eq!(Some(Pattern::new("c*").unwrap()), actual.2);

    let actual = extract_path_parts("file:///a/b*/c").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("a/", actual.1);
    assert_eq!(Some(Pattern::new("a/b*/c").unwrap()), actual.2);
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
    ctx: &SessionContext,
    object_store_url: &ObjectStoreUrl,
    prefix: &str,
    maybe_glob: &Option<Pattern>,
) -> Result<ListingTable> {

    //     // now resolve the object store...
    //     let object_store = ctx.runtime_env().object_store(&object_store_url)?;

    let matching_files = list_matching_files(ctx, object_store_url, prefix, maybe_glob).await?;

    let matching_file_urls: Vec<_> = matching_files
        .iter()
        .map(|meta| {
            ListingTableUrl::parse(format!("{}{}", object_store_url.as_str(), meta.location.as_ref()))
                .expect("failed to create listingtableurl")
        })
        .collect();

    let mut config = ListingTableConfig::new_with_multi_paths(matching_file_urls);
    config = config.infer_options(&ctx.state()).await?;
    config = config.infer_schema(&ctx.state()).await?;
    let table = ListingTable::try_new(config)?;
    Ok(table)
}

async fn list_matching_files(ctx: &SessionContext, object_store_url: &ObjectStoreUrl, prefix: &str, maybe_glob: &Option<Pattern>) -> Result<Vec<ObjectMeta>> {

    let prefix_path = Path::parse(prefix)?;

    let store = ctx.runtime_env().object_store(&object_store_url)?;

    let head_result = store.head(&prefix_path).await;
    let matching_files: Vec<ObjectMeta> = match head_result {
        Ok(om) => vec![om],
        Err(_) => {
            let list_result = store.list(Some(&prefix_path)).await?;

            let matching_files_result: BoxStream<Result<ObjectMeta>> = list_result
                .map_err(Into::into)
                .try_filter(move |meta| {
                    let glob_ok = maybe_glob
                        .as_ref()
                        .map_or_else(|| true, |glob| glob.matches(meta.location.as_ref()));
                    let is_hidden = is_hidden(&meta.location);
                    futures::future::ready(glob_ok && !is_hidden)
                })
                .boxed();

            matching_files_result.try_collect().await?
        }
    };

    Ok(matching_files)
}

fn is_hidden(path: &Path) -> bool {
    path.parts()
        .find(|part| part.as_ref().starts_with('.') || part.as_ref().starts_with('_'))
        .map_or_else(|| false, |_| true)
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

    /// Optional AWS Profile to use
    #[clap(short, long)]
    profile: Option<String>,
}

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

// So how do we want to proceed???

// what do we need? an object_store_url, a prefix and a glob

/// Update the data_location such that it starts with a scheme
/// In case no scheme is provided, we assume the data_location is a (globbing) expression on the local filesystem.
fn ensure_scheme(data_location: &str) -> Result<String, DataFusionError> {
    let (non_globbed_path, maybe_globbed_path) = match split_glob_expression(data_location) {
        Some((non_globbed, globbed)) => (non_globbed, Some(globbed)),
        None => (data_location, None),
    };

    let non_globbed_path_with_scheme = match Url::parse(non_globbed_path) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            let local_path = std::path::Path::new(non_globbed_path).canonicalize()?;
            if local_path.is_file() {
                Url::from_file_path(&local_path)
            } else {
                Url::from_directory_path(&local_path)
            }
            .map_err(|_| {
                DataFusionError::Execution(format!(
                    "failed to build url from file path for {:?}",
                    &local_path
                ))
            })
        }
        Err(e) => Err(DataFusionError::Execution(format!(
            "Failed to parse {} as url because {}",
            data_location, e
        ))),
    }?;

    let path_with_scheme = match maybe_globbed_path {
        Some(globbed_path) => format!("{}{}", non_globbed_path_with_scheme.as_str(), globbed_path),
        None => String::from(non_globbed_path_with_scheme.as_str()),
    };

    Ok(path_with_scheme)
}

#[test]
fn test_ensure_scheme() {
    assert_eq!("s3://", ensure_scheme("s3://").unwrap());
    assert_eq!("s3://bucket", ensure_scheme("s3://bucket").unwrap());
    assert_eq!("s3://bucket/a", ensure_scheme("s3://bucket/a").unwrap());
    assert_eq!("file:///", ensure_scheme("file:///").unwrap());
    assert_eq!("file:///a", ensure_scheme("file:///a").unwrap());

    let actual_relative_file = ensure_scheme("src/main.rs").unwrap();
    assert!(actual_relative_file.starts_with("file:///"));
    assert!(actual_relative_file.ends_with("src/main.rs"));

    let actual_relative_dir = ensure_scheme("src").unwrap();
    assert!(actual_relative_dir.starts_with("file:///"));
    assert!(actual_relative_dir.ends_with("src/"));

    let actual_relative_file_glob = ensure_scheme("src/ma*.rs").unwrap();
    assert!(actual_relative_file_glob.starts_with("file:///"));
    assert!(actual_relative_file_glob.ends_with("src/ma*.rs"));

    let actual_relative_dir_glob = ensure_scheme("s*c").unwrap();
    assert!(actual_relative_dir_glob.starts_with("file:///"));
    assert!(actual_relative_dir_glob.ends_with("s*c"));
}

/// Splits `path` at the first path segment containing a glob expression, returning
/// `None` if no glob expression found.
///
/// Path delimiters are determined using [`std::path::is_separator`] which
/// permits `/` as a path delimiter even on Windows platforms.
///
fn split_glob_expression(path: &str) -> Option<(&str, &str)> {
    const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

    let mut last_separator = 0;

    for (byte_idx, char) in path.char_indices() {
        if GLOB_START_CHARS.contains(&char) {
            if last_separator == 0 {
                return Some((".", path));
            }
            return Some(path.split_at(last_separator));
        }

        if std::path::is_separator(char) {
            last_separator = byte_idx + char.len_utf8();
        }
    }
    None
}
