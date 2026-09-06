#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_glue::types::{StorageDescriptor, Table};
use aws_sdk_glue::Client;
use aws_types::SdkConfig;
use clap::Parser;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use deltalake::open_table;
use futures::TryStreamExt;
use iceberg::io::{
    FileIOBuilder, GCS_CREDENTIALS_JSON, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS,
    S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg::table::{StaticTable, Table as IcebergTable};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_glue::{
    GlueCatalogBuilder, AWS_PROFILE_NAME, AWS_REGION_NAME, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE,
};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_datafusion::IcebergStaticTableProvider;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use regex::Regex;
use url::Url;

use crate::args::Args;

mod args;

enum InputSource {
    Path {
        location: String,
        file_format: Option<Arc<dyn FileFormat>>,
    },
    Iceberg(IcebergTable),
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let args: Args = Args::parse();

    let sdk_config = get_sdk_config(&args).await;

    let (_, data_path) = replace_s3_console_url_with_s3_path(&args.path);
    let input = resolve_input(&data_path, &args, &sdk_config).await?;

    let (data_path, file_format) = match input {
        InputSource::Iceberg(table) => {
            let table = build_iceberg_provider(table, args.at.as_ref()).await?;
            return run_query(&ctx, &args, table).await;
        }
        InputSource::Path {
            location,
            file_format,
        } => (location, file_format),
    };

    let data_path = if data_path.starts_with("s3://") {
        // register s3 object store
        let s3_url = Url::parse(&data_path)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse url, {e}")))?;
        let s3 = build_s3(&s3_url, &sdk_config).await?;
        let s3_arc = Arc::new(s3);
        ctx.runtime_env()
            .register_object_store(&s3_url, s3_arc.clone());

        deltalake::aws::register_handlers(None);

        // add trailing slash to folder
        if !data_path.ends_with('/') {
            let path = Path::from_url_path(s3_url.path())?;
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

    let data_path = if data_path.starts_with("gs://") || data_path.starts_with("gcs://") {
        let gcs_url = Url::parse(&data_path)
            .map_err(|e| DataFusionError::Execution(format!("Failed to parse url, {e}")))?;
        let gcs = build_gcs(&gcs_url).await?;
        let gcs_arc = Arc::new(gcs);
        ctx.runtime_env()
            .register_object_store(&gcs_url, gcs_arc.clone());

        deltalake::gcp::register_handlers(None);

        // add trailing slash to folder
        if !data_path.ends_with('/') {
            let path = Path::from_url_path(gcs_url.path())?;
            if gcs_arc.head(&path).await.is_err() {
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

    let data_path = normalize_data_path(&data_path)?;

    let table: Arc<dyn TableProvider> = if let Some(table) =
        try_build_iceberg_provider(&data_path, &ctx, &sdk_config, &args).await?
    {
        table
    } else if let Some(delta_url) = parse_as_url(&data_path) {
        if let Ok(mut delta_table) = open_table(delta_url).await {
            if let Some(at) = args.at {
                delta_table.load_with_datetime(at).await?;
            }
            delta_table.table_provider().await?
        } else {
            build_listing_table(&data_path, file_format, &ctx).await?
        }
    } else {
        build_listing_table(&data_path, file_format, &ctx).await?
    };

    run_query(&ctx, &args, table).await
}

async fn run_query(ctx: &SessionContext, args: &Args, table: Arc<dyn TableProvider>) -> Result<()> {
    ctx.register_table(TableReference::from("datafusion.public.tbl"), table)?;

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
    if env::var("AWS_DEFAULT_REGION").is_err() && env::var("AWS_REGION").is_err() {
        env::set_var("AWS_DEFAULT_REGION", "eu-central-1");
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

async fn resolve_input(path: &str, args: &Args, sdk_config: &SdkConfig) -> Result<InputSource> {
    if let Some(catalog_uri) = &args.rest_catalog {
        return load_rest_catalog_table(path, catalog_uri, args).await;
    }

    let Some((database_name, table_name)) = parse_glue_url(path) else {
        return Ok(InputSource::Path {
            location: path.to_string(),
            file_format: None,
        });
    };

    let table = get_glue_table(sdk_config, &database_name, &table_name).await?;
    if is_iceberg_glue_table(&table) {
        let table =
            load_glue_iceberg_table(table, &database_name, &table_name, args, sdk_config).await?;
        return Ok(InputSource::Iceberg(table));
    }

    let sd = table.storage_descriptor().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Could not find storage descriptor for {database_name}.{table_name} in Glue"
        ))
    })?;
    let location = lookup_storage_location(sd)?;
    let file_format = lookup_file_format(sd)?;

    Ok(InputSource::Path {
        location,
        file_format: Some(file_format),
    })
}

async fn load_rest_catalog_table(
    table_name: &str,
    catalog_uri: &str,
    args: &Args,
) -> Result<InputSource> {
    let mut properties =
        parse_catalog_properties(&args.catalog_properties, &args.catalog_property_env)?;
    properties.insert(REST_CATALOG_PROP_URI.to_string(), catalog_uri.to_string());
    if let Some(warehouse) = &args.catalog_warehouse {
        properties.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
    }

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load("rest", properties)
        .await
        .map_err(iceberg_error)?;
    let ident = parse_table_ident(table_name)?;
    let table = catalog.load_table(&ident).await.map_err(iceberg_error)?;

    Ok(InputSource::Iceberg(table))
}

fn parse_catalog_properties(
    values: &[String],
    environment_values: &[String],
) -> Result<HashMap<String, String>> {
    let mut properties = HashMap::new();
    for (index, property) in values.iter().enumerate() {
        let (key, value) = property.split_once('=').ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Invalid catalog property #{}; expected KEY=VALUE",
                index + 1
            ))
        })?;
        if key.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "Catalog property #{} has an empty key",
                index + 1
            )));
        }
        properties.insert(key.to_string(), value.to_string());
    }

    for (index, property) in environment_values.iter().enumerate() {
        let (key, variable) = property.split_once('=').ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Invalid environment-backed catalog property #{}; expected KEY=ENV_VAR",
                index + 1
            ))
        })?;
        if key.is_empty() || variable.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "Environment-backed catalog property #{} has an empty key or variable name",
                index + 1
            )));
        }
        let value = env::var(variable).map_err(|_| {
            DataFusionError::Configuration(format!(
                "Environment variable '{variable}' is not set for catalog property #{}",
                index + 1
            ))
        })?;
        properties.insert(key.to_string(), value);
    }

    Ok(properties)
}

fn parse_table_ident(value: &str) -> Result<TableIdent> {
    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() < 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(DataFusionError::Configuration(format!(
            "Iceberg catalog table must be namespace-qualified, for example 'analytics.events'; got '{value}'"
        )));
    }
    TableIdent::from_strs(parts).map_err(iceberg_error)
}

#[test]
fn test_catalog_arguments() -> Result<()> {
    assert_eq!(
        parse_table_ident("analytics.events")?,
        TableIdent::from_strs(["analytics", "events"]).map_err(iceberg_error)?
    );
    assert_eq!(
        parse_table_ident("org.analytics.events")?,
        TableIdent::from_strs(["org", "analytics", "events"]).map_err(iceberg_error)?
    );
    assert!(parse_table_ident("events").is_err());

    let properties = parse_catalog_properties(
        &[
            "token=secret".to_string(),
            "header.X-Tenant=acme".to_string(),
        ],
        &[],
    )?;
    assert_eq!(properties.get("token").map(String::as_str), Some("secret"));
    assert_eq!(
        properties.get("header.X-Tenant").map(String::as_str),
        Some("acme")
    );
    assert!(parse_catalog_properties(&["invalid".to_string()], &[]).is_err());

    let variable = "QV_TEST_CATALOG_SECRET";
    env::set_var(variable, "from-env");
    let properties = parse_catalog_properties(&[], &[format!("token={variable}")])?;
    env::remove_var(variable);
    assert_eq!(
        properties.get("token").map(String::as_str),
        Some("from-env")
    );
    assert!(parse_catalog_properties(&[], &[format!("token={variable}")]).is_err());
    Ok(())
}

async fn get_glue_table(
    sdk_config: &SdkConfig,
    database_name: &str,
    table_name: &str,
) -> Result<Table> {
    Client::new(sdk_config)
        .get_table()
        .set_database_name(Some(database_name.to_string()))
        .set_name(Some(table_name.to_string()))
        .send()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .table
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Could not find {database_name}.{table_name} in Glue"
            ))
        })
}

fn is_iceberg_glue_table(table: &Table) -> bool {
    table
        .parameters
        .as_ref()
        .and_then(|parameters| parameters.get("table_type"))
        .is_some_and(|table_type| table_type.eq_ignore_ascii_case("ICEBERG"))
}

async fn load_glue_iceberg_table(
    table: Table,
    database_name: &str,
    table_name: &str,
    args: &Args,
    sdk_config: &SdkConfig,
) -> Result<IcebergTable> {
    let warehouse = table
        .storage_descriptor()
        .and_then(StorageDescriptor::location)
        .map(ToString::to_string)
        .or_else(|| {
            table
                .parameters
                .as_ref()
                .and_then(|parameters| parameters.get("metadata_location"))
                .and_then(|location| location.split_once("/metadata/").map(|(root, _)| root))
                .map(ToString::to_string)
        })
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Could not determine the warehouse for Iceberg table {database_name}.{table_name}"
            ))
        })?;

    let mut properties = iceberg_storage_properties(sdk_config, args).await?;
    properties
        .entry(GLUE_CATALOG_PROP_WAREHOUSE.to_string())
        .or_insert(warehouse);
    if let Some(profile) = &args.profile {
        properties
            .entry(AWS_PROFILE_NAME.to_string())
            .or_insert_with(|| profile.clone());
    }
    if let Some(region) = sdk_config.region() {
        properties
            .entry(AWS_REGION_NAME.to_string())
            .or_insert_with(|| region.as_ref().to_string());
    }
    if let Ok(endpoint) = env::var("AWS_ENDPOINT_URL_GLUE") {
        properties
            .entry(GLUE_CATALOG_PROP_URI.to_string())
            .or_insert(endpoint);
    }

    let catalog = GlueCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load("glue", properties)
        .await
        .map_err(iceberg_error)?;
    let ident = TableIdent::from_strs([database_name, table_name]).map_err(iceberg_error)?;
    catalog.load_table(&ident).await.map_err(iceberg_error)
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

fn lookup_storage_location(sd: &StorageDescriptor) -> Result<String> {
    let location = sd.location().ok_or_else(|| {
        DataFusionError::Execution(format!("Could not find sd.location for {sd:#?}",))
    })?;
    Ok(location.to_string())
}

fn lookup_file_format(sd: &StorageDescriptor) -> Result<Arc<dyn FileFormat>> {
    let empty_str = String::from("");
    let input_format = sd.input_format.as_ref().unwrap_or(&empty_str);
    let output_format = sd.output_format.as_ref().unwrap_or(&empty_str);
    let serde_info = sd.serde_info.as_ref().ok_or_else(|| {
        DataFusionError::Execution(
            "Failed to find serde_info in storage descriptor for glue table".to_string(),
        )
    })?;
    let serialization_library = serde_info
        .serialization_library
        .as_ref()
        .unwrap_or(&empty_str);
    let serde_info_parameters = serde_info
        .parameters
        .as_ref()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Failed to find parameters of serde_info in storage descriptor for glue table"
                    .to_string(),
            )
        })?
        .clone();
    let sd_parameters = match &sd.parameters {
        Some(x) => x.clone(),
        None => HashMap::new(),
    };

    let item: (&str, &str, &str) = (input_format, output_format, serialization_library);
    let format_result: Result<Arc<dyn FileFormat>> = match item {
        (
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        ) => Ok(Arc::new(ParquetFormat::default())),
        (
            // actually this is Deltalake format...
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        ) => Ok(Arc::new(ParquetFormat::default())),
        (
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        ) => {
            let mut format = CsvFormat::default();
            let delim = serde_info_parameters
                .get("field.delim")
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "Failed to find field.delim in serde_info parameters".to_string(),
                    )
                })?
                .as_bytes();
            let delim_char = delim[0];
            format = format.with_delimiter(delim_char);
            let has_header = sd_parameters
                .get("skip.header.line.count")
                .unwrap_or(&empty_str)
                .eq("1");
            format = format.with_has_header(has_header);
            Ok(Arc::new(format))
        }
        (
            "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
            "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
            "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
        ) => Ok(Arc::new(AvroFormat)),
        (
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "org.apache.hive.hcatalog.data.JsonSerDe",
        ) => Ok(Arc::new(JsonFormat::default())),
        (
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "org.openx.data.jsonserde.JsonSerDe",
        ) => Ok(Arc::new(JsonFormat::default())),
        (
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "com.amazon.ionhiveserde.IonHiveSerDe",
        ) => Ok(Arc::new(JsonFormat::default())),
        _ => Err(DataFusionError::Execution(format!(
            "No support for: {}, {}, {:?} yet.",
            input_format, output_format, sd
        ))),
    };

    let format = format_result?;
    Ok(format)
}

fn normalize_data_path(data_path: &str) -> Result<String> {
    if data_path.contains("://") {
        return Ok(data_path.to_string());
    }

    let canonical = std::path::Path::new(data_path)
        .canonicalize()
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to canonicalize path {data_path}: {e}"))
        })?;
    let file_url = Url::from_file_path(&canonical).map_err(|_| {
        DataFusionError::Execution(format!("Failed to convert path {data_path} to file:// URL"))
    })?;

    Ok(file_url.to_string())
}

fn parse_as_url(path: &str) -> Option<Url> {
    Url::parse(path)
        .ok()
        .or_else(|| Url::from_file_path(path).ok())
}

async fn try_build_iceberg_provider(
    data_path: &str,
    ctx: &SessionContext,
    sdk_config: &SdkConfig,
    args: &Args,
) -> Result<Option<Arc<dyn TableProvider>>> {
    let Some(metadata_location) = find_iceberg_metadata(data_path, ctx).await? else {
        return Ok(None);
    };

    let file_io = FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new()))
        .with_props(iceberg_storage_properties(sdk_config, args).await?)
        .build();
    let ident = TableIdent::from_strs(["qv", "tbl"]).map_err(iceberg_error)?;
    let table = StaticTable::from_metadata_file(&metadata_location, ident, file_io)
        .await
        .map_err(iceberg_error)?
        .into_table();

    Ok(Some(build_iceberg_provider(table, args.at.as_ref()).await?))
}

async fn build_iceberg_provider(
    table: IcebergTable,
    at: Option<&chrono::DateTime<chrono::Utc>>,
) -> Result<Arc<dyn TableProvider>> {
    let provider = if let Some(at) = at {
        let metadata = table.metadata();
        let snapshot_id = if metadata.history().is_empty() {
            metadata
                .snapshots()
                .filter(|snapshot| snapshot.timestamp_ms() <= at.timestamp_millis())
                .max_by_key(|snapshot| snapshot.timestamp_ms())
                .map(|snapshot| snapshot.snapshot_id())
        } else {
            metadata
                .history()
                .iter()
                .filter(|entry| entry.timestamp_ms() <= at.timestamp_millis())
                .max_by_key(|entry| entry.timestamp_ms())
                .map(|entry| entry.snapshot_id)
        }
        .ok_or_else(|| {
            DataFusionError::Execution(format!("Iceberg table has no snapshot at or before {at}"))
        })?;
        IcebergStaticTableProvider::try_new_from_table_snapshot(table.clone(), snapshot_id)
            .await
            .map_err(iceberg_error)?
    } else {
        IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .map_err(iceberg_error)?
    };
    Ok(Arc::new(provider))
}

async fn find_iceberg_metadata(data_path: &str, ctx: &SessionContext) -> Result<Option<String>> {
    let data_path = data_path.trim_end_matches('/');
    if data_path.ends_with(".metadata.json") {
        return Ok(Some(data_path.to_string()));
    }

    let mut root_url = match Url::parse(data_path) {
        Ok(url) => url,
        Err(_) => return Ok(None),
    };
    if root_url.scheme() == "file" && root_url.to_file_path().is_ok_and(|path| path.is_file()) {
        return Ok(None);
    }
    let store_url = if root_url.scheme() == "file" {
        ObjectStoreUrl::local_filesystem()
    } else {
        let authority = root_url.host_str().ok_or_else(|| {
            DataFusionError::Execution(format!("URL has no storage authority: {root_url}"))
        })?;
        ObjectStoreUrl::parse(format!("{}://{authority}", root_url.scheme()))?
    };
    let store = ctx.runtime_env().object_store(store_url)?;
    let object_path = Path::from_url_path(root_url.path())?;
    if root_url.scheme() != "file" && store.head(&object_path).await.is_ok() {
        return Ok(None);
    }
    let metadata_prefix = object_path.join("metadata");
    let mut entries = store.list(Some(&metadata_prefix));
    let mut candidates = Vec::new();
    let mut saw_metadata_json = false;

    while let Some(entry) = entries
        .try_next()
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?
    {
        let file_name = entry.location.filename().unwrap_or_default();
        saw_metadata_json |= file_name.ends_with(".metadata.json");
        if let Some(version) = iceberg_metadata_version(file_name) {
            candidates.push((version, entry.location));
        }
    }

    let version_hint_path = metadata_prefix.clone().join("version-hint.text");
    let hinted_version = match store.get(&version_hint_path).await {
        Ok(result) => {
            let bytes = result
                .bytes()
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            Some(
                String::from_utf8_lossy(&bytes)
                    .trim()
                    .parse::<u64>()
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "Invalid Iceberg version hint at {version_hint_path}: {error}"
                        ))
                    })?,
            )
        }
        Err(object_store::Error::NotFound { .. }) => None,
        Err(error) => return Err(DataFusionError::External(Box::new(error))),
    };
    let selected = if let Some(version) = hinted_version {
        candidates
            .iter()
            .filter(|(candidate_version, _)| *candidate_version == version)
            .max()
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Iceberg version hint {version} has no matching metadata file under {metadata_prefix}"
                ))
            })?
    } else if let Some(candidate) = candidates.into_iter().max() {
        candidate
    } else if saw_metadata_json {
        return Err(DataFusionError::Execution(format!(
            "No supported Iceberg metadata filename found under {metadata_prefix}"
        )));
    } else {
        return Ok(None);
    };
    root_url.set_path(&format!("/{}", selected.1));
    root_url.set_query(None);
    root_url.set_fragment(None);

    Ok(Some(root_url.to_string()))
}

fn iceberg_metadata_version(file_name: &str) -> Option<u64> {
    if !file_name.ends_with(".metadata.json") {
        return None;
    }

    if let Some(version) = file_name
        .strip_prefix('v')
        .and_then(|name| name.split('.').next())
        .and_then(|version| version.parse().ok())
    {
        return Some(version);
    }

    file_name
        .split_once('-')
        .and_then(|(version, _)| version.parse().ok())
}

#[test]
fn test_iceberg_metadata_version() {
    assert_eq!(iceberg_metadata_version("v3.metadata.json"), Some(3));
    assert_eq!(
        iceberg_metadata_version("00042-acde.metadata.json"),
        Some(42)
    );
    assert_eq!(iceberg_metadata_version("snap-42.avro"), None);
}

#[tokio::test]
async fn test_find_local_iceberg_metadata_honors_version_hint() -> Result<()> {
    let directory = tempfile::Builder::new()
        .prefix("qv iceberg ")
        .tempdir()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let metadata = directory.path().join("metadata");
    std::fs::create_dir(&metadata).map_err(|error| DataFusionError::External(Box::new(error)))?;
    std::fs::write(metadata.join("v1.metadata.json"), [])
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    std::fs::write(metadata.join("v2.metadata.json"), [])
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    std::fs::write(metadata.join("version-hint.text"), "1")
        .map_err(|error| DataFusionError::External(Box::new(error)))?;

    let table_url = Url::from_directory_path(directory.path()).map_err(|_| {
        DataFusionError::Execution("Failed to create local test table URL".to_string())
    })?;
    let found = find_iceberg_metadata(table_url.as_str(), &SessionContext::new()).await?;

    assert!(found
        .as_deref()
        .is_some_and(|location| location.ends_with("/metadata/v1.metadata.json")));
    Ok(())
}

async fn iceberg_storage_properties(
    sdk_config: &SdkConfig,
    args: &Args,
) -> Result<HashMap<String, String>> {
    let mut properties = HashMap::new();

    if let Some(credentials_provider) = sdk_config.credentials_provider() {
        if let Ok(credentials) = credentials_provider.provide_credentials().await {
            properties.insert(
                S3_ACCESS_KEY_ID.to_string(),
                credentials.access_key_id().to_string(),
            );
            properties.insert(
                S3_SECRET_ACCESS_KEY.to_string(),
                credentials.secret_access_key().to_string(),
            );
            if let Some(token) = credentials.session_token() {
                properties.insert(S3_SESSION_TOKEN.to_string(), token.to_string());
            }
        }
    }
    if let Some(region) = sdk_config.region() {
        properties.insert(S3_REGION.to_string(), region.as_ref().to_string());
    }
    if let Some(endpoint) = s3_endpoint() {
        properties.insert(S3_ENDPOINT.to_string(), endpoint);
        let path_style_access = env::var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST")
            .map(|value| !value.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        properties.insert(
            S3_PATH_STYLE_ACCESS.to_string(),
            path_style_access.to_string(),
        );
    }
    if let Ok(credentials_path) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        let credentials = std::fs::read_to_string(&credentials_path).map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to read Google credentials from {credentials_path}: {error}"
            ))
        })?;
        properties.insert(GCS_CREDENTIALS_JSON.to_string(), credentials);
    }
    properties.extend(parse_catalog_properties(
        &args.catalog_properties,
        &args.catalog_property_env,
    )?);

    Ok(properties)
}

fn s3_endpoint() -> Option<String> {
    env::var("AWS_ENDPOINT_URL_S3")
        .ok()
        .or_else(|| env::var("AWS_ENDPOINT_URL").ok())
}

fn iceberg_error(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

async fn build_listing_table(
    data_path: &str,
    file_format: Option<Arc<dyn FileFormat>>,
    ctx: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let table_path = ListingTableUrl::parse(data_path)?;
    let mut config = ListingTableConfig::new(table_path);

    config = if let Some(format) = file_format {
        config.with_listing_options(ListingOptions::new(format))
    } else {
        config.infer_options(&ctx.state()).await?
    };

    config = config.infer_schema(&ctx.state()).await?;
    let table = ListingTable::try_new(config)?;
    Ok(Arc::new(table))
}

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
    let builder = if let Some(aws_endpoint_url) = s3_endpoint() {
        builder.with_endpoint(aws_endpoint_url)
    } else {
        builder
    };

    let s3 = builder.build()?;

    Ok(s3)
}

async fn build_gcs(gcs_url: &Url) -> Result<GoogleCloudStorage> {
    let google_application_credentials =
        env::var("GOOGLE_APPLICATION_CREDENTIALS").map_err(|_| {
            DataFusionError::Execution(String::from(
                "Could not find GOOGLE_APPLICATION_CREDENTIALS environment variable",
            ))
        })?;

    let bucket_name = gcs_url.host_str().unwrap();

    let gcs_builder = GoogleCloudStorageBuilder::new();
    let gcs_builder = gcs_builder.with_bucket_name(bucket_name);
    let gcs_builder = gcs_builder.with_service_account_path(google_application_credentials);
    let gcs = gcs_builder.build()?;

    Ok(gcs)
}
