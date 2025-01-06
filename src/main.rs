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
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use deltalake::open_table;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::ObjectStore;
use regex::Regex;
use url::Url;

use crate::args::Args;

mod args;

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let args: Args = Args::parse();

    let (_, data_path) = replace_s3_console_url_with_s3_path(&args.path.clone());

    let sdk_config = get_sdk_config(&args).await;

    let (data_path, file_format) = replace_glue_table_with_path(&data_path, &sdk_config).await?;

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
            let path = Path::parse(gcs_url.path())?;
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

    let table: Arc<dyn TableProvider> = if let Ok(mut delta_table) = open_table(&data_path).await {
        if let Some(at) = args.at {
            delta_table.load_with_datetime(at).await?;
        }
        Arc::new(delta_table)
    } else {
        let table_path = ListingTableUrl::parse(&data_path)?;
        let mut config = ListingTableConfig::new(table_path);

        config = if let Some(format) = file_format {
            config.with_listing_options(ListingOptions::new(format))
        } else {
            config.infer_options(&ctx.state()).await?
        };

        config = config.infer_schema(&ctx.state()).await?;
        let table = ListingTable::try_new(config)?;
        Arc::new(table)
    };

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

async fn replace_glue_table_with_path(
    path: &str,
    sdk_config: &SdkConfig,
) -> Result<(String, Option<Arc<dyn FileFormat>>)> {
    if let Some((database, table)) = parse_glue_url(path) {
        let (location, format) = get_path_and_format(sdk_config, &database, &table).await?;
        Ok((location, Some(format)))
    } else {
        Ok((String::from(path), None))
    }
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

async fn get_path_and_format(
    sdk_config: &SdkConfig,
    database_name: &str,
    table_name: &str,
) -> Result<(String, Arc<dyn FileFormat>)> {
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

    let sd = table.storage_descriptor().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Could not find storage descriptor for {}.{} in glue",
            database_name, table_name
        ))
    })?;

    let location = lookup_storage_location(sd)?;
    let format_arc = lookup_file_format(table.clone(), sd)?;
    Ok((location, format_arc))
}

fn lookup_storage_location(sd: &StorageDescriptor) -> Result<String> {
    let location = sd.location().ok_or_else(|| {
        DataFusionError::Execution(format!("Could not find sd.location for {sd:#?}",))
    })?;
    Ok(location.to_string())
}

fn lookup_file_format(table: Table, sd: &StorageDescriptor) -> Result<Arc<dyn FileFormat>> {
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

    let table_parameters = table.parameters.unwrap_or_default();
    let _table_type = table_parameters
        .get("table_type")
        .map(|x| x.as_str())
        .unwrap_or_default();

    // this can be delta...
    // or ICEBERG...

    /*
        Table format: Apache Iceberg
    Input format: -
    Output format: -
    Serde serialization lib:-
         */

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
