use crate::GlobbingPath;
use aws_sdk_glue::Client;
use aws_types::SdkConfig;
use chrono::{DateTime, Utc};
use clap::Parser;
use datafusion::common::{DataFusionError, Result};
use regex::Regex;
use std::collections::HashMap;
use url::Url;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
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
    pub limit: usize,

    /// Optional AWS Profile to use
    #[clap(short, long)]
    pub profile: Option<String>,

    /// Optional timestamp for delta table
    #[clap(
        short,
        long,
        help = "Timestamp to load deltatable in RFC format, eg: 2022-01-13T16:39:00+01:00"
    )]
    pub at: Option<DateTime<Utc>>,
}

impl Args {
    pub fn get_query(&self) -> &str {
        let query = if self.schema {
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'tbl'"
        } else {
            self.query.as_str()
        };
        query
    }

    pub async fn get_globbing_path(&self, sdk_config: &SdkConfig) -> Result<GlobbingPath> {
        let data_location = update_s3_console_url(&self.path);
        let data_location = update_glue_url(sdk_config, &data_location).await;
        GlobbingPath::parse(&data_location)
    }
}

/// When the provided s looks like glue://database.table we lookup the storage location
/// When the provided s does not look like glue://database.table, return s as is.
async fn update_glue_url(sdk_config: &SdkConfig, s: &str) -> String {
    if let Some((database_name, table_name)) = parse_glue_url(s) {
        get_storage_location(sdk_config, &database_name, &table_name)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "failed to get storage location for {}.{}",
                    database_name, table_name
                )
            })
    } else {
        s.to_string()
    }
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
