use anyhow::Result;
use camino::Utf8PathBuf;
use clap::Parser;
//use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::prelude::*;
//use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
use std::sync::Arc;
//use datafusion::datafusion_data_access::object_store::ObjectStore;
//use std::any::Any;
use aws_types::credentials::*;
use datafusion::arrow::compute::lt;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::object_store::ObjectStoreUrl;
use object_store::aws::AmazonS3Builder;
use std::collections::HashMap;
//use datafusion::arrow::datatypes::Schema as ArrowSchema;
//use datafusion::datasource::file_format::FileFormat;
//use datafusion::datasource::file_format::parquet::ParquetFormat;
//use datafusion::datasource::listing::PartitionedFile;
//use datafusion::datasource::TableProvider;
//use datafusion::logical_expr::TableType;
//use datafusion::physical_plan::ExecutionPlan;
//use datafusion::physical_plan::file_format::FileScanConfig;
//use deltalake::DeltaTable;
//use async_trait::async_trait;
use url::Url;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Location where the data is located
    path: Utf8PathBuf,

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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let data_location = update_s3_console_url(args.path.as_str())?;

    let (object_store_url, path) = extract_object_store_url_and_path(&data_location)?;
    let bucket_name = String::from(
        Url::parse(object_store_url.as_str())
            .expect("failed to parse object_store_url")
            .host_str()
            .expect("failed to extract host/bucket from path"),
    );

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let sdk_config = aws_config::load_from_env().await;
    let credentials_providder = sdk_config
        .credentials_provider()
        .expect("could not find credentials provider");
    let credentials = credentials_providder
        .provide_credentials()
        .await
        .expect("could not load credentials");

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(&bucket_name)
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

    ctx.runtime_env()
        .register_object_store("s3", &bucket_name, Arc::new(s3));

    //let store = ctx.runtime_env().object_store(&object_store_url)?;

    //let delta_table_result = deltalake::open_table(path).await;
    let delta_table_result = false;
    if delta_table_result {
        //    let table = delta_table_result.unwrap();
        //    let delta_table_os = DeltaTableWithObjectStore { table, object_store };
        //    ctx.register_table("tbl", Arc::new(delta_table_os))?;
    } else {
        let ltu = ListingTableUrl::parse(data_location)?;
        let mut config = ListingTableConfig::new(ltu);
        config = config.infer_options(&ctx.state.read()).await?;
        config = config.infer_schema(&ctx.state.read()).await?;

        let table = ListingTable::try_new(config)?;
        ctx.register_table("tbl", Arc::new(table))?;
    }

    let query = if args.schema {
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'tbl'"
    } else {
        args.query.as_str()
    };

    let df = ctx.sql(query).await?;
    df.show_limit(args.limit).await?;

    Ok(())
}

/*
pub struct DeltaTableWithObjectStore {
    table: DeltaTable,
    object_store: Arc<dyn ObjectStore>,
}

#[async_trait]
impl TableProvider for DeltaTableWithObjectStore {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::new(
            <ArrowSchema as TryFrom<&deltalake::schema::Schema>>::try_from(
                DeltaTable::schema(&self.table).unwrap(),
            )
                .unwrap(),
        )
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(<ArrowSchema as TryFrom<&deltalake::schema::Schema>>::try_from(
            DeltaTable::schema(&self.table).unwrap(),
        )?);
        let filenames = self.table.get_file_uris();

        let partitions = filenames
            .into_iter()
            .zip(self.table.get_active_add_actions())
            .enumerate()
            .map(|(_idx, (fname, action))| {
                let schema_less_filepath = if let Some((_, path_without_schema)) = fname.split_once("://") {
                    path_without_schema.to_string()
                } else {
                    fname
                };
                Ok(vec![PartitionedFile::new(schema_less_filepath, action.size as u64)])
            })
            .collect::<datafusion::error::Result<_>>()?;

        ParquetFormat::default()
            .create_physical_plan(
                FileScanConfig {
                    object_store: self.object_store.clone(),
                    file_schema: schema,
                    file_groups: partitions,
                    statistics: self.table.datafusion_table_statistics(),
                    projection: projection.clone(),
                    limit,
                    table_partition_cols: self.table.get_metadata().unwrap().partition_columns.clone(),
                },
                filters,
            )
            .await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

 */

fn update_s3_console_url(path: &str) -> Result<String> {
    if path.starts_with("https://s3.console.aws.amazon.com/s3/buckets/") {
        let parsed_url = Url::parse(path)?;
        let path_segments = parsed_url
            .path_segments()
            .map(|c| c.collect::<Vec<_>>())
            .unwrap_or(vec![]);
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
            if maybe_prefix.is_some() {
                let prefix = maybe_prefix.unwrap();
                Ok(format!("s3://{}/{}", bucket_name, prefix))
            } else {
                Ok(path.to_string())
            }
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
