use anyhow::Result;
use camino::Utf8PathBuf;
use clap::Parser;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::prelude::*;
use datafusion_objectstore_s3::object_store::s3::S3FileSystem;
use std::sync::Arc;
use datafusion::datafusion_data_access::object_store::ObjectStore;
use std::any::Any;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::file_format::FileScanConfig;
use deltalake::DeltaTable;
use async_trait::async_trait;

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

    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let s3_fs = Arc::new(S3FileSystem::default().await);
    ctx.runtime_env().register_object_store("s3", s3_fs);

    let (object_store, path) = ctx
        .runtime_env()
        .object_store_registry
        .get_by_uri(args.path.as_str())?;

    let delta_table_result = deltalake::open_table(path).await;
    if delta_table_result.is_ok() {
        let table = delta_table_result.unwrap();
        let delta_table_os = DeltaTableWithObjectStore { table, object_store };
        ctx.register_table("tbl", Arc::new(delta_table_os))?;
    } else {
        let config = ListingTableConfig::new(object_store, path).infer().await?;
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
                let sn = &fname[5..]; // strip s3://
                let sns = sn.to_string();
                // TODO: no way to associate stats per file in datafusion at the moment, see:
                // https://github.com/apache/arrow-datafusion/issues/1301
                Ok(vec![PartitionedFile::new(sns, action.size as u64)])
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
