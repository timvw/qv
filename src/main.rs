use clap::Parser;
use datafusion::catalog::TableReference;
use std::sync::Arc;
//use datafusion::catalog::TableReference;

use datafusion::common::Result;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;

mod args;
mod globbing_path;
mod globbing_table;
mod object_store_util;

use crate::args::Args;
use crate::globbing_path::GlobbingPath;
//use crate::globbing_table::build_table_provider;
//use crate::object_store_util::register_object_store;

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    let args: Args = Args::parse();
    //let globbing_path = args.get_globbing_path().await?;
    //register_object_store(&ctx, &globbing_path.object_store_url).await?;

    let table_path = ListingTableUrl::parse(&args.path)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert_cmd::cargo::CargoError;
    use assert_cmd::prelude::*;
    use datafusion::common::DataFusionError;
    use predicates::prelude::*;
    use std::env;
    use std::process::Command;

    fn map_cargo_to_datafusion_error(e: CargoError) -> DataFusionError {
        DataFusionError::External(Box::new(e))
    }

    fn get_qv_cmd() -> Result<Command> {
        Command::cargo_bin("qv").map_err(map_cargo_to_datafusion_error)
    }

    #[tokio::test]
    async fn run_without_file_exits_with_usage() -> Result<()> {
        let mut cmd = get_qv_cmd()?;
        cmd.assert()
            .failure()
            .stderr(predicate::str::contains("Usage: qv <PATH>"));
        Ok(())
    }

    #[tokio::test]
    async fn run_with_local_avro_file() -> Result<()> {
        let mut cmd = get_qv_cmd()?;
        let cmd = cmd.arg(get_qv_testing_path("data/avro/alltypes_plain.avro"));
        cmd.assert().success()
            .stdout(predicate::str::contains("| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |"))
            .stdout(predicate::str::contains("| 4  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30332f30312f3039 | 30         | 2009-03-01T00:00:00 |"));
        Ok(())
    }

    fn get_qv_testing_path(rel_data_path: &str) -> String {
        let testing_path = env::var("QV_TESTING_PATH").unwrap_or_else(|_| "./testing".to_string());
        format!("{}/{}", testing_path, rel_data_path)
    }

    #[tokio::test]
    async fn run_with_local_parquet_file() -> Result<()> {
        let mut cmd = get_qv_cmd()?;
        let cmd = cmd.arg(get_qv_testing_path(
            "data/parquet/generated_simple_numerics/blogs.parquet",
        ));
        cmd.assert()
            .success()
            .stdout(predicate::str::contains(
                r#"| reply                                        | blog_id              |"#,
            ))
            .stdout(predicate::str::contains(
                r#"| {reply_id: 332770973, next_id: }             | -1473106667809783919 |"#,
            ));
        Ok(())
    }

    #[tokio::test]
    async fn run_with_local_parquet_files_in_folder() -> Result<()> {
        let mut cmd = get_qv_cmd()?;
        let cmd = cmd.arg(get_qv_testing_path(
            "data/iceberg/db/COVID-19_NYT/data",
        ));
        cmd.assert()
            .success()
            .stdout(predicate::str::contains(
                r#"| date       | county     | state    | fips  | cases | deaths |"#,
            ))
            .stdout(predicate::str::contains(
                r#"| 2020-05-19 | Lawrence   | Illinois | 17101 | 4     | 0      |"#,
            ));
        Ok(())
    }
}
