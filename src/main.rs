use clap::Parser;
use datafusion::common::Result;
use datafusion::prelude::*;

mod args;
mod globbing_path;
mod globbing_table;
mod object_store_util;

use crate::args::Args;
use crate::globbing_path::GlobbingPath;
use crate::globbing_table::build_table_provider;
use crate::object_store_util::register_object_store;

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    let args: Args = Args::parse();
    let (globbing_path, maybe_sdk_config) = args.get_globbing_path().await?;
    register_object_store(&maybe_sdk_config, &ctx, &globbing_path.object_store_url).await?;

    let table_arc = build_table_provider(&ctx, &globbing_path, &args.at).await?;
    ctx.register_table("tbl", table_arc)?;

    let query = &args.get_query();
    let df = ctx.sql(query).await?;
    df.show_limit(args.limit).await?;

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
            .stdout(predicate::str::contains("| 4  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30332f30312f3039 | 30         | 2009-03-01 00:00:00 |"));
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
                r#"| reply                                            | blog_id              |"#,
            ))
            .stdout(predicate::str::contains(
                r#"| {"reply_id": 332770973, "next_id": null}         | -1473106667809783919 |"#,
            ));
        Ok(())
    }

    use object_store::aws::AmazonS3Builder;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn read_s3_iceberg_table() -> Result<()> {
        let bucket_name = "data";
        let path = Path::parse("iceberg/db/COVID-19_NYT")?;

        let store = get_local_minio_s3_object_store(bucket_name)?;

        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store("s3", bucket_name, store);

        let _ = GlobbingPath::parse(&format!("s3://{}/{}", bucket_name, path))?;
        /*
        let table_provider = build_table_provider(&ctx, &globbing_path, &None).await?;

        ctx.register_table("t", table_provider)?;

        let rb = ctx.sql("select * from t").await?.collect().await?;
        assert_eq!(rb.len(), 139);*/
        Ok(())
    }

    fn get_local_minio_s3_object_store(bucket_name: &str) -> Result<Arc<dyn ObjectStore>> {
        /*
                docker run \
        --detach \
        --rm \
        --publish 9000:9000 \
        --publish 9001:9001 \
        --name minio \
        --volume "/Users/timvw/src/github/qv/testing:/data" \
        --env "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
        --env "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
        quay.io/minio/minio:RELEASE.2022-05-26T05-48-41Z server /data \
        --console-address ":9001"
                 */
        let s3 = AmazonS3Builder::new()
            .with_region("eu-central-1")
            .with_access_key_id("AKIAIOSFODNN7EXAMPLE")
            .with_secret_access_key("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .with_bucket_name(bucket_name)
            .build()?;
        let store: Arc<dyn ObjectStore> = Arc::new(s3);
        Ok(store)
    }
}
