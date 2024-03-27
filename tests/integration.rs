use assert_cmd::cargo::CargoError;
use assert_cmd::prelude::*;
use datafusion::common::DataFusionError;
use predicates::prelude::*;
use std::env;
use std::process::Command;

fn map_cargo_to_datafusion_error(e: CargoError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

fn get_qv_cmd() -> datafusion::common::Result<Command> {
    Command::cargo_bin(env!("CARGO_PKG_NAME")).map_err(map_cargo_to_datafusion_error)
}

#[tokio::test]
async fn run_without_file_exits_with_usage() -> datafusion::common::Result<()> {
    let mut cmd = get_qv_cmd()?;
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Usage: qv <PATH>"));
    Ok(())
}

#[tokio::test]
async fn run_with_local_avro_file() -> datafusion::common::Result<()> {
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
async fn run_with_local_parquet_file() -> datafusion::common::Result<()> {
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
async fn run_with_local_parquet_files_in_folder() -> datafusion::common::Result<()> {
    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg(&get_qv_testing_path("data/iceberg/db/COVID-19_NYT/data"))
        .arg("-q")
        .arg("select * from tbl order by date desc");
    cmd.assert()
        .success()
        .stdout(
            predicate::str::contains(
                r#"| date       | county   | state   | fips | cases | deaths |"#,
            )
            .trim(),
        )
        .stdout(predicate::str::contains(
            r#"| 2021-03-11 | Bibb     | Alabama | 1007 | 2474  | 58     |"#,
        ));
    Ok(())
}

fn configure_minio() {
    env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
    env::set_var(
        "AWS_SECRET_ACCESS_KEY",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    );
    env::set_var("AWS_ENDPOINT_URL", "http://localhost:9000");
}

#[tokio::test]
async fn run_with_s3_parquet_file() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("s3://data/iceberg/db/COVID-19_NYT/data/00000-2-2d39563f-6901-4e2d-9903-84a8eab8ac3d-00001.parquet")
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");
    cmd.assert()
        .success()
        .stdout(
            predicate::str::contains(
                r#"| date       | county   | state   | fips | cases | deaths |"#,
            )
            .trim(),
        )
        .stdout(predicate::str::contains(
            r#"| 2020-01-21 | Snohomish | Washington | 53061 | 1     | 0      |"#,
        ));
    Ok(())
}
