#![allow(clippy::result_large_err)]

use assert_cmd::prelude::*;
use predicates::prelude::*;
use predicates::str::RegexPredicate;
use std::env;
use std::process::Command;

fn configure_minio() {
    env::set_var("AWS_REGION", "eu-central-1");
    env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
    env::set_var(
        "AWS_SECRET_ACCESS_KEY",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    );
    env::set_var("AWS_ENDPOINT_URL", "http://localhost:9000");
    env::set_var("AWS_ENDPOINT", "http://localhost:9000");
    env::set_var("AWS_ALLOW_HTTP", "true");
}

fn get_qv_cmd() -> datafusion::common::Result<Command> {
    Ok(Command::new(assert_cmd::cargo::cargo_bin!()))
}

fn build_row_regex_predicate(columns: Vec<&str>) -> RegexPredicate {
    let row = columns
        .into_iter()
        .map(regex::escape)
        .collect::<Vec<_>>()
        .join("\\s*\\|\\s*");
    let pattern = format!(r"\|\s*{row}\s*\|");
    predicate::str::is_match(pattern).unwrap()
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
async fn run_with_s3_parquet_file() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("s3://data/iceberg/db/COVID-19_NYT/data/00000-2-2d39563f-6901-4e2d-9903-84a8eab8ac3d-00001.parquet")
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");

    let header_predicate =
        build_row_regex_predicate(vec!["date", "county", "state", "fips", "cases", "deaths"]);

    let data_predicate = build_row_regex_predicate(vec![
        "2020-01-21",
        "Snohomish",
        "Washington",
        "53061",
        "1",
        "0",
    ]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_s3_console_parquet_file() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("https://s3.console.aws.amazon.com/s3/buckets/data?region=eu-central-1&prefix=iceberg/db/COVID-19_NYT/data/00000-2-2d39563f-6901-4e2d-9903-84a8eab8ac3d-00001.parquet&showversions=false")
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");

    let header_predicate =
        build_row_regex_predicate(vec!["date", "county", "state", "fips", "cases", "deaths"]);

    let data_predicate = build_row_regex_predicate(vec![
        "2020-01-21",
        "Snohomish",
        "Washington",
        "53061",
        "1",
        "0",
    ]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_s3_parquet_files_in_folder_trailing_slash() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("s3://data/iceberg/db/COVID-19_NYT/data/")
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");

    let header_predicate =
        build_row_regex_predicate(vec!["date", "county", "state", "fips", "cases", "deaths"]);

    let data_predicate = build_row_regex_predicate(vec![
        "2020-01-21",
        "Snohomish",
        "Washington",
        "53061",
        "1",
        "0",
    ]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_s3_parquet_files_in_folder_no_trailing_slash() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("s3://data/iceberg/db/COVID-19_NYT/data")
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");

    let header_predicate =
        build_row_regex_predicate(vec!["date", "county", "state", "fips", "cases", "deaths"]);

    let data_predicate = build_row_regex_predicate(vec![
        "2020-01-21",
        "Snohomish",
        "Washington",
        "53061",
        "1",
        "0",
    ]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_s3_iceberg_table() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("s3://data/iceberg/db/COVID-19_NYT")
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");

    let header_predicate =
        build_row_regex_predicate(vec!["date", "county", "state", "fips", "cases", "deaths"]);

    let data_predicate = build_row_regex_predicate(vec![
        "2020-01-21",
        "Snohomish",
        "Washington",
        "53061",
        "1",
        "0",
    ]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_s3_iceberg_metadata_file() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("s3://data/iceberg/db/COVID-19_NYT/metadata/v3.metadata.json")
        .arg("-q")
        .arg("select count(*) as row_count from tbl");

    cmd.assert()
        .success()
        .stdout(build_row_regex_predicate(vec!["row_count"]))
        .stdout(build_row_regex_predicate(vec!["1111930"]));
    Ok(())
}

#[tokio::test]
async fn run_with_s3_iceberg_time_travel() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("s3://data/iceberg/db/COVID-19_NYT/metadata/v2.metadata.json")
        .arg("--at")
        .arg("2022-11-04T03:10:41Z")
        .arg("-q")
        .arg("select count(*) as row_count from tbl");

    cmd.assert()
        .success()
        .stdout(build_row_regex_predicate(vec!["row_count"]))
        .stdout(build_row_regex_predicate(vec!["157865"]));
    Ok(())
}

#[tokio::test]
async fn run_with_rest_catalog_iceberg_table() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("db.COVID-19_NYT")
        .arg("--rest-catalog")
        .arg("http://localhost:8181")
        .arg("--catalog-warehouse")
        .arg("s3://data/iceberg")
        .arg("--catalog-property")
        .arg("s3.endpoint=http://localhost:9000")
        .arg("--catalog-property")
        .arg("s3.path-style-access=true")
        .arg("--catalog-property-env")
        .arg("s3.access-key-id=AWS_ACCESS_KEY_ID")
        .arg("--catalog-property-env")
        .arg("s3.secret-access-key=AWS_SECRET_ACCESS_KEY")
        .arg("--catalog-property-env")
        .arg("s3.region=AWS_REGION")
        .arg("-q")
        .arg("select count(*) as row_count from tbl");

    cmd.assert()
        .success()
        .stdout(build_row_regex_predicate(vec!["row_count"]))
        .stdout(build_row_regex_predicate(vec!["1111930"]));
    Ok(())
}

#[tokio::test]
async fn run_with_s3_deltalake() -> datafusion::common::Result<()> {
    configure_minio();

    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg("s3://data/delta/COVID-19_NYT")
        .arg("--at")
        .arg("2022-01-13T16:39:00+01:00")
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");

    let header_predicate =
        build_row_regex_predicate(vec!["date", "county", "state", "fips", "cases", "deaths"]);

    let data_predicate = build_row_regex_predicate(vec![
        "2020-01-21",
        "Snohomish",
        "Washington",
        "53061",
        "1",
        "0",
    ]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}
