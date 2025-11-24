#![allow(clippy::result_large_err)]

use assert_cmd::cargo::CargoError;
use assert_cmd::prelude::*;
use datafusion::common::DataFusionError;
use predicates::prelude::*;
use predicates::str::RegexPredicate;
use std::env;
use std::process::Command;

fn map_cargo_to_datafusion_error(e: CargoError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

fn get_qv_cmd() -> datafusion::common::Result<Command> {
    Command::cargo_bin(env!("CARGO_PKG_NAME")).map_err(map_cargo_to_datafusion_error)
}

fn get_qv_testing_path(rel_data_path: &str) -> String {
    let testing_path = env::var("QV_TESTING_PATH").unwrap_or_else(|_| "./testing".to_string());
    format!("{}/{}", testing_path, rel_data_path)
}

fn build_row_regex_predicate(columns: Vec<&str>) -> RegexPredicate {
    let pattern = columns.join("\\s*|\\s*");
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
async fn run_with_local_avro_file() -> datafusion::common::Result<()> {
    let mut cmd = get_qv_cmd()?;
    let cmd = cmd.arg(get_qv_testing_path("data/avro/alltypes_plain.avro"));

    let header_predicate = build_row_regex_predicate(vec![
        "id",
        "bool_col",
        "tinyint_col",
        "smallint_col",
        "int_col",
        "bigint_col",
        "float_col",
        "double_col",
        "date_string_col",
        "string_col",
        "timestamp_col",
    ]);

    let data_predicate = build_row_regex_predicate(vec![
        "4",
        "true",
        "0",
        "0",
        "0",
        "0",
        "0.0",
        "0.0",
        "30332f30312f3039",
        "30",
        "2009-03-01T00:00:00",
    ]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_local_ndjson_file() -> datafusion::common::Result<()> {
    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg(get_qv_testing_path("data/json/ndjson-sample.json"))
        .arg("-q")
        .arg("SELECT url from tbl");

    let header_predicate = build_row_regex_predicate(vec!["url"]);

    let data_predicate = build_row_regex_predicate(vec!["https://www.yelp.com/search"]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_local_ndjson_gz_file() -> datafusion::common::Result<()> {
    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg(get_qv_testing_path("data/json/ndjson-sample.json.gz"))
        .arg("-q")
        .arg("SELECT url from tbl");

    let header_predicate = build_row_regex_predicate(vec!["url"]);

    let data_predicate = build_row_regex_predicate(vec!["https://www.yelp.com/search"]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_local_parquet_file() -> datafusion::common::Result<()> {
    let mut cmd = get_qv_cmd()?;
    let cmd = cmd.arg(get_qv_testing_path(
        "data/parquet/generated_simple_numerics/blogs.parquet",
    ));

    let header_predicate = build_row_regex_predicate(vec!["reply", "blog_id"]);

    let data_predicate = build_row_regex_predicate(vec![
        "\\{reply_id: 332770973, next_id: }",
        "-1473106667809783919",
    ]);

    cmd.assert()
        .success()
        .stdout(header_predicate)
        .stdout(data_predicate);
    Ok(())
}

#[tokio::test]
async fn run_with_local_parquet_files_in_folder() -> datafusion::common::Result<()> {
    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg(get_qv_testing_path("data/iceberg/db/COVID-19_NYT/data"))
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");

    let header_predicate =
        build_row_regex_predicate(vec!["date", "county", "state", "fips", "case", "deaths"]);

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
async fn run_with_local_deltalake() -> datafusion::common::Result<()> {
    let mut cmd = get_qv_cmd()?;
    let cmd = cmd
        .arg(get_qv_testing_path("data/delta/COVID-19_NYT"))
        .arg("--at")
        .arg("2022-01-13T16:39:00+01:00")
        .arg("-q")
        .arg("select * from tbl order by date, county, state, fips, cases, deaths");

    let header_predicate =
        build_row_regex_predicate(vec!["date", "county", "state", "fips", "case", "deaths"]);

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
