use aws_types::SdkConfig;
use datafusion::common::Result;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use std::env;
use std::sync::Arc;
use url::Url;

pub async fn register_object_store(
    maybe_sdk_config: &Option<SdkConfig>,
    ctx: &SessionContext,
    object_store_url: &ObjectStoreUrl,
) -> Result<()> {
    if object_store_url.as_str().starts_with("s3://") {
        let bucket_name = extract_bucket_name(object_store_url);
        let s3 = build_s3(&bucket_name).await?;
        ctx.runtime_env()
            .register_object_store("s3", &bucket_name, Arc::new(s3));
    }
    if object_store_url.as_str().starts_with("gs://") {
        let bucket_name = extract_bucket_name(object_store_url);
        let gcs = build_gcs(&bucket_name)?;
        ctx.runtime_env()
            .register_object_store("gs", &bucket_name, Arc::new(gcs));
    }
    Ok(())
}

fn extract_bucket_name(object_store_url: &ObjectStoreUrl) -> String {
    let bucket_name = String::from(
        Url::parse(object_store_url.as_str())
            .expect("failed to parse object_store_url")
            .host_str()
            .expect("failed to extract host/bucket from path"),
    );
    bucket_name
}

fn build_gcs(bucket_name: &str) -> Result<GoogleCloudStorage> {
    let google_application_credentials = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .expect("Could not find GOOGLE_APPLICATION_CREDENTIALS env variable");

    let gcs_builder = GoogleCloudStorageBuilder::new();
    let gcs_builder = gcs_builder.with_bucket_name(bucket_name);
    let gcs_builder = gcs_builder.with_service_account_path(google_application_credentials);
    let gcs = gcs_builder.build()?;
    Ok(gcs)
}

async fn build_s3(bucket_name: &str) -> Result<AmazonS3> {
    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
    .build()?;

    Ok(s3)
}

/// List all the objects with the given prefix and for which the predicate closure returns true.
// Prefixes are evaluated on a path segment basis, i.e. foo/bar/ is a prefix of foo/bar/x but not of foo/bar_baz/x.
pub async fn list_matching_files<P>(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    predicate: P,
) -> Result<Vec<ObjectMeta>>
where
    P: FnMut(&ObjectMeta) -> bool,
{
    let items: Vec<ObjectMeta> = store.list(Some(prefix)).await?.try_collect().await?;
    let items = if items.is_empty() {
        vec![store.head(prefix).await?]
    } else {
        items
    };
    let filtered_items = items.into_iter().filter(predicate).collect();
    Ok(filtered_items)
}

/// Determines whether a file is "hidden"
pub fn is_hidden(path: &Path) -> bool {
    path.parts()
        .find(|part| part.as_ref().starts_with('.') || part.as_ref().starts_with('_'))
        .map_or_else(|| false, |_| true)
}

#[test]
fn test_is_hidden() {
    assert!(!is_hidden(&Path::parse("a").unwrap()));
    assert!(!is_hidden(&Path::parse("a/b").unwrap()));
    assert!(is_hidden(&Path::parse(".hidden").unwrap()));
    assert!(is_hidden(&Path::parse("_hidden").unwrap()));
    assert!(is_hidden(&Path::parse("a/.hidden").unwrap()));
    assert!(is_hidden(&Path::parse("a/_hidden").unwrap()));
    assert!(is_hidden(&Path::parse("a/.hidden/b").unwrap()));
    assert!(is_hidden(&Path::parse("a/_hidden/b").unwrap()));
    assert!(is_hidden(&Path::parse("a/.hidden/b").unwrap()));
}

/// Determines whether there is a _delta_log folder
pub async fn has_delta_log_folder(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<bool> {
    let to_probe = Path::parse(format!("{}/_delta_log", prefix))?;
    let predicate = |meta: &ObjectMeta| {
        let json_file = meta.location.as_ref().ends_with(".json");
        json_file
    };
    let result: bool = list_matching_files(store, &to_probe, predicate)
        .await
        .map(|files| !files.is_empty())
        .unwrap_or_else(|_| false);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::path::PathBuf;

    fn get_project_path() -> Result<PathBuf> {
        std::path::Path::new(".").canonicalize().map_err(Into::into)
    }

    fn get_testing_path() -> Result<PathBuf> {
        get_project_path().map(|pb| pb.join("testing"))
    }

    fn get_testing_data_path() -> Result<PathBuf> {
        get_testing_path().map(|pb| pb.join("data"))
    }

    #[tokio::test]
    async fn test_has_delta_log_folder() -> Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::default());

        let testing_data_path = get_testing_data_path()?;

        let csv_file = testing_data_path.join("csv/aggregate_test_100.csv");
        assert!(!has_delta_log_folder(&store.clone(), &Path::from_absolute_path(csv_file)?).await?);

        let csv_folder = testing_data_path.join("csv");
        assert!(
            !has_delta_log_folder(&store.clone(), &Path::from_absolute_path(csv_folder)?).await?
        );

        // TODO! fix this on CI
        //let delta_folder = testing_data_path.join("delta");
        //let covid_delta_folder = delta_folder.join("COVID-19_NYT");
        //assert!(has_delta_log_folder(store.clone(), &Path::from_absolute_path(covid_delta_folder)?).await?);

        Ok(())
    }
}
