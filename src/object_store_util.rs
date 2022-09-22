use aws_types::credentials::ProvideCredentials;
use aws_types::SdkConfig;
use datafusion::common::Result;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;
use url::Url;

pub async fn register_object_store(
    ctx: &SessionContext,
    object_store_url: &ObjectStoreUrl,
) -> Result<()> {
    if object_store_url.as_str().starts_with("s3://") {
        let bucket_name = String::from(
            Url::parse(object_store_url.as_str())
                .expect("failed to parse object_store_url")
                .host_str()
                .expect("failed to extract host/bucket from path"),
        );

        let sdk_config = aws_config::load_from_env().await;
        let s3 = build_s3_from_sdk_config(&bucket_name, &sdk_config).await?;
        ctx.runtime_env()
            .register_object_store("s3", &bucket_name, Arc::new(s3));
    }
    Ok(())
}

async fn build_s3_from_sdk_config(bucket_name: &str, sdk_config: &SdkConfig) -> Result<AmazonS3> {
    let credentials_providder = sdk_config
        .credentials_provider()
        .expect("could not find credentials provider");
    let credentials = credentials_providder
        .provide_credentials()
        .await
        .expect("could not load credentials");

    let s3_builder = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(
            sdk_config
                .region()
                .expect("could not find region")
                .to_string(),
        )
        .with_access_key_id(credentials.access_key_id())
        .with_secret_access_key(credentials.secret_access_key());

    let s3 = match credentials.session_token() {
        Some(session_token) => s3_builder.with_token(session_token),
        None => s3_builder,
    }
    .build()?;

    Ok(s3)
}

/// List all the objects with the given prefix and for which the predicate closure returns true.
// Prefixes are evaluated on a path segment basis, i.e. foo/bar/ is a prefix of foo/bar/x but not of foo/bar_baz/x.
pub async fn list_matching_files<P>(
    store: Arc<dyn ObjectStore>,
    prefix: &Path,
    predicate: P,
) -> Result<Vec<ObjectMeta>>
where
    P: FnMut(&ObjectMeta) -> bool,
{
    let items: Vec<ObjectMeta> = match store.head(prefix).await {
        Ok(meta) => vec![meta],
        Err(_) => store.list(Some(prefix)).await?.try_collect().await?,
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
