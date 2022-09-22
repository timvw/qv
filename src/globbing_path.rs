use anyhow::Result;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaTable, DeltaTableConfig, DeltaTableError, StorageUrl};
use futures::TryStreamExt;
use glob::Pattern;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;
use url::{ParseError, Url};

pub struct GlobbingPath {
    pub object_store_url: ObjectStoreUrl,
    pub prefix: Path,
    pub maybe_glob: Option<Pattern>,
}

impl GlobbingPath {
    /// Try to interpret the provided s as a globbing path
    pub fn parse(s: &str) -> Result<GlobbingPath> {
        let s_with_scheme = ensure_scheme(s)?;
        let (object_store_url, prefix, maybe_glob) = extract_path_parts(&s_with_scheme)?;
        Ok(GlobbingPath {
            object_store_url,
            prefix,
            maybe_glob,
        })
    }

    /// Build a ListingTableUrl for the object(meta)
    pub fn build_listing_table_url(&self, object_meta: &ObjectMeta) -> ListingTableUrl {
        let s = format!(
            "{}{}",
            self.object_store_url.as_str(),
            object_meta.location.as_ref()
        );
        ListingTableUrl::parse(&s)
            .unwrap_or_else(|_| panic!("failed to build ListingTableUrl from {}", s))
    }

    /// Build a table provider for the globbing_path
    /// When a globbing pattern is present a ListingTable will be built (using the non-hidden files which match the globbing pattern)
    /// Otherwise when _delta_log is present, a DeltaTable will be built
    /// Otherwise a ListingTable will be built (using the non-hidden files which match the prefix)
    pub async fn build_table_provider(
        &self,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_arc: Arc<dyn TableProvider> = if self.maybe_glob.is_some() {
            let table = self.load_listing_table(ctx).await?;
            Arc::new(table)
        } else if self.maybe_glob.is_none() {
            match self.load_delta_table(ctx).await {
                Ok(delta_table) => Arc::new(delta_table),
                Err(_) => {
                    let table = self.load_listing_table(ctx).await?;
                    Arc::new(table)
                }
            }
        } else {
            let table = self.load_listing_table(ctx).await?;
            Arc::new(table)
        };
        Ok(table_arc)
    }

    async fn load_listing_table(&self, ctx: &SessionContext) -> Result<ListingTable> {
        let matching_files = self.list_glob_matching_files(ctx).await?;
        let matching_file_urls: Vec<_> = matching_files
            .iter()
            .map(|x| self.build_listing_table_url(x))
            .collect();

        let mut config = ListingTableConfig::new_with_multi_paths(matching_file_urls);
        config = config.infer_options(&ctx.state()).await?;
        config = config.infer_schema(&ctx.state()).await?;
        let table = ListingTable::try_new(config)?;
        Ok(table)
    }

    async fn load_delta_table(&self, ctx: &SessionContext) -> Result<DeltaTable, DeltaTableError> {
        let data_location = format!(
            "{}{}",
            &self.object_store_url.as_str(),
            &self.prefix.as_ref()
        );
        let store = ctx.runtime_env().object_store(&self.object_store_url)?;
        let delta_storage_url =
            StorageUrl::parse(&data_location).expect("failed to parse storage url");
        let delta_storage = DeltaObjectStore::new(delta_storage_url, store);
        let delta_config = DeltaTableConfig::default();
        let mut delta_table = DeltaTable::new(Arc::new(delta_storage), delta_config);
        let delta_table_load_result = delta_table.load().await;
        delta_table_load_result.map(|_| delta_table)
    }

    async fn list_glob_matching_files(&self, ctx: &SessionContext) -> Result<Vec<ObjectMeta>> {
        let store = ctx.runtime_env().object_store(&self.object_store_url)?;

        let predicate = |meta: &ObjectMeta| {
            let visible = !is_hidden(&meta.location);
            let glob_ok = self
                .maybe_glob
                .clone()
                .map(|glob| glob.matches(meta.location.as_ref()))
                .unwrap_or(true);
            visible && glob_ok
        };

        list_matching_files(store, &self.prefix, predicate).await
    }
}

/// Update the s such that it starts with a scheme
/// In case no scheme is provided, we assume the s is a (globbing) expression on the local filesystem.
fn ensure_scheme(s: &str) -> Result<String, DataFusionError> {
    let (leading_non_globbed, maybe_trailing_globbed) = match split_glob_expression(s) {
        Some((non_globbed, globbed)) => (non_globbed, Some(globbed)),
        None => (s, None),
    };

    let leading_non_globbed_with_scheme = match Url::parse(leading_non_globbed) {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            let local_path = std::path::Path::new(leading_non_globbed).canonicalize()?;
            if local_path.is_file() {
                Url::from_file_path(&local_path)
            } else {
                Url::from_directory_path(&local_path)
            }
            .map_err(|_| {
                DataFusionError::Execution(format!(
                    "failed to build url from file path for {:?}",
                    &local_path
                ))
            })
        }
        Err(e) => Err(DataFusionError::Execution(format!(
            "Failed to parse {} as url because {}",
            s, e
        ))),
    }?;

    let s_with_scheme = match maybe_trailing_globbed {
        Some(globbed_path) => format!(
            "{}{}",
            leading_non_globbed_with_scheme.as_str(),
            globbed_path
        ),
        None => String::from(leading_non_globbed_with_scheme.as_str()),
    };

    Ok(s_with_scheme)
}

#[test]
fn test_ensure_scheme() {
    assert_eq!("s3://", ensure_scheme("s3://").unwrap());
    assert_eq!("s3://bucket", ensure_scheme("s3://bucket").unwrap());
    assert_eq!("s3://bucket/a", ensure_scheme("s3://bucket/a").unwrap());
    assert_eq!("file:///", ensure_scheme("file:///").unwrap());
    assert_eq!("file:///a", ensure_scheme("file:///a").unwrap());

    let actual_relative_file = ensure_scheme("src/main.rs").unwrap();
    assert!(actual_relative_file.starts_with("file:///"));
    assert!(actual_relative_file.ends_with("src/main.rs"));

    let actual_relative_dir = ensure_scheme("src").unwrap();
    assert!(actual_relative_dir.starts_with("file:///"));
    assert!(actual_relative_dir.ends_with("src/"));

    let actual_relative_file_glob = ensure_scheme("src/ma*.rs").unwrap();
    assert!(actual_relative_file_glob.starts_with("file:///"));
    assert!(actual_relative_file_glob.ends_with("src/ma*.rs"));

    let actual_relative_dir_glob = ensure_scheme("s*c").unwrap();
    assert!(actual_relative_dir_glob.starts_with("file:///"));
    assert!(actual_relative_dir_glob.ends_with("s*c"));
}

fn extract_path_parts(path_with_scheme: &str) -> Result<(ObjectStoreUrl, Path, Option<Pattern>)> {
    let (non_globbed_path, maybe_globbed_path) = match split_glob_expression(path_with_scheme) {
        Some((non_globbed, globbed)) => (non_globbed, Some(globbed)),
        None => (path_with_scheme, None),
    };

    let non_globbed_url = Url::parse(non_globbed_path)?;
    let (object_store_url, prefix) = match non_globbed_url.scheme() {
        "file" => ObjectStoreUrl::parse("file://").map(|osu| (osu, non_globbed_url.path())),
        "s3" => ObjectStoreUrl::parse(&non_globbed_url[..url::Position::BeforePath])
            .map(|osu| (osu, non_globbed_url.path())),
        _ => Err(DataFusionError::NotImplemented(format!(
            "no support scheme {}.",
            non_globbed_url.scheme()
        ))),
    }?;

    let prefix_without_leading_delimiter = prefix.strip_prefix('/').unwrap_or(prefix);
    let maybe_glob = maybe_globbed_path
        .map(|globbed_path| format!("{}{}", &prefix_without_leading_delimiter, globbed_path));

    let maybe_result = maybe_glob.map(|glob| {
        Pattern::new(&glob).map_err(|_| {
            DataFusionError::Execution(format!("Failed to parse {} as a globbing pattern", &glob))
        })
    });

    let prefix_path = Path::parse(prefix_without_leading_delimiter)?;

    match maybe_result {
        Some(Ok(pattern)) => Ok((object_store_url, prefix_path, Some(pattern))),
        Some(Err(e)) => Err(anyhow::Error::from(e)),
        None => Ok((object_store_url, prefix_path, None)),
    }
}

#[test]
fn test_extract_path_parts() {
    let actual = extract_path_parts("s3://bucket").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("", actual.1.as_ref());
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("s3://bucket/").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("", actual.1.as_ref());
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("s3://bucket/a").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("a", actual.1.as_ref());
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("s3://bucket/a*").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("", actual.1.as_ref());
    assert_eq!(Some(Pattern::new("a*").unwrap()), actual.2);

    let actual = extract_path_parts("s3://bucket/a/b*").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("a", actual.1.as_ref());
    assert_eq!(Some(Pattern::new("a/b*").unwrap()), actual.2);

    let actual = extract_path_parts("s3://bucket/a/b*/c").unwrap();
    assert_eq!("s3://bucket/", actual.0.as_str());
    assert_eq!("a", actual.1.as_ref());
    assert_eq!(Some(Pattern::new("a/b*/c").unwrap()), actual.2);

    let actual = extract_path_parts("file://").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("", actual.1.as_ref());
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("file:///a").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("a", actual.1.as_ref());
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("file:///c/b").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("c/b", actual.1.as_ref());
    assert_eq!(None, actual.2);

    let actual = extract_path_parts("file:///c/b*").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("c", actual.1.as_ref());
    assert_eq!(Some(Pattern::new("c/b*").unwrap()), actual.2);

    let actual = extract_path_parts("file://c*").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("", actual.1.as_ref());
    assert_eq!(Some(Pattern::new("c*").unwrap()), actual.2);

    let actual = extract_path_parts("file:///a/b*/c").unwrap();
    assert_eq!("file:///", actual.0.as_str());
    assert_eq!("a", actual.1.as_ref());
    assert_eq!(Some(Pattern::new("a/b*/c").unwrap()), actual.2);
}

/// Splits `path` at the first path segment containing a glob expression, returning
/// `None` if no glob expression found.
///
/// Path delimiters are determined using [`std::path::is_separator`] which
/// permits `/` as a path delimiter even on Windows platforms.
///
fn split_glob_expression(path: &str) -> Option<(&str, &str)> {
    const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

    let mut last_separator = 0;

    for (byte_idx, char) in path.char_indices() {
        if GLOB_START_CHARS.contains(&char) {
            if last_separator == 0 {
                return Some((".", path));
            }
            return Some(path.split_at(last_separator));
        }

        if std::path::is_separator(char) {
            last_separator = byte_idx + char.len_utf8();
        }
    }
    None
}

/// List all the objects with the given prefix and for which the predicate closure returns true.
// Prefixes are evaluated on a path segment basis, i.e. foo/bar/ is a prefix of foo/bar/x but not of foo/bar_baz/x.
async fn list_matching_files<P>(
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

fn is_hidden(path: &Path) -> bool {
    path.parts()
        .find(|part| part.as_ref().starts_with('.') || part.as_ref().starts_with('_'))
        .map_or_else(|| false, |_| true)
}
