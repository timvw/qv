/*
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
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

    pub fn get_store(&self, ctx: &SessionContext) -> Result<Arc<dyn ObjectStore>> {
        ctx.runtime_env().object_store(&self.object_store_url)
    }
}

/// Update the s such that it starts with a scheme
/// In case no scheme is provided, we assume the s is a (globbing) expression on the local filesystem.
fn ensure_scheme(s: &str) -> Result<String> {
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

    let non_globbed_url =
        Url::parse(non_globbed_path).map_err(|e| DataFusionError::External(Box::new(e)))?;
    let (object_store_url, prefix) = match non_globbed_url.scheme() {
        "file" => ObjectStoreUrl::parse("file://").map(|osu| (osu, non_globbed_url.path())),
        _ => ObjectStoreUrl::parse(
            &non_globbed_url[url::Position::BeforeScheme..url::Position::BeforePath],
        )
        .map(|osu| (osu, non_globbed_url.path())),
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
        Some(Err(e)) => Err(e),
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

    // let actual = extract_path_parts("s3://bucket/a/b* /c").unwrap();
    // assert_eq!("s3://bucket/", actual.0.as_str());
    // assert_eq!("a", actual.1.as_ref());
    // assert_eq!(Some(Pattern::new("a/b* /c").unwrap()), actual.2);

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

    // let actual = extract_path_parts("file:///a/b* /c").unwrap();
    // assert_eq!("file:///", actual.0.as_str());
    // assert_eq!("a", actual.1.as_ref());
    // assert_eq!(Some(Pattern::new("a/b* /c").unwrap()), actual.2);
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
*/
