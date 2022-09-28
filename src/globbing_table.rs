use crate::object_store_util::*;
use crate::GlobbingPath;
use chrono::{DateTime, Utc};
use datafusion::common::Result;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaTable, DeltaTableConfig, StorageUrl};
use object_store::path::Path;
use object_store::ObjectMeta;
use std::sync::Arc;

/// Build a table provider for the globbing_path
/// When a globbing pattern is present a ListingTable will be built (using the non-hidden files which match the globbing pattern)
/// Otherwise when _delta_log is present, a DeltaTable will be built
/// Otherwise a ListingTable will be built (using the non-hidden files which match the prefix)
pub async fn build_table_provider(
    ctx: &SessionContext,
    globbing_path: &GlobbingPath,
    maybe_at: &Option<DateTime<Utc>>,
) -> Result<Arc<dyn TableProvider>> {
    let store = globbing_path.get_store(ctx)?;
    let table_arc: Arc<dyn TableProvider> =
        if has_delta_log_folder(store, &globbing_path.prefix).await? {
            let delta_table = load_delta_table(
                ctx,
                &globbing_path.object_store_url,
                &globbing_path.prefix,
                maybe_at,
            )
            .await?;
            Arc::new(delta_table)
        } else {
            let listing_table = load_listing_table(ctx, globbing_path).await?;
            Arc::new(listing_table)
        };
    Ok(table_arc)
}

async fn load_listing_table(
    ctx: &SessionContext,
    globbing_path: &GlobbingPath,
) -> Result<ListingTable> {
    let matching_file_urls = list_matching_table_urls(ctx, globbing_path).await?;
    assert!(!matching_file_urls.is_empty());
    let mut config = ListingTableConfig::new_with_multi_paths(matching_file_urls);
    config = config.infer_options(&ctx.state()).await?;
    config = config.infer_schema(&ctx.state()).await?;
    let table = ListingTable::try_new(config)?;
    Ok(table)
}

async fn list_matching_table_urls(
    ctx: &SessionContext,
    globbing_path: &GlobbingPath,
) -> Result<Vec<ListingTableUrl>> {
    let store = globbing_path.get_store(ctx)?;

    let predicate = |meta: &ObjectMeta| {
        let visible = !is_hidden(&meta.location);
        let glob_ok = globbing_path
            .maybe_glob
            .clone()
            .map(|glob| glob.matches(meta.location.as_ref()))
            .unwrap_or(true);
        visible && glob_ok
    };

    let matching_files = list_matching_files(store, &globbing_path.prefix, predicate).await?;

    let matching_listing_table_urls = matching_files
        .iter()
        .map(|x| globbing_path.build_listing_table_url(x))
        .collect();

    Ok(matching_listing_table_urls)
}

async fn load_delta_table(
    ctx: &SessionContext,
    object_store_url: &ObjectStoreUrl,
    path: &Path,
    maybe_at: &Option<DateTime<Utc>>,
) -> Result<DeltaTable> {
    let store = ctx.runtime_env().object_store(&object_store_url)?;
    let data_location = format!("{}{}", &object_store_url.as_str(), &path.as_ref());
    let delta_storage_url = StorageUrl::parse(&data_location).expect("failed to parse storage url");
    let delta_storage = DeltaObjectStore::new(delta_storage_url, store);
    let delta_config = DeltaTableConfig::default();
    let mut delta_table = DeltaTable::new(Arc::new(delta_storage), delta_config);
    let delta_table_load_result = match *maybe_at {
        Some(at) => delta_table.load_with_datetime(at).await,
        None => delta_table.load().await,
    };
    delta_table_load_result
        .map(|_| delta_table)
        .map_err(|dte| DataFusionError::External(Box::new(dte)))
}
