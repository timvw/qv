use crate::object_store_util::*;
use crate::GlobbingPath;
use anyhow::Result;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use deltalake::storage::DeltaObjectStore;
use deltalake::{DeltaTable, DeltaTableConfig, DeltaTableError, StorageUrl};
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
) -> Result<Arc<dyn TableProvider>> {
    let table_arc: Arc<dyn TableProvider> = if globbing_path.maybe_glob.is_some() {
        let table = load_listing_table(ctx, globbing_path).await?;
        Arc::new(table)
    } else if globbing_path.maybe_glob.is_none() {
        match load_delta_table(ctx, &globbing_path.object_store_url, &globbing_path.prefix).await {
            Ok(delta_table) => Arc::new(delta_table),
            Err(_) => {
                let table = load_listing_table(ctx, globbing_path).await?;
                Arc::new(table)
            }
        }
    } else {
        let table = load_listing_table(ctx, globbing_path).await?;
        Arc::new(table)
    };
    Ok(table_arc)
}

async fn load_listing_table(
    ctx: &SessionContext,
    globbing_path: &GlobbingPath,
) -> Result<ListingTable> {
    let matching_files = list_glob_matching_files(ctx, globbing_path).await?;
    let matching_file_urls: Vec<_> = matching_files
        .iter()
        .map(|x| globbing_path.build_listing_table_url(x))
        .collect();

    let mut config = ListingTableConfig::new_with_multi_paths(matching_file_urls);
    config = config.infer_options(&ctx.state()).await?;
    config = config.infer_schema(&ctx.state()).await?;
    let table = ListingTable::try_new(config)?;
    Ok(table)
}

async fn list_glob_matching_files(
    ctx: &SessionContext,
    globbing_path: &GlobbingPath,
) -> Result<Vec<ObjectMeta>> {
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

    list_matching_files(store, &globbing_path.prefix, predicate).await
}

async fn load_delta_table(
    ctx: &SessionContext,
    object_store_url: &ObjectStoreUrl,
    path: &Path,
) -> Result<DeltaTable, DeltaTableError> {
    let store = ctx.runtime_env().object_store(&object_store_url)?;
    let data_location = format!("{}{}", &object_store_url.as_str(), &path.as_ref());
    let delta_storage_url = StorageUrl::parse(&data_location).expect("failed to parse storage url");
    let delta_storage = DeltaObjectStore::new(delta_storage_url, store);
    let delta_config = DeltaTableConfig::default();
    let mut delta_table = DeltaTable::new(Arc::new(delta_storage), delta_config);
    let delta_table_load_result = delta_table.load().await;
    delta_table_load_result.map(|_| delta_table)
}
