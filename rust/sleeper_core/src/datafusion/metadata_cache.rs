//! Pre-populates `DataFusion`'s file metadata cache. The behaviour of `DataFusion`'s cache is that it will always
//! pre-fetch the Parquet page indexes (column and offset indexes) regardless of the configuration settings.
//! This is usually desirable, but not for most range queries, where very large Parquet files can have 10's of MiB of
//! page indexes. Retrieving those for a range query adds significant latency in retrieving them from S3 and ultimately
//! slows down a query.
//!
//! Since we can't disable reading page indexes in `DataFusion`s caching, we take the approach of prepopulating the cache
//! with the metadata from the Parquet files we are going to read, but do so explicitly without the page indexes
//! loaded. Therefore, when DF metadata caching code checks to see if cached metadata is available for a specific file,
//! it will find the metadata is already cached so will not attempt a further read (thus avoiding reading page indexes).
/*
* Copyright 2022-2026 Crown Copyright
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
use datafusion::{
    datasource::{
        file_format::parquet::ObjectStoreFetch,
        physical_plan::parquet::metadata::CachedParquetMetaData,
    },
    error::DataFusionError,
    execution::{
        cache::cache_manager::{CachedFileMetadataEntry, FileMetadataCache},
        context::SessionContext,
    },
    parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
};
use log::debug;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;
use url::Url;

/// Loads the Parquet metadata for the given file into the cache, if not already present. The [`ObjectStore`] instance is
/// used to retrieve the data, it should be the same one that created the [`ObjectMeta`].
///
/// # Returns
/// The number of rows in the file, as recorded in its Parquet metadata.
///
/// # Errors
/// An error will be returned if the object store couldn't retrieve data or the Parquet file was invalid.
async fn load_metadata(
    cache: Arc<dyn FileMetadataCache>,
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    metadata_size_hint: Option<usize>,
    page_index_policy: PageIndexPolicy,
) -> Result<u64, DataFusionError> {
    if let Some(cached) = cache.get(&meta.location)
        && cached.is_valid_for(&meta)
        && let Some(cached_parquet) = cached
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
    {
        return Ok(row_count(cached_parquet.parquet_metadata()));
    }
    debug!("Pre-fetching {} to metadata cache", meta.location);
    let metadata = Arc::new(
        ParquetMetaDataReader::new()
            .with_prefetch_hint(metadata_size_hint)
            .with_page_index_policy(page_index_policy)
            .load_and_finish(ObjectStoreFetch::new(&store, &meta), meta.size)
            .await?,
    );
    let rows = row_count(&metadata);
    cache.put(
        &meta.location.clone(),
        CachedFileMetadataEntry::new(meta, Arc::new(CachedParquetMetaData::new(metadata))),
    );
    Ok(rows)
}

/// Number of rows in a Parquet file according to its metadata. Negative values are treated as zero.
fn row_count(metadata: &ParquetMetaData) -> u64 {
    u64::try_from(metadata.file_metadata().num_rows()).unwrap_or(0)
}

struct UrlWrap<'a>(&'a Url);

impl AsRef<Url> for UrlWrap<'_> {
    fn as_ref(&self) -> &Url {
        self.0
    }
}

/// Prepopulates the cache with Parquet metadata, loading page indexes according to the given policy.
///
/// # Returns
/// The total number of rows across all the given files, as recorded in their Parquet metadata.
///
/// # Errors
/// Errors can occur due to failures in communicating with the object store or decoding Parquet metadata.
pub async fn prepopulate_metadata_cache(
    ctx: &SessionContext,
    input_files: &[Url],
    object_metas: &[ObjectMeta],
    metadata_size_hint: Option<usize>,
    page_index_policy: PageIndexPolicy,
) -> Result<u64, DataFusionError> {
    let file_metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();

    let mut handles = Vec::new();
    for (meta, url) in object_metas.iter().zip(input_files) {
        let store = ctx.runtime_env().object_store(UrlWrap(url))?;
        handles.push(tokio::spawn(load_metadata(
            file_metadata_cache.clone(),
            store,
            meta.clone(),
            metadata_size_hint,
            page_index_policy,
        )));
    }
    let mut total_rows = 0u64;
    for handle in handles {
        total_rows += handle
            .await
            .map_err(|e| DataFusionError::External(e.into()))?
            .map_err(|e| DataFusionError::External(e.into()))?;
    }
    Ok(total_rows)
}
