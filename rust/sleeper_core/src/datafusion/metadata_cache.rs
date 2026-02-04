//! Pre-populates `DataFusion`'s file metadata cache. The behaviour of `DataFusion`'s cache is that it will always
//! pre-fetch the Parquet page indexes (column and offset indexes)regardless of the configuration settings.
//! This is usually desirable, but not for most range queries, where very large Parquet files can have 10's of MiB of
//! page indexes. Retrieving those for a range query adds significant latency in retrieving them from S3 and ultimately
//! slows down a query.
//!
//! Since we can't disable the behaviour we want in `DataFusion` we take the approach of prepopulating the cache
//! with the metadata from the Parquet files we are going to read, but do so explicitly without the page indexes
//! loaded.
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
    execution::{cache::cache_manager::FileMetadataCache, context::SessionContext},
    parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader},
};
use log::debug;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;
use url::Url;

/// Loads the Parquet metadata for the given file. The [`ObjectStore`] instance is used to retrieve the data, it should
/// be the same one that created the [`ObjectMeta`].
///
/// # Errors
/// An error will be retuned if the object store couldn't retrieve data or the Parquet file was invalid.
async fn load_metadata(
    cache: Arc<dyn FileMetadataCache>,
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<bool, DataFusionError> {
    Ok(if !cache.contains_key(&meta) {
        debug!("Pre-fetching {} to metadata cache", meta.location);
        let metadata = ParquetMetaDataReader::new()
            .with_prefetch_hint(metadata_size_hint)
            .with_page_index_policy(PageIndexPolicy::Skip)
            .load_and_finish(ObjectStoreFetch::new(&store, &meta), meta.size)
            .await?;

        cache.put(
            &meta,
            Arc::new(CachedParquetMetaData::new(Arc::new(metadata))),
        );
        true
    } else {
        false
    })
}

struct UrlWrap<'a>(&'a Url);

impl<'a> AsRef<Url> for UrlWrap<'a> {
    fn as_ref(&self) -> &Url {
        self.0
    }
}

/// Prepopulates the cache with Parquet metadata without page indexes.
///
/// # Errors
/// Errors can occur due to failures in communicating with the object store or decoding Parquet metadata.
pub async fn prepopulate_metadata_cache(
    ctx: &SessionContext,
    input_files: &[Url],
    object_metas: &[ObjectMeta],
    metadata_size_hint: Option<usize>,
) -> Result<(), DataFusionError> {
    let file_metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();

    let mut handles = Vec::new();
    for (meta, url) in object_metas.iter().zip(input_files) {
        let store = ctx.runtime_env().object_store(UrlWrap(url))?;
        handles.push(tokio::spawn(load_metadata(
            file_metadata_cache.clone(),
            store,
            meta.clone(),
            metadata_size_hint,
        )));
    }
    for handle in handles {
        handle
            .await
            .map_err(|e| DataFusionError::External(e.into()))?
            .map_err(|e| DataFusionError::External(e.into()))?;
    }
    Ok(())
}
