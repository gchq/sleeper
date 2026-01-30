//! An [`ObjectStore`] implementation that uses [`ObjectStore`]'s implementations for most calls
//! but delegates to a custom GET implementation. This allows it to implement a readahead mechanism
//! to reduce the number of GET calls.
//!
//! When a ranged get request is made, [`ReadaheadStore`] will convert the [`GetRange::Bounded`] range to a
//! [`GetRange::Offset`] range when the request is made to the underlying store so that we can read beyond the
//! original range requested. A [`PositionedStream`] is returned which honours the original bounded range. The returned
//! stream has a link to a [`CacheInserter`] object such that when the client has finished with the stream and drops it,
//! the cache inserter will place the underlying data stream in [`ReadaheadStore`]'s cache. When a client makes a
//! subsequent get request on the same object, we check the cache to see if we already have a cached open stream that is
//! positioned close to where the new request starts. If the most closest positioned cached stream is within the readahead
//! limit of the new request, we remove that stream from the cache, skip the requisite number of bytes (and discard
//! them) to arrive at the new desired position and then return a new [`PositionedStream`] to the client.
//!
//! We don't attempt (see [`should_skip_readahead`]) to cache if:
//! * the underlying object resides on the local file system
//! * if no range was specified
//! * if a [`GetRange::Suffix`] range is requested
//! * the request was only for the object's header data.
//!
//!
//! # Example
//! Assume the object `test_file` is 100 bytes long. This store may operate as follows.
//!
//! 1. Client makes a request via [`ObjectStore::get_opts`] or [`ObjectStore::get_range`] for byte range 20-30 of
//!    `test_file`.
//!     1. [`ReadaheadStore`] will make a request to the underlying store for byte range 20-...
//!     2. A [`PositionedStream`] is returned to the client for range 20-30.
//! 2. Client reads some data then drops the stream at position 28.
//!     1. The [`CacheInserter`] caches the stream at position 28.
//! 3. Some time later...
//! 4. Client makes a request for `test_file` byte range 35-45.
//!     1. [`ReadaheadStore`] checks the cache entries for `test_file` and finds an entry with a stream at position 28.
//!        This is within the readahead limit, so the stream is removed from the cache.
//!     2. The stream is skipped (35 - 28) = 7 bytes ahead.
//!     3. A [`PositionedStream`] is returned to the client for range 35-45.
//!     4. *We have avoided making a second GET request on the underlying [`ObjectStore`]!*
//! 5. Client uses and drops stream and it will be returned to the cache.
//! 6. Some more time passes...
//! 7. Client makes a request for `test_file` for the whole file.
//!     1. As the request matches one of the trigger conditions above, [`ReadaheadStore`] simply passes the request
//!        through to the underlying store.
//!
/*
 * Copyright 2022-2025 Crown Copyright
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
mod stream;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, empty};
use log::debug;
use log::info;
use num_format::{Locale, ToFormattedString};
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    path::Path,
};
use std::{
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Display},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use stream::PositionedStream;
use thiserror::Error;

/// Error type for [`ReadaheadStore`].
#[derive(Error, Debug, Clone, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub enum ReadaheadError {
    #[error("position {index} is out of bounds, total size {size}")]
    RangeOutOfBounds { index: u64, size: u64 },
    #[error("underlying stream terminated earlier than expected")]
    StreamTerminatedEarly,
}

/// Default amount in bytes to allow a stream to be readahead before the
/// stream will be closed and re-opened.
pub const DEFAULT_READAHEAD: u64 = 2u64.pow(16);
/// Maximum number of cached streams per location before being purged.
pub const DEFAULT_MAX_STREAM_PER_LOCATION: usize = 2;
/// Maximum age of a stream before being purged.
pub const DEFAULT_MAX_STREAM_AGE: Duration = Duration::from_secs(10);

/// Simple container for storing a byte stream and its location.
struct Container {
    /// Byte stream reader for more data.
    pub inner: BoxStream<'static, Result<Bytes>>,
    /// Absolute position within object.
    pub pos: u64,
    /// Time of last use.
    pub last_use: Instant,
}

impl Debug for Container {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Container")
            .field("inner", &"<<< stream of Result<Bytes> >>>")
            .field("pos", &self.pos)
            .field("last_use", &self.last_use)
            .finish()
    }
}

/// Simple stash for [`ObjectStore`] metadata.
#[derive(Debug)]
struct CacheObject {
    meta: ObjectMeta,
    attrs: Attributes,
    /// A cache of partially used streams, sorted by absolute stream position.
    streams: BTreeMap<u64, Container>,
}

/// Cache inserter struct to allow insertion logic to exist independently of the store
/// itself. We store a pointer to the cache as a weak pointer so isolate its lifetime
/// from the owning [`ReadaheadStore`].
struct CacheInserter {
    /// Weak pointer to the cache.
    pub cache_ptr: Weak<Mutex<HashMap<Path, CacheObject>>>,
    /// The location to insert a stream to once it has been dropped.
    pub location: Path,
}

impl CacheInserter {
    /// This should be passed as a closure to instances of [`PositionedStream`]
    /// so that once the stream is dropped, this function is called.
    /// This will insert the remaining stream into the cache if the pointer
    /// is still valid.
    fn cache_used_stream(&self, s: &mut PositionedStream) {
        // Check if cache still exists (has our owning object store been destroyed?)
        if let Some(cache) = self.cache_ptr.upgrade() {
            let mut cache_guard = cache.lock().expect("ReadaheadStore cache lock poisoned");
            if let Some(cache_ob) = cache_guard.get_mut(&self.location) {
                cache_ob.streams.insert(
                    s.pos(),
                    Container {
                        inner: std::mem::replace(s.inner(), Box::pin(empty())),
                        pos: s.pos(),
                        last_use: Instant::now(),
                    },
                );
            }
        }
    }
}

/// An wrapper for `object_store`'s [`ObjectStore`] implementation that passes
/// most requests straight down to the original implementation except for GET requests.
///
/// This enables us to implement readahead on the returned stream. Then if subsequent GET requests are for ranges
/// within the readahead limit of the last request, we don't need to issue a new request to the underlying store,
/// we can continue reading from the existing stream.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ReadaheadStore<T: ObjectStore> {
    /// Underlying `object_store` implementation.
    inner: T,
    /// Path prefix for logging purposes
    path_prefix: String,
    /// Cache for object meta-data and streams.
    cache: Arc<Mutex<HashMap<Path, CacheObject>>>,
    /// The amount of readahead we will tolerate before using a new stream.
    max_readahead: u64,
    /// Maximum age of a cached stream.
    max_age: Duration,
    /// Maximum number of live streams per object.
    max_live_streams: usize,
    /// Number of underlying GET requests
    underlying_gets: Arc<Mutex<usize>>,
}

impl<T: ObjectStore> ReadaheadStore<T> {
    /// Creates a new store with the default settings.
    #[must_use]
    pub fn new(inner: T, path_prefix: impl Into<String>) -> Self {
        Self {
            inner,
            path_prefix: path_prefix.into(),
            cache: Arc::new(Mutex::new(HashMap::new())),
            max_readahead: DEFAULT_READAHEAD,
            max_age: DEFAULT_MAX_STREAM_AGE,
            max_live_streams: DEFAULT_MAX_STREAM_PER_LOCATION,
            underlying_gets: Arc::new(Mutex::new(0)),
        }
    }

    /// Changes the maximum age of a cached stream before it can be evicted.
    #[must_use]
    #[allow(dead_code)]
    pub fn with_max_stream_age(mut self, max_age: Duration) -> Self {
        self.max_age = max_age;
        self
    }

    /// Changes the maximum number of live streams per location (object path)
    /// before being evicted.
    #[must_use]
    #[allow(dead_code)]
    pub fn with_max_live_streams(mut self, max_streams: usize) -> Self {
        self.max_live_streams = max_streams;
        self
    }

    /// Changes the maximum allowable readahead on a previously opened stream. If
    /// a new GET operation is requested and an previously used stream is present
    /// less than the readahead bytes behind, then it is skipped ahead to the needed
    /// position and re-used, otherwise a new stream is opened.
    #[must_use]
    #[allow(dead_code)]
    pub fn with_readahead(mut self, max_readahead: u64) -> Self {
        self.max_readahead = max_readahead;
        self
    }

    /// Find the nearest cached stream to the given point for the file.
    /// Returns copies of the cached metadata and the stream if one was available.
    /// The stream will be advanced to the required starting position from the given
    /// range.
    ///
    /// # Panics
    /// If the underlying assumptions fail. These are bug conditions.
    async fn get_cached_stream(
        &self,
        location: &Path,
        req_range: Option<&GetRange>,
    ) -> Result<Option<(ObjectMeta, Attributes, PositionedStream)>> {
        let start_pos = get_start_pos(req_range);
        // Do cache lookup on the given path
        let cached_data = match self
            .cache
            .lock()
            .expect("ReadaheadStore cache lock poisoned")
            .get_mut(location)
        {
            Some(cache_ob) => {
                // Any cache entries between 0 and this position?
                let Some(nearest_stream_pos) = cache_ob
                    .streams
                    .range(..=start_pos)
                    .next_back()
                    .map(|(pos, _)| *pos)
                else {
                    // No cache entries for this position
                    return Ok(None);
                };
                debug!(
                    "Location {}/{} cache hit found for position {} at cached position {}",
                    self.path_prefix,
                    location,
                    start_pos.to_formatted_string(&Locale::en),
                    nearest_stream_pos.to_formatted_string(&Locale::en)
                );

                let distance = start_pos.checked_sub(nearest_stream_pos).expect(
                    "BUG: Nearest stream pos should never be greater than desired position",
                );
                // Is this within the readahead limit?
                if distance <= self.max_readahead {
                    Some((cache_ob.meta.clone(),cache_ob.attrs.clone(), cache_ob.streams.remove(&nearest_stream_pos).expect(
                        "BUG: Couldn't remove cached stream, even though it was previously present",
                    )))
                } else {
                    debug!(
                        "Required readahead of {} exceeds limit of {}, cache hit rejected",
                        distance.to_formatted_string(&Locale::en),
                        self.max_readahead.to_formatted_string(&Locale::en)
                    );
                    // Readahead too far
                    None
                }
            }
            None => None,
        };

        match cached_data {
            Some((meta, attrs, container)) => {
                if start_pos >= meta.size {
                    return Err(object_store::Error::Generic {
                        store: "ReadaheadStore",
                        source: Box::new(ReadaheadError::RangeOutOfBounds {
                            index: start_pos,
                            size: meta.size,
                        }),
                    });
                }

                let inserter = CacheInserter {
                    cache_ptr: Arc::downgrade(&self.cache),
                    location: location.clone(),
                };
                let mut stream = PositionedStream::new_with_disposal(
                    container.inner,
                    container.pos,
                    get_stop_pos(req_range, meta.size),
                    Some(move |s: &mut PositionedStream| inserter.cache_used_stream(s)),
                );
                // Move stream to desired position
                if stream.skip_to(start_pos).await?.is_none() {
                    return Err(object_store::Error::Generic {
                        store: "ReadaheadStore",
                        source: Box::new(ReadaheadError::StreamTerminatedEarly),
                    });
                }

                Ok(Some((meta, attrs, stream)))
            }
            None => Ok(None),
        }
    }

    /// Make a GET request to the underlying store and cache request and state once
    /// the stream is no longer used (gets dropped).
    async fn make_get_request(
        &self,
        location: &Path,
        mut options: GetOptions,
    ) -> Result<GetResult> {
        let original_range = options.range.clone();
        options.range = extend_range(options.range.as_ref());
        let response = self.inner.get_opts(location, options).await?;
        let GetResult {
            meta,
            payload,
            range,
            attributes,
        } = response;

        *self
            .underlying_gets
            .lock()
            .expect("ReadaheadStore lock poisoned") += 1;
        debug!(
            "ReadaheadStore GET request to {}/{location}",
            self.path_prefix
        );

        let stop_pos = get_stop_pos(original_range.as_ref(), meta.size);
        let payload = match payload {
            GetResultPayload::Stream(stream) => {
                // Retrieve data from cache or insert it if needed
                let mut cache = self.cache.lock().unwrap();
                cache
                    .entry(location.to_owned())
                    .or_insert_with(|| CacheObject {
                        meta: meta.clone(),
                        attrs: attributes.clone(),
                        streams: BTreeMap::new(),
                    });

                let inserter = CacheInserter {
                    cache_ptr: Arc::downgrade(&self.cache),
                    location: location.clone(),
                };
                object_store::GetResultPayload::Stream(Box::pin(
                    PositionedStream::new_with_disposal(
                        stream,
                        range.start,
                        stop_pos,
                        Some(move |s: &mut PositionedStream| inserter.cache_used_stream(s)),
                    ),
                ))
            }
            // If it's a local file, then don't change it
            GetResultPayload::File(_, _) => payload,
        };

        Ok(GetResult {
            payload,
            range: range.start..stop_pos,
            meta,
            attributes,
        })
    }

    /// Purge all cache entries that are too old or too numerous across all files.
    /// A lock will be obtained for the cache object and all streams that are too old
    /// will be purged as well as the lowest position streams if there are too many.
    ///
    /// Returns the total number of live streams still in the cache.
    ///
    /// # Panics
    /// If the mutex lock that guards the cache has become poisoned.
    pub fn clean_cache(&self) -> usize {
        let now = Instant::now();
        debug!(
            "Purging cache of streams older than {}s or when more than {} streams per location",
            self.max_age.as_secs().to_formatted_string(&Locale::en),
            self.max_live_streams.to_formatted_string(&Locale::en)
        );
        let mut cache = self
            .cache
            .lock()
            .expect("ReadaheadStore cache lock poisoned");
        let mut total_live_streams = 0;
        for (_, cache_ob) in cache.iter_mut() {
            // Evict all which are too old
            cache_ob
                .streams
                .retain(|_, c| now.duration_since(c.last_use) < self.max_age);
            // Max size evictions. Evict "earliest" in file first
            while cache_ob.streams.len() > self.max_live_streams {
                cache_ob.streams.pop_first();
            }
            total_live_streams += cache_ob.streams.len();
        }
        debug!(
            "Cache contains live streams {} across {} files",
            total_live_streams.to_formatted_string(&Locale::en),
            cache.len().to_formatted_string(&Locale::en),
        );
        total_live_streams
    }

    /// Empty the cache of previous streams.
    ///
    /// # Panics
    /// If the mutex lock that guards the cache has become poisoned.
    #[allow(dead_code)]
    pub fn purge_cache(&self) {
        self.cache
            .lock()
            .expect("ReadaheadStore cache lock poisoned")
            .clear();
    }
}

impl<T: ObjectStore> Drop for ReadaheadStore<T> {
    fn drop(&mut self) {
        info!(
            "ReadaheadStore made {} GET requests to underlying location {}",
            self.underlying_gets
                .lock()
                .expect("ReadaheadStore lock poisoned")
                .to_formatted_string(&Locale::en),
            self.path_prefix,
        );
    }
}

/// Find the end position (exclusive) given the range and object size
#[must_use]
pub fn get_stop_pos(range: Option<&GetRange>, total_size: u64) -> u64 {
    match range {
        Some(GetRange::Bounded(r)) => std::cmp::min(r.end, total_size),
        Some(_) | None => total_size,
    }
}

/// Convert a range into an extended one. That is a bounded range is
/// turned into a half open range. Suffix ranges are not yet supported.
///
/// # Panics
/// If a suffix range is supplied.
#[must_use]
pub fn extend_range(range: Option<&GetRange>) -> Option<GetRange> {
    range.map(|r| match r {
        GetRange::Bounded(r) => GetRange::Offset(r.start),
        x @ GetRange::Offset(_) => x.clone(),
        GetRange::Suffix(_) => {
            unimplemented!("Attempting to extend suffix ranges is not possible yet")
        }
    })
}

/// Find the starting position (inclusive) given the range of the object
/// request.
///
/// # Panics
/// If the range is a [`GetRange::Suffix`] as this is not implemented.
#[must_use]
pub fn get_start_pos(range: Option<&GetRange>) -> u64 {
    match range {
        Some(GetRange::Bounded(r)) => r.start,
        Some(GetRange::Offset(o)) => *o,
        Some(GetRange::Suffix(_)) | None => {
            unimplemented!("Start position for suffix ranges or None isn't supported yet")
        }
    }
}

/// Returns true if a particular get request should avoid trying to
/// use the cache of already open streams.
#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn should_skip_readahead(options: &GetOptions) -> bool {
    options.head || options.range.is_none() || matches!(options.range, Some(GetRange::Suffix(_)))
}

impl<T: ObjectStore> Display for ReadaheadStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReadaheadStore(inner: {}, path_prefix: {})",
            self.inner, self.path_prefix
        )
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for ReadaheadStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // If this an options head or full request (no range) or a suffix range, then pass it through, don't try
        // to do anything with it
        if should_skip_readahead(&options) {
            return self.inner.get_opts(location, options).await;
        }

        let start_pos = get_start_pos(options.range.as_ref());
        let cached = self
            .get_cached_stream(location, options.range.as_ref())
            .await?;

        // Clean cache here so that we don't expire something we're just about to use, even if it is about to expire
        self.clean_cache();

        // If cache hit
        match cached {
            Some((meta, attributes, stream)) => {
                let stop_pos = get_stop_pos(options.range.as_ref(), meta.size);
                Ok(GetResult {
                    payload: object_store::GetResultPayload::Stream(Box::pin(stream)),
                    range: start_pos..stop_pos,
                    meta,
                    attributes,
                })
            }
            None => {
                // Make request and cache stream
                self.make_get_request(location, options).await
            }
        }
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use crate::store::LoggingObjectStore;

    use super::*;
    use futures::{StreamExt, stream};
    use object_store::{integration::*, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    pub(super) fn into_stream<'a, I: IntoIterator<Item = &'static str, IntoIter: Send + 'a>>(
        items: I,
    ) -> BoxStream<'a, Result<Bytes>> {
        let s = stream::iter(items.into_iter().map(|s| Ok(Bytes::from(s))));
        Box::pin(s)
    }

    pub(super) fn test_stream<'a>() -> BoxStream<'a, Result<Bytes>> {
        into_stream(vec!["12345", "6789", "012", "34567890"])
    }

    pub(super) async fn assert_stream_eq<'a, T: Eq + Debug>(
        mut lhs: BoxStream<'a, Result<T>>,
        mut rhs: BoxStream<'a, Result<T>>,
    ) {
        loop {
            match (lhs.next().await, rhs.next().await) {
                (Some(Ok(lhs_val)), Some(Ok(rhs_val))) if lhs_val != rhs_val => {
                    panic!("stream lhs {lhs_val:?} rhs {rhs_val:?} not equal");
                }
                (Some(_), None) => {
                    panic!("stream rhs ended early");
                }
                (None, Some(_)) => {
                    panic!("stream lhs ended early");
                }
                (Some(_), Some(_)) => {}
                (None, None) => break,
            }
        }
    }

    fn make_store() -> ReadaheadStore<InMemory> {
        let inner = InMemory::new();
        ReadaheadStore::new(inner, "memory:/")
    }

    fn make_cache_map() -> BTreeMap<u64, Container> {
        let mut map = BTreeMap::new();
        map.insert(
            5,
            Container {
                inner: test_stream(),
                pos: 5,
                last_use: Instant::now(),
            },
        );
        map.insert(
            10,
            Container {
                inner: test_stream(),
                pos: 10,
                last_use: Instant::now(),
            },
        );
        map.insert(
            18,
            Container {
                inner: test_stream(),
                pos: 18,
                last_use: Instant::now(),
            },
        );
        map
    }

    #[tokio::test]
    async fn readahead_store_test() {
        let integration = make_store();

        put_get_delete_list(&integration).await;
        get_opts(&integration).await;
        list_uses_directories_correctly(&integration).await;
        list_with_delimiter(&integration).await;
        rename_and_copy(&integration).await;
        copy_if_not_exists(&integration).await;
        stream_get(&integration).await;
        put_opts(&integration, true).await;
        put_get_attributes(&integration).await;
    }

    #[test]
    fn stop_pos_offset() {
        // Given
        let range = GetRange::Offset(1);

        // Then
        assert_eq!(get_stop_pos(Some(&range), 10), 10);
    }

    #[test]
    fn stop_pos_suffix() {
        // Given
        let range = GetRange::Suffix(1);

        // Then
        assert_eq!(get_stop_pos(Some(&range), 10), 10);
    }

    #[test]
    fn stop_pos_none() {
        // Given

        // Then
        assert_eq!(get_stop_pos(None, 10), 10);
    }

    #[test]
    fn stop_pos_bounded_in_range() {
        // Given
        let range = GetRange::Bounded(0..5);

        // Then
        assert_eq!(get_stop_pos(Some(&range), 10), 5);
    }

    #[test]
    fn stop_pos_bounded_out_of_range() {
        // Given
        let range = GetRange::Bounded(0..15);

        // Then
        assert_eq!(get_stop_pos(Some(&range), 10), 10);
    }

    #[test]
    fn extend_range_bounded() {
        // Given
        let range = GetRange::Bounded(1..10);

        // Then
        assert_eq!(extend_range(Some(&range)), Some(GetRange::Offset(1)));
    }

    #[test]
    fn extend_range_offset() {
        // Given
        let range = GetRange::Offset(5);

        // Then
        assert_eq!(extend_range(Some(&range)), Some(GetRange::Offset(5)));
    }

    #[test]
    #[should_panic(expected = "Attempting to extend suffix ranges is not possible yet")]
    fn should_panic_on_extend_suffix_range() {
        // Given
        let range = GetRange::Suffix(3);

        // When
        let _ = extend_range(Some(&range));

        // Then - panic
    }

    #[test]
    fn extend_range_none() {
        // Given

        // Then
        assert_eq!(extend_range(None), None);
    }

    #[test]
    fn start_pos_bounded() {
        // Given
        let range = GetRange::Bounded(1..10);

        // Then
        assert_eq!(get_start_pos(Some(&range)), 1);
    }

    #[test]
    fn start_pos_offset() {
        // Given
        let range = GetRange::Offset(5);

        // Then
        assert_eq!(get_start_pos(Some(&range)), 5);
    }

    #[test]
    #[should_panic(expected = "Start position for suffix ranges or None isn't supported yet")]
    fn should_panic_on_start_pos_suffix_range() {
        // Given
        let range = GetRange::Suffix(3);

        // When
        let _ = get_start_pos(Some(&range));

        // Then - panic
    }

    #[test]
    #[should_panic(expected = "Start position for suffix ranges or None isn't supported yet")]
    fn should_panic_on_start_pos_none() {
        // Given

        // When
        let _ = get_start_pos(None);

        // Then - panic
    }

    #[test]
    fn read_correct_fields() {
        // Given
        let store = make_store()
            .with_max_live_streams(5)
            .with_max_stream_age(Duration::from_secs(32))
            .with_readahead(12345);

        // Then
        assert_eq!(store.max_age, Duration::from_secs(32));
        assert_eq!(store.max_live_streams, 5);
        assert_eq!(store.max_readahead, 12345);
    }

    #[tokio::test]
    async fn no_location_cache() {
        // Given
        let ps = make_store();

        // Then
        assert!(matches!(
            ps.get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(100..200)))
                .await,
            Ok(None)
        ));
    }

    #[tokio::test]
    #[should_panic(expected = "Start position for suffix ranges or None isn't supported yet")]
    async fn panic_on_no_range_for_cached() {
        // Given
        let ps = make_store();

        // When
        let _ = ps.get_cached_stream(&"test_file".into(), None).await;

        // Then - panic
    }

    fn test_meta() -> ObjectMeta {
        ObjectMeta {
            e_tag: Some("test-e-tag".into()),
            last_modified: chrono::DateTime::from_timestamp(1000, 0).unwrap(),
            location: "test_file".into(),
            size: 20,
            version: Some("1".into()),
        }
    }

    fn test_attributes() -> Attributes {
        let mut attrs = Attributes::default();
        attrs.insert(
            object_store::Attribute::ContentEncoding,
            "text/plain".into(),
        );
        attrs.insert(object_store::Attribute::ContentLanguage, "en-us".into());
        attrs
    }

    #[tokio::test]
    async fn location_no_entries_is_none() {
        // Given
        let ps = make_store();

        // When
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: BTreeMap::new(),
            },
        );

        // Then
        assert!(matches!(
            ps.get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(100..200)))
                .await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn ask_for_stream_before_any_cached_streams() {
        // Given
        let ps = make_store();

        // When
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: make_cache_map(),
            },
        );

        // Then
        assert!(matches!(
            ps.get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(4..20)))
                .await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn ask_for_stream_at_exact_position() -> std::result::Result<(), &'static str> {
        // Given
        let ps = make_store();

        // When
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: make_cache_map(),
            },
        );

        let Some((meta, attrs, stream)) = ps
            .get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(5..6)))
            .await
            .map_err(|_| "error getting stream")?
        else {
            return Err("invalid cache response");
        };

        // Then
        assert_eq!(meta, test_meta());
        assert_eq!(attrs, test_attributes());
        assert_eq!(stream.pos(), 5);
        assert_stream_eq(Box::pin(stream), into_stream(vec!["1"])).await;

        Ok(())
    }

    #[tokio::test]
    async fn no_stream_when_larger_than_readahead() -> std::result::Result<(), &'static str> {
        // Given
        let ps = make_store().with_readahead(0);

        // When
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: make_cache_map(),
            },
        );

        // Then
        assert!(matches!(
            ps.get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(6..7)))
                .await,
            Ok(None)
        ));

        Ok(())
    }

    #[tokio::test]
    async fn stream_is_same_as_inserted() -> std::result::Result<(), &'static str> {
        // Given
        let ps = make_store();

        // When
        let mut map = make_cache_map();
        map.insert(
            0,
            Container {
                inner: test_stream(),
                pos: 0,
                last_use: Instant::now(),
            },
        );
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: map,
            },
        );

        let Some((meta, attrs, stream)) = ps
            .get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(0..20)))
            .await
            .map_err(|_| "error getting stream")?
        else {
            return Err("invalid cache response");
        };

        // Then
        assert_eq!(meta, test_meta());
        assert_eq!(attrs, test_attributes());
        assert_eq!(stream.pos(), 0);
        assert_stream_eq(Box::pin(stream), test_stream()).await;

        Ok(())
    }

    #[tokio::test]
    async fn stream_is_removed_from_cache() -> std::result::Result<(), &'static str> {
        // Given
        let ps = make_store();

        // When
        let map = make_cache_map();
        let cache_count = map.len();
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: map,
            },
        );

        let Some((_, _, _stream)) = ps
            .get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(5..7)))
            .await
            .map_err(|_| "error getting stream")?
        else {
            return Err("invalid cache response");
        };

        // Then
        let new_count = ps
            .cache
            .lock()
            .unwrap()
            .get(&"test_file".into())
            .unwrap()
            .streams
            .len();
        assert!(cache_count > new_count);
        assert_eq!(new_count, 2);

        Ok(())
    }

    #[tokio::test]
    async fn error_when_position_out_of_range() -> std::result::Result<(), &'static str> {
        // Given
        let ps = make_store();

        // When
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: make_cache_map(),
            },
        );

        let res = ps
            .get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(40..50)))
            .await;

        // Then
        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_meta_attributes_match() -> std::result::Result<(), &'static str> {
        // Given
        let ps = make_store();

        // When
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: make_cache_map(),
            },
        );

        let Some((meta, attrs, _stream)) = ps
            .get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(5..6)))
            .await
            .map_err(|_| "error getting stream")?
        else {
            return Err("invalid cache response");
        };

        // Then
        assert_eq!(meta, test_meta());
        assert_eq!(attrs, test_attributes());
        Ok(())
    }

    #[tokio::test]
    async fn returned_stream_has_correct_start_stop_pos() -> std::result::Result<(), &'static str> {
        // Given
        let ps = make_store();

        // When
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: make_cache_map(),
            },
        );

        let Some((_, _, stream)) = ps
            .get_cached_stream(&"test_file".into(), Some(&GetRange::Bounded(10..15)))
            .await
            .map_err(|_| "error getting stream")?
        else {
            return Err("invalid cache response");
        };

        // Then
        assert_eq!(stream.pos(), 10);
        assert_eq!(stream.stop_pos(), 15);
        Ok(())
    }

    #[test]
    fn empty_cache_empties_no_error() {
        // Given
        let ps = make_store();
        ps.cache.lock().unwrap().insert(
            "test_file".into(),
            CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: make_cache_map(),
            },
        );

        // When
        ps.purge_cache();

        // Then
        assert!(ps.cache.lock().unwrap().is_empty());
    }

    #[test]
    fn non_empty_cache_empties() {
        // Given
        let ps = make_store();

        // When
        ps.purge_cache();

        // Then
        assert!(ps.cache.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_count_is_zero() -> Result<()> {
        // Given
        let ps = make_store();
        ps.put(&"test_file".into(), "some data".into()).await?;

        // When - no op

        // Then
        assert_eq!(*ps.underlying_gets.lock().unwrap(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn make_get_requests_increments_get_count() -> Result<()> {
        // Given
        let ps = make_store();
        ps.put(&"test_file".into(), "some data".into()).await?;

        // When
        assert_eq!(*ps.underlying_gets.lock().unwrap(), 0);
        let _ = ps
            .make_get_request(&"test_file".into(), GetOptions::default())
            .await?;

        // Then
        assert_eq!(*ps.underlying_gets.lock().unwrap(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn make_get_request_doesnt_alter_local_file() -> Result<()> {
        // Given
        let temp = TempDir::new().unwrap();
        let inner = LocalFileSystem::new_with_prefix(temp.path()).unwrap();
        let ps = ReadaheadStore::new(inner, "file:/");
        ps.put(&"test_file".into(), "some data".into()).await?;

        // When
        let result = ps
            .make_get_request(&"test_file".into(), GetOptions::default())
            .await?;

        // Then
        assert!(matches!(result.payload, GetResultPayload::File(_, _)));
        Ok(())
    }

    #[tokio::test]
    async fn make_get_request_makes_inner_requset() -> Result<()> {
        // Given
        let temp = TempDir::new().unwrap();
        let inner = LoggingObjectStore::new(
            LocalFileSystem::new_with_prefix(temp.path()).unwrap(),
            "TEST",
            "file:/",
        );
        let ps = ReadaheadStore::new(inner, "file:/");
        ps.put(&"test_file".into(), "some data".into()).await?;

        // When
        let _ = ps
            .make_get_request(&"test_file".into(), GetOptions::default())
            .await?;

        // Then
        assert_eq!(ps.inner.get_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn make_get_request_correct_payload_and_range_bounded() -> Result<()> {
        // Given
        let temp = TempDir::new().unwrap();
        let inner = LocalFileSystem::new_with_prefix(temp.path()).unwrap();
        let ps = ReadaheadStore::new(inner, "file:/");
        ps.put(&"test_file".into(), "some data".into()).await?;

        // When
        let result = ps
            .make_get_request(
                &"test_file".into(),
                GetOptions {
                    range: Some(GetRange::Bounded(1..3)),
                    ..GetOptions::default()
                },
            )
            .await?;

        // Then
        assert_eq!(result.range, 1..3);
        assert_eq!(result.meta.size, 9);
        assert_stream_eq(result.into_stream(), into_stream(vec!["om"])).await;

        Ok(())
    }

    #[tokio::test]
    async fn make_get_request_correct_payload_and_range_offset() -> Result<()> {
        // Given
        let temp = TempDir::new().unwrap();
        let inner = LocalFileSystem::new_with_prefix(temp.path()).unwrap();
        let ps = ReadaheadStore::new(inner, "file:/");
        ps.put(&"test_file".into(), "some data".into()).await?;

        // When
        let result = ps
            .make_get_request(
                &"test_file".into(),
                GetOptions {
                    range: Some(GetRange::Offset(2)),
                    ..GetOptions::default()
                },
            )
            .await?;

        // Then
        assert_eq!(result.range, 2..9);
        assert_eq!(result.meta.size, 9);
        assert_stream_eq(result.into_stream(), into_stream(vec!["me data"])).await;

        Ok(())
    }

    #[tokio::test]
    async fn make_get_request_insert_to_cache() -> Result<()> {
        // Given
        let ps = make_store();
        ps.put(&"test_file".into(), "some data".into()).await?;

        // When
        let result = ps
            .make_get_request(
                &"test_file".into(),
                GetOptions {
                    range: Some(GetRange::Bounded(4..7)),
                    ..GetOptions::default()
                },
            )
            .await?;
        let _ = result.into_stream().collect::<Vec<_>>().await;

        // Then
        assert_eq!(ps.cache.lock().unwrap().len(), 1);
        assert_eq!(
            ps.cache
                .lock()
                .unwrap()
                .get(&"test_file".into())
                .unwrap()
                .streams
                .len(),
            1
        );
        assert_eq!(
            ps.cache
                .lock()
                .unwrap()
                .get(&"test_file".into())
                .unwrap()
                .streams
                .get(&7)
                .unwrap()
                .pos,
            7
        );

        Ok(())
    }

    fn test_cache_purge<T: Into<Path>, I: IntoIterator<Item = (T, u64, Instant)>>(
        live_streams: usize,
        max_age: Duration,
        expected: usize,
        items: I,
    ) {
        // Given
        let ps = make_store()
            .with_max_live_streams(live_streams)
            .with_max_stream_age(max_age);
        let mut cache = ps.cache.lock().unwrap();
        for (path, pos, time) in items {
            let k = cache.entry(path.into()).or_insert_with(|| CacheObject {
                meta: test_meta(),
                attrs: test_attributes(),
                streams: BTreeMap::new(),
            });
            k.streams.insert(
                pos,
                Container {
                    inner: Box::pin(stream::empty()),
                    pos,
                    last_use: time,
                },
            );
        }
        drop(cache);
        // When
        let size = ps.clean_cache();
        // Then
        assert_eq!(size, expected, "Incorrect number of items left in cache");
    }

    #[tokio::test]
    async fn purge_empty_cache() {
        test_cache_purge::<&str, Vec<(&str, u64, std::time::Instant)>>(
            8,
            Duration::from_secs(10),
            0,
            vec![],
        );
    }

    #[tokio::test]
    async fn purge_all_when_zero_live_zero_duration() {
        test_cache_purge(
            0,
            Duration::from_nanos(0),
            0,
            vec![
                ("test_file", 12, Instant::now()),
                ("test_file2", 34, Instant::now()),
            ],
        );
    }

    #[tokio::test]
    async fn purge_all_when_zero_duration() {
        test_cache_purge(
            9999,
            Duration::from_nanos(0),
            0,
            vec![
                ("test_file", 12, Instant::now()),
                ("test_file2", 34, Instant::now()),
            ],
        );
    }

    #[tokio::test]
    async fn purge_all_when_zero_live() {
        test_cache_purge(
            0,
            Duration::from_secs(9999),
            0,
            vec![
                ("test_file", 12, Instant::now()),
                ("test_file", 56, Instant::now()),
                ("test_file2", 34, Instant::now()),
            ],
        );
    }

    #[tokio::test]
    async fn purge_some_when_two_live() {
        test_cache_purge(
            2,
            Duration::from_secs(9999),
            4,
            vec![
                ("test_file", 12, Instant::now()),
                ("test_file", 56, Instant::now()),
                ("test_file", 34, Instant::now()),
                ("test_file2", 34, Instant::now()),
                ("test_file2", 89, Instant::now()),
                ("test_file2", 105, Instant::now()),
            ],
        );
    }

    #[tokio::test]
    async fn purge_some_when_short_duration() {
        test_cache_purge(
            99999,
            Duration::from_secs(60),
            3,
            vec![
                (
                    "test_file",
                    12,
                    Instant::now().checked_sub(Duration::from_secs(59)).unwrap(),
                ),
                (
                    "test_file",
                    56,
                    Instant::now().checked_sub(Duration::from_secs(61)).unwrap(),
                ),
                (
                    "test_file",
                    34,
                    Instant::now()
                        .checked_sub(Duration::from_secs(120))
                        .unwrap(),
                ),
                (
                    "test_file2",
                    34,
                    Instant::now().checked_sub(Duration::from_secs(25)).unwrap(),
                ),
                (
                    "test_file2",
                    89,
                    Instant::now().checked_sub(Duration::from_secs(30)).unwrap(),
                ),
                (
                    "test_file2",
                    105,
                    Instant::now()
                        .checked_sub(Duration::from_secs(999))
                        .unwrap(),
                ),
            ],
        );
    }

    #[tokio::test]
    async fn purge_some_when_short_duration_and_two_live() {
        test_cache_purge(
            2,
            Duration::from_secs(60),
            2,
            vec![
                (
                    "test_file",
                    12,
                    Instant::now().checked_sub(Duration::from_secs(10)).unwrap(),
                ),
                (
                    "test_file",
                    56,
                    Instant::now().checked_sub(Duration::from_secs(10)).unwrap(),
                ),
                (
                    "test_file",
                    45,
                    Instant::now().checked_sub(Duration::from_secs(10)).unwrap(),
                ),
                (
                    "test_file",
                    34,
                    Instant::now()
                        .checked_sub(Duration::from_secs(120))
                        .unwrap(),
                ),
                (
                    "test_file",
                    98,
                    Instant::now().checked_sub(Duration::from_secs(65)).unwrap(),
                ),
                (
                    "test_file",
                    100,
                    Instant::now()
                        .checked_sub(Duration::from_secs(200))
                        .unwrap(),
                ),
            ],
        );
    }

    #[test]
    fn should_skip_readhead_on_head() {
        assert!(should_skip_readahead(&GetOptions {
            head: true,
            ..GetOptions::default()
        }));
    }

    #[test]
    fn should_skip_readhead_on_suffix() {
        assert!(should_skip_readahead(&GetOptions {
            range: Some(GetRange::Suffix(0)),
            ..GetOptions::default()
        }));
    }

    #[test]
    fn should_skip_readhead_on_none_range() {
        assert!(should_skip_readahead(&GetOptions {
            range: None,
            ..GetOptions::default()
        }));
    }

    #[test]
    fn should_not_skip_readhead_on_offset_range() {
        assert!(!should_skip_readahead(&GetOptions {
            range: Some(GetRange::Offset(0)),
            ..GetOptions::default()
        }));
    }

    #[test]
    fn should_not_skip_readhead_on_bounded_range() {
        assert!(!should_skip_readahead(&GetOptions {
            range: Some(GetRange::Bounded(1..2)),
            ..GetOptions::default()
        }));
    }

    #[tokio::test]
    async fn get_opts_should_increment_get_count() -> Result<()> {
        // Given
        let ps = make_store();
        ps.put(&"test_file".into(), "some data".into()).await?;

        assert_eq!(*ps.underlying_gets.lock().unwrap(), 0);

        // When
        {
            let _ = ps
                .get_opts(
                    &"test_file".into(),
                    GetOptions {
                        range: Some(GetRange::Bounded(1..2)),
                        ..GetOptions::default()
                    },
                )
                .await?;
        }

        // Then - should have incremented underlying GET count
        assert_eq!(*ps.underlying_gets.lock().unwrap(), 1);

        // When - make cached request
        {
            let _ = ps
                .get_opts(
                    &"test_file".into(),
                    GetOptions {
                        range: Some(GetRange::Bounded(3..5)),
                        ..GetOptions::default()
                    },
                )
                .await?;
        }
        // Then - cached request should NOT have incremented GET count
        assert_eq!(*ps.underlying_gets.lock().unwrap(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn get_opts_should_clean_cache() -> Result<()> {
        // Given
        let ps = make_store()
            .with_max_live_streams(1)
            .with_max_stream_age(Duration::from_secs(10));
        // Insert an old entry into cache and a recent entry into cache
        {
            let mut cache = ps.cache.lock().unwrap();
            let k = cache
                .entry("test_file".into())
                .or_insert_with(|| CacheObject {
                    meta: test_meta(),
                    attrs: test_attributes(),
                    streams: BTreeMap::new(),
                });
            k.streams.insert(
                // should be evicted
                0,
                Container {
                    inner: Box::pin(stream::empty()),
                    pos: 0,
                    last_use: Instant::now(),
                },
            );
            k.streams.insert(
                // should remain
                1,
                Container {
                    inner: Box::pin(stream::empty()),
                    pos: 1,
                    last_use: Instant::now(),
                },
            );
            k.streams.insert(
                // should be evicted
                2,
                Container {
                    inner: Box::pin(stream::empty()),
                    pos: 1,
                    last_use: Instant::now().checked_sub(Duration::from_secs(60)).unwrap(),
                },
            );
        }

        ps.put(&"test_file2".into(), "some data".into()).await?;

        // When
        ps.get_range(&"test_file2".into(), 1..2).await?;

        // Then
        // test_file cache should have 1 entry (2 removed)
        let cache = ps.cache.lock().unwrap();
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&"test_file".into()).unwrap().streams.len(), 1);
        assert_eq!(cache.get(&"test_file2".into()).unwrap().streams.len(), 1);
        assert_eq!(
            *cache
                .get(&"test_file".into())
                .unwrap()
                .streams
                .first_key_value()
                .unwrap()
                .0,
            1
        );

        Ok(())
    }

    #[tokio::test]
    async fn get_opts_should_return_stream_correct_pos_len_content_uncached() -> Result<()> {
        // Given
        let store = make_store();
        store
            .put(&"test_file".into(), "01234567890123456789".into())
            .await?;

        // When
        let stream = store
            .get_opts(
                &"test_file".into(),
                GetOptions {
                    range: Some(GetRange::Bounded(1..6)),
                    ..GetOptions::default()
                },
            )
            .await?;

        // Then
        assert_stream_eq(stream.into_stream(), into_stream(vec!["12345"])).await;

        Ok(())
    }

    #[tokio::test]
    async fn get_opts_should_return_stream_correct_pos_len_content_readahead() -> Result<()> {
        // Given
        let store = make_store();
        store
            .put(&"test_file".into(), "01234567890123456789".into())
            .await?;

        // When
        {
            let _stream = store
                .get_opts(
                    &"test_file".into(),
                    GetOptions {
                        range: Some(GetRange::Bounded(1..6)),
                        ..GetOptions::default()
                    },
                )
                .await?;
        }

        // Then - check cache contains entry
        {
            let cache = store.cache.lock().unwrap();
            assert_eq!(cache.get(&"test_file".into()).unwrap().streams.len(), 1);
        }

        // When - get same result
        let stream = store
            .get_opts(
                &"test_file".into(),
                GetOptions {
                    range: Some(GetRange::Bounded(2..6)),
                    ..GetOptions::default()
                },
            )
            .await?;

        // Then - check cache entry
        {
            let cache = store.cache.lock().unwrap();
            assert_eq!(cache.get(&"test_file".into()).unwrap().streams.len(), 0);
        }
        assert_stream_eq(stream.into_stream(), into_stream(vec!["2345"])).await;

        Ok(())
    }

    #[tokio::test]
    async fn get_opts_caches_streams_correctly() -> Result<()> {
        // Given
        let store = make_store();
        store
            .put(&"test_file".into(), "01234567890123456789".into())
            .await?;

        // Get and drop 1st stream -> will be uncached, inserted to cache on drop
        // Get and drop 2nd earlier position stream -> should be uncached, inserted to cache on drop
        // Get 3rd stream from same pos as 1st stream -> should be cached

        // When - get 1st stream
        {
            let _stream = store
                .get_opts(
                    &"test_file".into(),
                    GetOptions {
                        range: Some(GetRange::Bounded(3..10)),
                        ..GetOptions::default()
                    },
                )
                .await?;
        }

        // Then - check cache contains entry
        {
            let cache = store.cache.lock().unwrap();
            assert_eq!(cache.get(&"test_file".into()).unwrap().streams.len(), 1);
        }

        // When - get earlier position stream
        {
            let _stream = store
                .get_opts(
                    &"test_file".into(),
                    GetOptions {
                        range: Some(GetRange::Bounded(2..17)),
                        ..GetOptions::default()
                    },
                )
                .await?;
        }

        // Then - check cache contains two entries
        {
            let cache = store.cache.lock().unwrap();
            assert_eq!(cache.get(&"test_file".into()).unwrap().streams.len(), 2);
        }

        // When - get 1st stream position - don't drop it
        let _stream = store
            .get_opts(
                &"test_file".into(),
                GetOptions {
                    range: Some(GetRange::Bounded(3..15)),
                    ..GetOptions::default()
                },
            )
            .await?;

        // Then - check cache contains one entry, i.e. a cached stream was returned
        {
            let cache = store.cache.lock().unwrap();
            assert_eq!(cache.get(&"test_file".into()).unwrap().streams.len(), 1);
        }

        Ok(())
    }

    #[test]
    fn cache_inserter_does_nothing_with_none() {
        // Given
        let inserter = CacheInserter {
            cache_ptr: Weak::new(),
            location: "test".into(),
        };
        let mut ps = PositionedStream::new(Box::pin(empty()), 2, 6);

        // When
        inserter.cache_used_stream(&mut ps);

        // Then - nothing
    }

    #[tokio::test]
    // Lock is not used after await, so no compile error will occur
    #[allow(clippy::await_holding_lock)]
    async fn cache_inserter_does_insert() {
        // Given
        let mut cache = HashMap::new();
        cache.insert(
            Path::from("test"),
            CacheObject {
                attrs: test_attributes(),
                meta: test_meta(),
                streams: BTreeMap::new(),
            },
        );
        let cache_ptr = Arc::new(Mutex::new(cache));
        let inserter = CacheInserter {
            cache_ptr: Arc::downgrade(&cache_ptr),
            location: "test".into(),
        };
        let mut ps = PositionedStream::new(test_stream(), 2, 6);

        // When
        // Check cache entry empty
        assert!(
            cache_ptr
                .lock()
                .unwrap()
                .get(&"test".into())
                .unwrap()
                .streams
                .is_empty()
        );
        inserter.cache_used_stream(&mut ps);

        // Then
        // Cache should have 1 entry
        assert_eq!(
            cache_ptr
                .lock()
                .unwrap()
                .get(&"test".into())
                .unwrap()
                .streams
                .len(),
            1
        );
        // Check position and content
        let mut cache_ptr_guard = cache_ptr.lock().unwrap();
        let container = cache_ptr_guard
            .get_mut(&"test".into())
            .unwrap()
            .streams
            .get_mut(&2)
            .unwrap();

        assert_eq!(container.pos, 2);
        let stream = std::mem::replace(&mut container.inner, Box::pin(empty()));
        assert_stream_eq(stream, test_stream()).await;
    }
}
