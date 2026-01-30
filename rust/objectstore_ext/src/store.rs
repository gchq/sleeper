//! This module contains a wrapper for an [`ObjectStore`] that adds logging functionality and customised multipart upload
//! buffering size.
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
use async_trait::async_trait;
use futures::stream::BoxStream;
use log::{debug, info};
use num_format::{Locale, ToFormattedString};
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart, path::Path,
};
use std::{pin::Pin, sync::Mutex};

/// Simple struct for storing various statistics about the operation of the store.
#[derive(Debug, Default, Eq, PartialOrd, Ord, PartialEq, Clone)]
struct LoggingData {
    /// The number of GET requests logged.
    get_count: usize,
    /// The total number of bytes read across all files.
    get_bytes_read: u64,
}

/// An [`ObjectStore`] wrapper that logs some requests (e.g. HEAD, LIST, GET, PUT)
/// the underlying store makes. The number of GETs can be retrieved
/// by using the `get_count` method.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct LoggingObjectStore<T: ObjectStore> {
    store: T,
    prefix: String,
    path_prefix: String,
    internal: Mutex<LoggingData>,
}

impl<T: ObjectStore> LoggingObjectStore<T> {
    /// Create a new [`LoggingObjectStore`] by wrapping
    /// an inner store.
    #[must_use]
    pub fn new(inner: T, prefix: impl Into<String>, path_prefix: impl Into<String>) -> Self {
        Self {
            store: inner,
            prefix: prefix.into(),
            path_prefix: path_prefix.into(),
            internal: Mutex::new(LoggingData::default()),
        }
    }

    /// Gives the total number of GET requests made on this store.
    ///
    /// # Panics
    /// If we are not able to acquire the lock for the store
    /// stats.
    pub fn get_count(&self) -> usize {
        self.internal
            .lock()
            .expect("LoggingObjectStore stats lock poisoned")
            .get_count
    }

    /// Get the total number of bytes requested by this store in ranged requests.
    /// This is only a hint. The actual number of bytes read may be lower or higher.
    /// Only bytes requested via [`GetRange::Bounded`] requests are recorded. Further,
    /// we have no control over how many bytes from a request was actually read by a client,
    /// therefore this value should be treated as a hint only.
    ///
    /// # Panics
    /// If we are not able to acquire the lock for the store
    /// stats.
    pub fn get_bytes_read(&self) -> u64 {
        self.internal
            .lock()
            .expect("LoggingObjectStore stats lock poisoned")
            .get_bytes_read
    }
}

impl<T: ObjectStore> std::fmt::Display for LoggingObjectStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LoggingObjectStore \"{}\" path prefix: \"{}\" ({})",
            self.prefix, self.path_prefix, self.store
        )
    }
}

impl<T: ObjectStore> Drop for LoggingObjectStore<T> {
    fn drop(&mut self) {
        info!(
            "LoggingObjectStore \"{}\" to \"{}\" made {} GET requests and requested a total of {} bytes (range bounded)",
            self.prefix,
            self.path_prefix,
            self.get_count().to_formatted_string(&Locale::en),
            self.get_bytes_read().to_formatted_string(&Locale::en)
        );
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for LoggingObjectStore<T> {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if !options.head {
            let stats = &mut *self
                .internal
                .lock()
                .expect("LoggingObjectStore stats lock poisoned");
            stats.get_count += 1;
            match &options.range {
                Some(GetRange::Bounded(get_range)) => {
                    let len = get_range
                        .end
                        .checked_sub(get_range.start)
                        .expect("Get range length is negative");
                    stats.get_bytes_read += len;
                    debug!(
                        "{} GET request on {}/{} byte range {} to {} = {} bytes",
                        self.prefix,
                        self.path_prefix,
                        location,
                        get_range.start.to_formatted_string(&Locale::en),
                        get_range.end.to_formatted_string(&Locale::en),
                        len.to_formatted_string(&Locale::en),
                    );
                }
                Some(GetRange::Offset(start_pos)) => {
                    debug!(
                        "{} GET request on {}/{} for byte {} to EOF",
                        self.prefix,
                        self.path_prefix,
                        location,
                        start_pos.to_formatted_string(&Locale::en)
                    );
                }
                Some(GetRange::Suffix(pos)) => {
                    debug!(
                        "{} GET request on {}/{} for last {} bytes of object",
                        self.prefix,
                        self.path_prefix,
                        location,
                        pos.to_formatted_string(&Locale::en)
                    );
                }
                None => {
                    debug!(
                        "{} GET request on {}/{} for complete file range",
                        self.prefix, self.path_prefix, location
                    );
                }
            }
        }
        self.store.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        debug!(
            "{} HEAD request {}/{}",
            self.prefix, self.path_prefix, location
        );
        self.store.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.store.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        debug!(
            "{} LIST request {}/{:?}",
            self.prefix, self.path_prefix, prefix
        );
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        debug!(
            "{} PUT request to {}/{} of {} bytes",
            self.prefix,
            self.path_prefix,
            location,
            payload.content_length().to_formatted_string(&Locale::en)
        );
        self.store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        debug!(
            "{} PUT MULTIPART request to {}/{}",
            self.prefix, self.path_prefix, location
        );
        let part_upload = self.store.put_multipart_opts(location, opts).await?;
        Ok(Box::new(LoggingMultipartUpload::new(
            part_upload,
            &self.prefix,
            format!("{}/{}", self.path_prefix, location),
        )) as Box<dyn MultipartUpload>)
    }
}

#[derive(Debug)]
struct LoggingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    prefix: String,
    path: String,
}

impl LoggingMultipartUpload {
    pub fn new(
        inner: Box<dyn MultipartUpload>,
        prefix: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            prefix: prefix.into(),
            path: path.into(),
        }
    }
}

impl MultipartUpload for LoggingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        debug!(
            "{} multipart PUT to {} of {} bytes",
            self.prefix,
            self.path,
            data.content_length().to_formatted_string(&Locale::en)
        );
        self.inner.put_part(data)
    }

    fn complete<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> Pin<Box<dyn Future<Output = Result<PutResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        debug!("multipart to {} COMPLETE", self.path);
        self.inner.complete()
    }

    fn abort<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.abort()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::Level;
    use object_store::{integration::*, memory::InMemory};

    #[tokio::test]
    async fn log_test() {
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

    fn make_store() -> LoggingObjectStore<InMemory> {
        let inner = InMemory::new();
        LoggingObjectStore::new(inner, "TEST", "memory:/")
    }

    #[tokio::test]
    async fn zero_get_request() {
        // Given
        let store = make_store();

        // When
        // no op

        // Then
        assert_eq!(
            store.get_bytes_read(),
            0,
            "Non-zero bytes read after zero ops"
        );
        assert_eq!(store.get_count(), 0, "Non-zero GET count after zero ops");
    }

    #[tokio::test]
    async fn single_get_request() -> Result<()> {
        // Given
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.get(&"test_file".into()).await?;

        // Then
        assert_eq!(store.get_bytes_read(), 0);
        assert_eq!(store.get_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn single_ranged_read_requests() -> Result<()> {
        // Given
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.get_range(&"test_file".into(), 2..5).await?;

        // Then
        assert_eq!(store.get_bytes_read(), 3,);
        assert_eq!(store.get_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn multi_ranged_read_requests() -> Result<()> {
        // Given
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.get_range(&"test_file".into(), 0..3).await?;
        store.get_range(&"test_file".into(), 1..3).await?;
        store.get_range(&"test_file".into(), 5..8).await?;

        // Then
        assert_eq!(store.get_bytes_read(), 8,);
        assert_eq!(store.get_count(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn zero_log() {
        // Given
        testing_logger::setup();
        let _store = make_store();

        // When
        // no-op

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 0);
        });
    }

    #[tokio::test]
    async fn drop_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.get_range(&"test_file".into(), 1..5).await?;
        drop(store);

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 3);
            assert_eq!(
                captured_logs[1].body,
                "TEST GET request on memory://test_file byte range 1 to 5 = 4 bytes"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
            assert_eq!(
                captured_logs[2].body,
                "LoggingObjectStore \"TEST\" to \"memory:/\" made 1 GET requests and requested a total of 4 bytes (range bounded)"
            );
            assert_eq!(captured_logs[2].level, Level::Info);
        });

        Ok(())
    }

    #[tokio::test]
    async fn ranged_get_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.get_range(&"test_file".into(), 1..5).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST GET request on memory://test_file byte range 1 to 5 = 4 bytes"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn offset_get_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        let opts = GetOptions {
            range: Some(GetRange::Offset(3)),
            ..Default::default()
        };
        store.get_opts(&"test_file".into(), opts).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST GET request on memory://test_file for byte 3 to EOF"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn suffix_get_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        let opts = GetOptions {
            range: Some(GetRange::Suffix(3)),
            ..Default::default()
        };
        store.get_opts(&"test_file".into(), opts).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST GET request on memory://test_file for last 3 bytes of object"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn no_range_get_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        let opts = GetOptions::default();
        store.get_opts(&"test_file".into(), opts).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST GET request on memory://test_file for complete file range"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn head_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();
        store.put(&"test_file".into(), "some_data".into()).await?;

        // When
        store.head(&"test_file".into()).await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 2);
            assert_eq!(
                captured_logs[1].body,
                "TEST HEAD request memory://test_file"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn list_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        #[allow(unused_must_use)]
        store.list(Some(&"foo".into()));

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 1);
            assert_eq!(
                captured_logs[0].body,
                "TEST LIST request memory://Some(Path { raw: \"foo\" })"
            );
            assert_eq!(captured_logs[0].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn put_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        store
            .put_opts(&"test_file".into(), "foo".into(), PutOptions::default())
            .await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 1);
            assert_eq!(
                captured_logs[0].body,
                "TEST PUT request to memory://test_file of 3 bytes"
            );
            assert_eq!(captured_logs[0].level, Level::Debug);
        });

        Ok(())
    }

    #[tokio::test]
    async fn put_multipart_log() -> Result<()> {
        // Given
        testing_logger::setup();
        let store = make_store();

        // When
        let mut part = store.put_multipart(&"test_file".into()).await?;
        part.put_part("foo".into()).await?;
        part.put_part("foo1".into()).await?;
        part.put_part("foo12".into()).await?;
        part.complete().await?;

        // Then
        testing_logger::validate(|captured_logs| {
            assert_eq!(captured_logs.len(), 5);
            assert_eq!(
                captured_logs[0].body,
                "TEST PUT MULTIPART request to memory://test_file"
            );
            assert_eq!(captured_logs[0].level, Level::Debug);
            assert_eq!(
                captured_logs[1].body,
                "TEST multipart PUT to memory://test_file of 3 bytes"
            );
            assert_eq!(captured_logs[1].level, Level::Debug);
            assert_eq!(
                captured_logs[2].body,
                "TEST multipart PUT to memory://test_file of 4 bytes"
            );
            assert_eq!(captured_logs[2].level, Level::Debug);
            assert_eq!(
                captured_logs[3].body,
                "TEST multipart PUT to memory://test_file of 5 bytes"
            );
            assert_eq!(captured_logs[3].level, Level::Debug);
            assert_eq!(
                captured_logs[4].body,
                "multipart to memory://test_file COMPLETE"
            );
            assert_eq!(captured_logs[4].level, Level::Debug);
        });

        let retrieved_data = String::from_utf8(
            store
                .get(&"test_file".into())
                .await?
                .bytes()
                .await?
                .to_vec(),
        )
        .expect("String should be valid UTF-8");
        assert_eq!(retrieved_data, "foofoo1foo12");
        Ok(())
    }
}
