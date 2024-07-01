//! AWS S3 crate.
//!
//! This module contains support functions and structs for accessing AWS S3 via the [`object_store`] crate.
/*
 * Copyright 2022-2024 Crown Copyright
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
use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    future::ready,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex},
};

use aws_types::region::Region;
use bytes::{Bytes, BytesMut};
use color_eyre::eyre::eyre;
use futures::{stream::BoxStream, Future};
use log::info;
use num_format::{Locale, ToFormattedString};
use object_store::{
    aws::{AmazonS3Builder, AwsCredential},
    local::LocalFileSystem,
    path::Path,
    CredentialProvider, Error, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
    UploadPart,
};
use url::Url;

pub const MULTIPART_BUF_SIZE: usize = 50 * 1024 * 1024;

/// A tuple struct to bridge AWS credentials obtained from the [`aws_config`] crate
/// and the [`CredentialProvider`] trait in the [`object_store`] crate.
#[derive(Debug)]
struct CredentialsFromConfigProvider(Arc<AwsCredential>);

impl CredentialsFromConfigProvider {
    pub fn new(creds: &aws_credential_types::Credentials) -> CredentialsFromConfigProvider {
        Self(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_owned(),
            secret_key: creds.secret_access_key().to_owned(),
            token: creds.session_token().map(ToOwned::to_owned).clone(),
        }))
    }
}

impl CredentialProvider for CredentialsFromConfigProvider {
    type Credential = AwsCredential;

    fn get_credential<'a, 'async_trait>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Self::Credential>, Error>> + Send + 'async_trait>>
    where
        'a: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(ready(Ok(self.0.clone())))
    }
}

/// Trait the provides ability to count the number of GET
/// operations a store makes and number of bytes read.
pub trait CountingObjectStore: ObjectStore {
    /// Get the number of GET object requests this store as made.
    fn get_count(&self) -> Option<usize>;

    /// Get the number of bytes read in requests.
    fn get_bytes_read(&self) -> Option<usize>;

    /// Trait upcasting.
    fn as_object_store(self: Arc<Self>) -> Arc<dyn ObjectStore>;
}

/// Creates [`ObjectStore`] implementations from a URL and loads credentials into the S3
/// object store.
pub struct ObjectStoreFactory {
    creds: Option<Arc<CredentialsFromConfigProvider>>,
    region: Region,
    store_map: RefCell<HashMap<String, Arc<dyn CountingObjectStore>>>,
}

impl ObjectStoreFactory {
    #[must_use]
    pub fn new(value: Option<aws_credential_types::Credentials>, region: &Region) -> Self {
        Self {
            creds: value.map(|value| Arc::new(CredentialsFromConfigProvider::new(&value))),
            region: region.clone(),
            store_map: RefCell::new(HashMap::new()),
        }
    }

    /// Retrieves the appropriate [`ObjectStore`] for a given URL.
    ///
    /// The object returned will be the same for each subsequent call to this method for a given URL scheme.
    /// This method uses an internal cache to store the created [`ObjectStore`]s. The object will only
    /// be created the first time it is needed.
    ///
    /// The loaded credentials will also be set in the builder to enable authentication with S3.
    ///
    /// # Errors
    ///
    /// If no credentials have been provided, then trying to access S3 URLs will fail.
    pub fn get_object_store(&self, src: &Url) -> color_eyre::Result<Arc<dyn CountingObjectStore>> {
        let scheme = src.scheme();
        let mut borrow = self.store_map.borrow_mut();
        // Perform a single lookup into the cache map
        match borrow.entry(scheme.to_owned()) {
            // if entry found, then clone the shared pointer
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            // otherwise, attempt to create the object store
            Entry::Vacant(vacant) => match self.make_object_store(src) {
                // success? Insert it into the entry (first clone) then return the shared pointer, cloned from reference
                Ok(x) => Ok(vacant.insert(x.clone()).clone()),
                // otherwise propogate error
                Err(x) => Err(x),
            },
        }
    }

    /// Creates the appropriate [`ObjectStore`] for a given URL.
    ///
    /// The loaded credentials will also be set in the builder to enable authentication with S3.
    ///
    /// # Errors
    ///
    /// If no credentials have been provided, then trying to access S3 URLs will fail.
    fn make_object_store(&self, src: &Url) -> color_eyre::Result<Arc<dyn CountingObjectStore>> {
        match src.scheme() {
            "s3" => {
                if let Some(creds) = &self.creds {
                    Ok(AmazonS3Builder::from_env()
                        .with_credentials(creds.clone())
                        .with_region(self.region.as_ref())
                        .with_bucket_name(src.host_str().ok_or(eyre!("invalid S3 bucket name"))?)
                        .build()
                        .map(|e| Arc::new(LoggingObjectStore::new(Arc::new(e))))?)
                } else {
                    Err(eyre!("Can't create AWS S3 object_store: no credentials provided to ObjectStoreFactory::from"))
                }
            }
            "file" => Ok(Arc::new(LoggingObjectStore::new(Arc::new(
                LocalFileSystem::new(),
            )))),
            _ => Err(eyre!("no object store for given schema")),
        }
    }
}

/// An [`ObjectStore`] wrapper that logs every HEAD and GET request
/// the underlying store makes. The number of GETs can be retrieved
/// by using the `get_count` method.
#[derive(Debug)]
pub struct LoggingObjectStore {
    store: Arc<dyn ObjectStore>,
    get_count: Arc<Mutex<usize>>,
    get_bytes_read: Arc<Mutex<usize>>,
}

impl LoggingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            store: inner,
            get_count: Arc::new(Mutex::new(0)),
            get_bytes_read: Arc::new(Mutex::new(0)),
        }
    }
}

impl std::fmt::Display for LoggingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LoggingObjectStore({})", self.store)
    }
}

/// Creates a [`Range`] of `usize` from a [`GetRange`].
///
/// If the range is bounded, the returned range has the same bounds,
/// otherwise the returned range is from 0..N or N..[`usize::MAX`] as
/// appropriate.
pub fn to_range(range: &GetRange) -> Range<usize> {
    match range {
        GetRange::Bounded(r) => r.clone(),
        GetRange::Offset(n) => *n..usize::MAX,
        GetRange::Suffix(n) => 0..*n,
    }
}

impl ObjectStore for LoggingObjectStore {
    fn put<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        bytes: PutPayload,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<PutResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.store.put(location, bytes)
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        options: GetOptions,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<GetResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        if let Some(ref get_range) = options.range {
            let range = to_range(get_range);
            info!(
                "GET request byte range {} to {} = {} bytes",
                range.start.to_formatted_string(&Locale::en),
                range.end.to_formatted_string(&Locale::en),
                range.len().to_formatted_string(&Locale::en)
            );
            *self.get_bytes_read.lock().unwrap() += range.len();
        }
        *self.get_count.lock().unwrap() += 1;
        self.store.get_opts(location, options)
    }

    fn head<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<ObjectMeta>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        info!("HEAD request {}", location);
        self.store.head(location)
    }

    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = Result<()>> + ::core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.store.delete(location)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        info!("LIST request {:?}", prefix);
        self.store.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 Path>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<ListResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.store.list_with_delimiter(prefix)
    }

    fn copy<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = Result<()>> + ::core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.store.copy(from, to)
    }

    fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = Result<()>> + ::core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.store.copy_if_not_exists(from, to)
    }

    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<PutResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        info!(
            "PUT request {} bytes",
            payload.content_length().to_formatted_string(&Locale::en)
        );
        self.store.put_opts(location, payload, opts)
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        opts: PutMultipartOpts,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn MultipartUpload>>> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        Box::pin(async move {
            info!("PUT MULTIPART request to {}", location);
            let thing = self.store.put_multipart_opts(location, opts).await?;
            Ok(
                Box::new(BufferingMultipartUpload::new(thing, MULTIPART_BUF_SIZE))
                    as Box<dyn MultipartUpload>,
            )
        })
    }
}

impl CountingObjectStore for LoggingObjectStore {
    fn get_count(&self) -> Option<usize> {
        Some(*self.get_count.lock().unwrap())
    }
    fn get_bytes_read(&self) -> Option<usize> {
        Some(*self.get_bytes_read.lock().unwrap())
    }

    fn as_object_store(self: Arc<Self>) -> Arc<dyn ObjectStore> {
        self
    }
}

#[derive(Debug)]
struct BufferingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    buffer: BytesMut,
    capacity: usize,
}

impl BufferingMultipartUpload {
    pub fn new(inner: Box<dyn MultipartUpload>, capacity: usize) -> Self {
        Self {
            inner,
            buffer: BytesMut::with_capacity(capacity),
            capacity,
        }
    }

    fn upload_buffer(&mut self) -> UploadPart {
        info!(
            "Uploading {} bytes",
            self.buffer.len().to_formatted_string(&Locale::en)
        );
        let oldbuf = std::mem::replace(&mut self.buffer, BytesMut::with_capacity(self.capacity));
        self.inner.put_part(PutPayload::from(Bytes::from(oldbuf)))
    }
}

impl MultipartUpload for BufferingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        for bytes in &data {
            self.buffer.extend_from_slice(bytes);
        }
        // Should we upload this?
        if self.buffer.len() >= self.capacity {
            return self.upload_buffer();
        }
        Box::pin(async { Ok(()) })
    }

    fn complete<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<PutResult>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            info!("multipart COMPLETE");
            if !self.buffer.is_empty() {
                self.upload_buffer().await?;
            }
            self.inner.complete().await
        })
    }

    fn abort<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = Result<()>> + ::core::marker::Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.abort()
    }
}
