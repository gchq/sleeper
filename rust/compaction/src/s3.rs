//! Functions and structs relating to retrieving AWS S3 credentials.
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
    pin::Pin,
    sync::Arc,
};

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::ProvideCredentials;
use color_eyre::eyre::eyre;
use futures::Future;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder, AwsCredential},
    local::LocalFileSystem,
    ClientOptions, CredentialProvider, Error, Result,
};
use url::Url;

use crate::store::{LoggingObjectStore, SizeHintableStore};

/// A tuple struct to bridge AWS credentials obtained from the [`aws_config`] crate
/// and the [`CredentialProvider`] trait in the [`object_store`] crate.
#[derive(Debug)]
struct CredentialsFromConfigProvider(Arc<AwsCredential>);

impl CredentialsFromConfigProvider {
    /// Create a credentials provider for an `object_store` [`AmazonS3`] implementation. The credentials
    /// should be able to provide AWS key, secret key and session token.
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

/// Create an [`ObjectStore`] builder for AWS S3 for the given region and with provided credentials.
pub fn config_for_s3_module(
    creds: &aws_credential_types::Credentials,
    region: &Region,
) -> AmazonS3Builder {
    AmazonS3Builder::from_env()
        .with_credentials(Arc::new(CredentialsFromConfigProvider::new(creds)))
        .with_client_options(ClientOptions::default().with_timeout_disabled())
        .with_region(region.as_ref())
}

/// Create an [`AmazonS3`] object store from the default credential provider.
///
/// # Errors
///
/// This function will fail if we can't find any credentials in any of the
/// [standard places](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html),
/// or if a default region is not set.
pub async fn default_creds_store() -> color_eyre::Result<AmazonS3Builder> {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let creds = config
        .credentials_provider()
        .ok_or(eyre!("Couldn't retrieve AWS credentials"))?
        .provide_credentials()
        .await?;
    let region = config
        .region()
        .ok_or(eyre!("Couldn't retrieve AWS region"))?;
    Ok(config_for_s3_module(&creds, region))
}

/// Creates [`ObjectStore`] implementations from a URL and loads credentials into the S3
/// object store.
pub struct ObjectStoreFactory {
    s3_config: Option<AmazonS3Builder>,
    store_map: RefCell<HashMap<String, Arc<dyn SizeHintableStore>>>,
}

impl ObjectStoreFactory {
    #[must_use]
    pub fn new(s3_config: Option<AmazonS3Builder>) -> Self {
        Self {
            s3_config,
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
    pub fn get_object_store(&self, src: &Url) -> color_eyre::Result<Arc<dyn SizeHintableStore>> {
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
    fn make_object_store(&self, src: &Url) -> color_eyre::Result<Arc<dyn SizeHintableStore>> {
        match src.scheme() {
            "s3" => Ok(self
                .connect_s3(src)
                .map(|e| Arc::new(LoggingObjectStore::new(e, "S3")))?),
            "file" => Ok(Arc::new(LoggingObjectStore::new(
                LocalFileSystem::new(),
                "LOCAL",
            ))),
            _ => Err(eyre!("no object store for given schema")),
        }
    }

    fn connect_s3(&self, src: &Url) -> color_eyre::Result<AmazonS3> {
        match &self.s3_config {
            Some(config) => Ok(config
                .clone()
                .with_bucket_name(src.host_str().ok_or(eyre!("invalid S3 bucket name"))?)
                .build()?),
            None => Err(eyre!(
                "Can't create AWS S3 object_store: no credentials provided to ObjectStoreFactory"
            )),
        }
    }
}
