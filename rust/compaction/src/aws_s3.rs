//! AWS S3 crate.
//!
//! This module contains support functions and structs for accessing AWS S3 via the [`object_store`] crate.
/*
 * Copyright 2022-2023 Crown Copyright
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
use std::{future::ready, pin::Pin, sync::Arc};

use arrow::error::ArrowError;
use aws_types::region::Region;
use futures::Future;
use object_store::{
    aws::{AmazonS3Builder, AwsCredential},
    local::LocalFileSystem,
    CredentialProvider, Error, ObjectStore,
};
use url::Url;

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

/// Creates [`ObjectStore`] implementations from a URL and loads credentials into the S3
/// object store.
pub struct ObjectStoreFactory(Option<Arc<CredentialsFromConfigProvider>>, Region);

impl ObjectStoreFactory {
    #[must_use]
    pub fn new(value: Option<aws_credential_types::Credentials>, region: &Region) -> Self {
        Self(
            value.map(|value| Arc::new(CredentialsFromConfigProvider::new(&value))),
            region.clone(),
        )
    }
}

impl ObjectStoreFactory {
    /// Creates the appropriate [`ObjectStore`] for a given URL.
    ///
    /// The loaded credentials will also be set in the builder to enable authentication with S3.
    ///
    /// # Errors
    ///
    /// If no credentials have been provided, then trying to access S3 URLs will fail.
    pub fn get_object_store(&self, src: &Url) -> Result<Arc<dyn ObjectStore>, ArrowError> {
        match src.scheme() {
            "s3" => {
                if let Some(creds) = &self.0 {
                    Ok(AmazonS3Builder::from_env()
                        .with_credentials(creds.clone())
                        .with_region(self.1.as_ref())
                        .with_bucket_name(
                            src.host_str()
                                .ok_or(ArrowError::InvalidArgumentError("invalid S3 bucket name".into()))?,
                        )
                        .build()
                        .map(Arc::new)
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?)
                } else {
                    Err(ArrowError::InvalidArgumentError("Can't create AWS S3 object_store: no credentials provided to ObjectStoreFactory::from".into()))
                }
            }
            "file" => Ok(Arc::new(LocalFileSystem::new())),
            _ => Err(ArrowError::InvalidArgumentError(
                "no object store for given schema".into(),
            )),
        }
    }
}
