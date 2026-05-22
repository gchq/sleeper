//! Details for AWS access.
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
use crate::{objects::ffi_common_config::FFICommonConfig, unpack::unpack_string};
use color_eyre::eyre::bail;
use sleeper_core::{AwsConfig, AwsSecrets};
use std::ffi::c_char;

pub fn unpack_aws_config(params: &FFICommonConfig) -> Option<AwsConfig> {
    if let Some(ffi_aws) = unsafe { params.aws_config.as_ref() } {
        AwsConfig::try_from(ffi_aws).ok()
    } else {
        None
    }
}

/// Contains the FFI compatible configuration data for AWS.
///
/// *THIS IS A C COMPATIBLE FFI STRUCT!* If you updated this struct (field ordering, types, etc.),
/// you MUST update the corresponding Java definition in java/common/foreign-bridge/src/main/java/sleeper/foreign/FFIAwsConfig.java.
/// The order and types of the fields must match exactly.
#[repr(C)]
pub struct FFIAwsConfig {
    pub region: *const c_char,
    pub endpoint: *const c_char,
    pub access_key_id: *const c_char,
    pub secret_access_key: *const c_char,
    pub session_token: *const c_char,
    pub allow_http: bool,
}

/// Convert a raw pointer into an [`Option`].
///
/// A NULL pointer becomes a [`None`].
fn to_option<T>(ptr: *const T) -> Option<*const T> {
    if ptr.is_null() { None } else { Some(ptr) }
}

impl TryFrom<&FFIAwsConfig> for AwsConfig {
    type Error = color_eyre::Report;

    fn try_from(value: &FFIAwsConfig) -> Result<Self, Self::Error> {
        let session_token = to_option(value.session_token)
            .map(unpack_string)
            .transpose()?;

        // access_key_id and secret_access_key must either both be null or both specified
        let secrets = match (
            to_option(value.access_key_id),
            to_option(value.secret_access_key),
        ) {
            (Some(access_key_id), Some(secret_access_key)) => Some(AwsSecrets {
                access_key_id: unpack_string(access_key_id)?,
                secret_access_key: unpack_string(secret_access_key)?,
                session_token,
            }),
            (None, None) => None,
            (None, Some(_)) => bail!(
                "FFIAwsConfig access_key_id is NULL, but secret_access_key was supplied. Supply both or neither"
            ),
            (Some(_), None) => bail!(
                "FFIAwsConfig secret_access_key is NULL, but access_key_id was supplied. Supply both or neither"
            ),
        };

        let region = to_option(value.region).map(unpack_string).transpose()?;
        let endpoint = to_option(value.region).map(unpack_string).transpose()?;

        Ok(AwsConfig {
            secrets,
            region,
            endpoint,
            allow_http: value.allow_http,
        })
    }
}
