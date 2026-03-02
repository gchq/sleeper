//! Details for AWS access.
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
use crate::{objects::ffi_common_config::FFICommonConfig, unpack::unpack_string};
use color_eyre::eyre::bail;
use sleeper_core::AwsConfig;
use std::ffi::c_char;

pub fn unpack_aws_config(
    params: &FFICommonConfig,
) -> Result<Option<AwsConfig>, color_eyre::Report> {
    Ok(if params.override_aws_config {
        let Some(ffi_aws) = (unsafe { params.aws_config.as_ref() }) else {
            bail!("override_aws_config is true, but aws_config pointer is NULL");
        };
        AwsConfig::try_from(ffi_aws).ok()
    } else {
        None
    })
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
    pub access_key: *const c_char,
    pub secret_key: *const c_char,
    pub session_token: *const c_char,
    pub allow_http: bool,
}

impl TryFrom<&FFIAwsConfig> for AwsConfig {
    type Error = color_eyre::Report;

    fn try_from(value: &FFIAwsConfig) -> Result<Self, Self::Error> {
        if value.region.is_null() {
            bail!("FFIAwsConfig region pointer is NULL");
        }
        if value.endpoint.is_null() {
            bail!("FFIAwsConfig endpoint pointer is NULL");
        }
        if value.access_key.is_null() {
            bail!("FFIAwsConfig access_key pointer is NULL");
        }
        if value.secret_key.is_null() {
            bail!("FFIAwsConfig secret_key pointer is NULL");
        }
        Ok(AwsConfig {
            region: unpack_string(value.region)?,
            endpoint: unpack_string(value.endpoint)?,
            access_key: unpack_string(value.access_key)?,
            secret_key: unpack_string(value.secret_key)?,
            session_token: Some(unpack_string(value.session_token)?).filter(|s| !s.is_empty()),
            allow_http: value.allow_http,
        })
    }
}
