//! Utility functions for unpacking FFI arrays and objects.
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
use color_eyre::eyre::Result;
use std::ffi::{CStr, c_char};

pub fn unpack_str<'a>(pointer: *const c_char) -> Result<&'a str> {
    Ok(unsafe { CStr::from_ptr(pointer) }.to_str()?)
}

pub fn unpack_string(pointer: *const c_char) -> Result<String> {
    unpack_str(pointer).map(ToOwned::to_owned)
}
