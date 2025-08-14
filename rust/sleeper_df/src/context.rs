//! Defines the FFI context object and functions.
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

use std::sync::Arc;
use tokio::runtime::Runtime;

/// Creates a Tokio [`Runtime`].
///
/// This is used internally to run asynchronous code
/// from a synchronous context to enable FFI code to work.
fn make_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// This contains all the information needed to run queries across an FFI
/// boundary. Consumers (likely in a different language) should not be
/// attempting to read the contents of anything in this struct.
///
/// This object should contain all "state" necessary to allow Rust allocated
/// data to exist between FFI function calls.
///
/// This struct is NOT marked as `#[repr(C)]` because it is not intended
/// for access by other languages. Although the [`create_context`] function
/// returns a pointer to an [`FFI_Context`], it should be treated as an
/// [opaque pointer](https://en.wikipedia.org/wiki/Opaque_pointer).
/// Passing a pointer back across an FFI layer is safe as long as the
/// consumer simply treats it as a meangingless handle.
#[allow(non_camel_case_types)]
pub struct FFIContext {
    /// Shared pointer to a runtime.
    pub rt: Arc<Runtime>,
}

impl FFIContext {
    /// Create a new context object for enabling
    /// FFI to work.
    pub fn new(rt: Runtime) -> Self {
        Self { rt: Arc::new(rt) }
    }
}

/// Creates the context needed to enable further FFI calls.
///
/// The needed state is allocated and initialised so that
/// data that must be maintained between different FFI calls
/// is not disposed of.
///
/// This forms part of the external interface to this crate,
/// thus it is marked as `extern "C"` to make it publicly available
/// to FFI clients.
///
/// It is the callers responsibility to free the context when
/// it is no longer needed by calling [`destroy_context`]. If this
/// context is destroyed while functions are still running with it,
/// some internal resources may not be freed until all remaining
/// references are themselves freed. E.g. if you have called [`run_query`]
/// and still hold references to the returned stream, you may
/// safely call [`destroy_context`] and still use the existing stream.
///
/// # Safety
/// Callers should treat the returned pointer as an [opaque
/// pointer](https://en.wikipedia.org/wiki/Opaque_pointer) and
/// **MUST NOT** assign any meaning to the value of the
/// returned pointer!
///
/// The value should be treated simply as a unique value
/// for a specific context created by this function.
///
/// Do not alter the returned pointer value, or attempt to
/// interpret its meaning. The value will change between
/// subsequent runs of an application and should NOT be
/// relied upon.
///
/// **DO NOT** attempt to read any data behind the pointer
/// as its layout, alignment and contents may change without
/// notice between versions and may differ between OSes, Rust
/// versions and hardware platform.
#[unsafe(no_mangle)]
#[must_use]
pub extern "C" fn create_context() -> *mut FFIContext {
    let rt = make_runtime();
    // Create new context and leak the pointer
    let content = Box::new(FFIContext::new(rt));
    // Rust is no longer reponsible for lifetime of this object
    let ptr = Box::into_raw(content);
    ptr
}

/// Destroy and free all resources previously allocated for a context.
///
/// This frees the given context. No more queries can be executed
/// by this context after this call, although currently running queries
/// will continue to execute safely.
///
/// It is the callers responsibility to call this function when a
/// context is no longer needed.
///
/// This forms part of the external interface to this crate,
/// thus it is marked as `extern "C"` to make it publicly available
/// to FFI clients.
///
/// # Safety
/// **DO NOT** call this function on the same pointer twice. Doing so
/// will result in undefined behaviour.
///
/// # Errors
/// If a null pointer is passed to this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn destroy_context(ctx: *mut FFIContext) {
    if let Some(ctx_ref) = unsafe { ctx.as_mut() } {
        // Rust assumes ownership of pointed to object here and then
        // safely de-allocates it
        let _ = unsafe { Box::from_raw(ctx_ref) };
    } else {
        panic!("Null pointer passed to destroy_context!");
    }
}
