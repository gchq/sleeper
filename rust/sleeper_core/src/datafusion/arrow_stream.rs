//! This module contains the iterator code that uses a Tokio runtime to
//! convert an asynchronous [`Stream`] into a synchronous iterator
//! that can then be served across an FFI boundary.
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
#[allow(unused)]
use arrow::{
    array::RecordBatchIterator, error::ArrowError, ffi_stream::FFI_ArrowArrayStream,
    record_batch::RecordBatch,
};
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use std::{pin::Pin, sync::Arc};
use tokio::runtime::Runtime;

/// Adapts a [`Stream`] into an [`Iterator`].
///
/// This allows objects implementing the asynchronous [`Stream`] trait to be
/// used in a synchronous context.
struct IterableStreamAdapter<S>
where
    S: ?Sized,
{
    stream: Pin<Box<S>>,
    rt: Arc<Runtime>,
}

impl<S: Stream + ?Sized> IterableStreamAdapter<S> {
    /// Creates a stream adapter.
    ///
    /// The runtime must be an active Tokio runtime.
    #[must_use]
    pub fn new(stream: Pin<Box<S>>, rt: Arc<Runtime>) -> Self {
        Self { stream, rt }
    }
}

impl<S: Stream + ?Sized> Iterator for IterableStreamAdapter<S> {
    type Item = S::Item;

    /// Gets the next item from the stream.
    ///
    /// This function will use the contained [`Runtime`] object to
    /// obtain the next item from the stream and block the current
    /// thread until the stream returns the next item.
    fn next(&mut self) -> Option<Self::Item> {
        // Ask Tokio runtime for next block from stream, so we block
        // the current thread.
        self.rt.block_on(async { self.stream.next().await })
    }
}

trait ErrorConvert<I> {
    /// Converts an iterator to a type that uses an [`ArrowError`] error type.
    ///
    /// This should be implemented for any [`Iterator`] type with a [`Result`]
    /// item type as long as the error variant can be converted to an [`ArrowError`].
    fn to_arrow_error(self) -> impl Iterator<Item = Result<I, ArrowError>>;
}

impl<I, E, T> ErrorConvert<I> for T
where
    T: Iterator<Item = Result<I, E>>,
    E: Into<ArrowError>,
{
    fn to_arrow_error(self) -> impl Iterator<Item = Result<I, ArrowError>> {
        self.map(|r| r.map_err(Into::into))
    }
}

/// Converts an asynchronous stream of Arrow [`RecordBatch`]es to an FFI compatible
/// Arrow array stream.
///
/// This function will convert the asynchronous stream of record batches to a
/// synchronous blocking iterator that is compatible with Arrow's C stream interface.
/// When the next data batch is requested, the current thread will block until it is
/// available from the stream.
///
/// # Memory management
///
/// This function consumes ownership of the stream, i.e., reponsibility for releasing
/// the underlying stream lies with the [`FFI_ArrowArrayStream`]. Once the stream
/// is no longer needed, the [`FFI_ArrowArrayStream::release`] function pointer should
/// be called.
///
/// See [<https://arrow.apache.org/docs/format/CStreamInterface.html>]
/// for details on Arrow's FFI streaming interface.
pub fn stream_to_ffi_arrow_stream(
    stream: SendableRecordBatchStream,
    rt: Arc<Runtime>,
) -> FFI_ArrowArrayStream {
    let schema = stream.schema().clone();

    // 1. The stream is asynchronous, so first convert stream to a blocking synchronous iterator
    let adapter = IterableStreamAdapter::new(stream, rt).to_arrow_error();

    // 2. Next make iterator into something that implements RecordBatchReader as required by the FFI array stream
    let batch_iterator = RecordBatchIterator::new(adapter, schema);

    // 3. This can now be made into the FFI compatible wrapper
    FFI_ArrowArrayStream::new(Box::new(batch_iterator))
}
