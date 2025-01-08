//! Implementation details for the stream readahead and caching for `object_store`.
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
use bytes::Bytes;
use futures::{
    stream::{empty, BoxStream},
    Stream, StreamExt,
};
use log::debug;
use num_format::{Locale, ToFormattedString};
use object_store::Result;
use std::{fmt::Debug, pin::Pin, task::Poll};

type BoxFunc = Box<dyn FnOnce(&mut PositionedStream) + Send + Sync>;

/// A simple wrapper on a `object_store` byte stream that knows its position.
#[allow(clippy::module_name_repetitions)]
pub struct PositionedStream {
    // Underlying stream of Bytes
    inner: BoxStream<'static, Result<Bytes>>,
    // Absolute position in stream of object
    pos: usize,
    // Absolute position for where to stop this stream (exclusive)
    stop_pos: usize,
    // Function to run when this instance is destructed
    des_func: Option<BoxFunc>,
}

impl Debug for PositionedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PositionedStream")
            .field("inner", &"<<< stream of Result<Bytes> >>>")
            .field("pos", &self.pos)
            .field("stop_pos", &self.stop_pos)
            .field("des_func", &"<<< function >>>")
            .finish()
    }
}

impl Drop for PositionedStream {
    /// Call the destruction function (if present) when an instance is dropped.
    fn drop(&mut self) {
        if let Some(func) = self.des_func.take() {
            (func)(self);
        }
    }
}

impl PositionedStream {
    /// Wrap the given byte stream.
    #[allow(dead_code)]
    pub fn new(inner: BoxStream<'static, Result<Bytes>>, pos: usize, stop_pos: usize) -> Self {
        Self::new_with_disposal(inner, pos, stop_pos, None::<fn(&mut PositionedStream)>)
    }
    /// Wrap the given byte stream with a given function to call when instance is dropped.
    pub fn new_with_disposal<F: FnOnce(&mut PositionedStream) + Send + Sync + 'static>(
        inner: BoxStream<'static, Result<Bytes>>,
        pos: usize,
        stop_pos: usize,
        des_func: Option<F>,
    ) -> Self {
        assert!(pos <= stop_pos, "Position greater than stop position");
        Self {
            inner,
            pos,
            stop_pos,
            des_func: des_func.map(|f| Box::new(f) as BoxFunc),
        }
    }

    /// Skip the underlying stream ahead to the given position.
    ///
    /// Any bytes read are thrown away. On success, the number of bytes
    /// skipped are returned. If the underlying stream ends before reaching
    /// the given position, the `Ok(None)` is returned.
    ///
    /// # Errors
    /// Any stream errors are returned to the user.
    ///
    /// # Panics
    /// If `desired_pos` is less than the current position or greater than
    /// the stop position of the stream.
    pub async fn skip_to(&mut self, desired_pos: usize) -> Result<Option<usize>> {
        assert!(
            desired_pos >= self.pos,
            "Desired position less than current position"
        );
        assert!(
            desired_pos <= self.stop_pos,
            "Desired position greater than stop position"
        );
        let mut count = 0;
        while self.pos < desired_pos {
            // Grab next group of bytes, bail if stream is empty
            match self.inner.next().await {
                Some(bytes) => {
                    let mut bytes = bytes?;
                    bytes = self.splice_bytes(bytes, desired_pos);
                    count += bytes.len();
                }
                None => return Ok(None),
            }
        }
        debug!(
            "Skipped stream {} bytes ahead to position {}",
            count.to_formatted_string(&Locale::en),
            self.pos.to_formatted_string(&Locale::en)
        );
        Ok(Some(count))
    }

    /// Takes the given [`Bytes`] and possibly slices them if they would take
    /// this stream passed the desired position. If not, then the bytes are
    /// returned unaltered. Otherwise, the excess bytes are pushed on to the front
    /// of the underlying stream to be read next time and the needed bytes are returned.
    ///
    /// # Panics
    /// If the desired position is less than the current stream position.
    ///
    fn splice_bytes(&mut self, mut bytes: Bytes, desired_pos: usize) -> Bytes {
        assert!(
            desired_pos >= self.pos,
            "Desired position must be less than current position"
        );
        // Does this byte slice extend beyond the required position?
        if self.pos + bytes.len() > desired_pos {
            // Calculate slice index
            let byte_slice_pos = desired_pos - self.pos;
            // Split byte array in two (O(1) operation) and mutate v
            let remaining_bytes = bytes.split_off(byte_slice_pos);
            // Swap out the stream in the struct
            let remaining_stream = std::mem::replace(&mut self.inner, Box::pin(empty()));
            // Create a single element stream of the remaining bytes and then chain the rest of the stream
            let stream =
                futures::stream::once(async { Ok(remaining_bytes) }).chain(remaining_stream);
            self.inner = Box::pin(stream);
        }
        self.pos += bytes.len();
        bytes
    }

    /// Get the current absolute position of this stream.
    #[must_use]
    #[allow(dead_code)]
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Get the absolute stop position of this stream (exclusive).
    #[must_use]
    #[allow(dead_code)]
    pub fn stop_pos(&self) -> usize {
        self.stop_pos
    }

    /// Retrieve the inner byte stream.
    pub fn inner(&mut self) -> &mut BoxStream<'static, Result<Bytes>> {
        &mut self.inner
    }
}

impl Stream for PositionedStream {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        assert!(
            self.stop_pos >= self.pos,
            "Position should never be greater than stop position"
        );
        // Have we already reached end of stream?
        let desired_pos = self.stop_pos;
        if self.pos >= desired_pos {
            Poll::Ready(None)
        } else {
            let mut result = self.inner.as_mut().poll_next(cx);
            // If we have bytes, then increase position
            if let Poll::Ready(Some(Ok(mut v))) = result {
                // If the bytes received takes us passed the desired position, then
                // splice back into stream
                v = self.splice_bytes(v, desired_pos);
                result = Poll::Ready(Some(Ok(v)));
            }
            result
        }
    }

    /// This method is overridden to always return `(0, None)` because
    /// we don't know how many `Result<Bytes>` the underlying stream will return.
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::readahead::tests::{assert_stream_eq, into_stream, test_stream};

    use super::*;
    use futures::stream;

    #[tokio::test]
    #[should_panic(expected = "Position greater than stop position")]
    async fn panic_on_bad_pos() {
        PositionedStream::new(Box::pin(stream::empty()), 5, 4);
    }

    #[tokio::test]
    async fn test_create() {
        let _ = PositionedStream::new(Box::pin(stream::empty()), 0, 1);
    }

    #[test]
    fn should_call_destruction_function() {
        // Given
        let flag = Arc::new(Mutex::new(false));
        let flag_clone = flag.clone();
        let stream = PositionedStream::new_with_disposal(
            Box::pin(stream::empty()),
            0,
            1,
            Some(move |_: &mut PositionedStream| {
                *flag_clone.lock().unwrap() = true;
            }),
        );

        // When
        assert!(!*flag.lock().unwrap()); // flag is false
        drop(stream);

        // Then
        assert!(*flag.lock().unwrap()); // flag is true
    }

    #[tokio::test]
    #[should_panic(expected = "Desired position less than current position")]
    async fn skip_panic_desired_pos() {
        // Given
        let mut ps = PositionedStream::new(Box::pin(stream::empty()), 5, 9);

        // Then - panic
        ps.skip_to(1).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "Desired position greater than stop position")]
    async fn skip_panic_desired_end_pos() {
        // Given
        let mut ps = PositionedStream::new(Box::pin(stream::empty()), 0, 9);

        // When
        ps.skip_to(10).await.unwrap();

        // Then - panic
    }

    #[tokio::test]
    async fn skip_zero() -> Result<()> {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 0, 20);

        // When
        ps.skip_to(0).await?;

        // Then
        assert_eq!(ps.pos, 0);
        assert_stream_eq(Box::pin(ps), test_stream()).await;

        Ok(())
    }

    #[tokio::test]
    async fn skip_one() -> Result<()> {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 0, 20);

        // When
        ps.skip_to(1).await?;

        // Then
        assert_eq!(ps.pos, 1);
        assert_stream_eq(
            Box::pin(ps),
            into_stream(vec!["2345", "6789", "012", "34567890"]),
        )
        .await;

        Ok(())
    }

    #[tokio::test]
    async fn skip_five() -> Result<()> {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 0, 20);

        // When
        ps.skip_to(5).await?;

        // Then
        assert_eq!(ps.pos, 5);
        assert_stream_eq(Box::pin(ps), into_stream(vec!["6789", "012", "34567890"])).await;

        Ok(())
    }

    #[tokio::test]
    async fn skip_multiple() -> Result<()> {
        // Given
        let mut ps = Box::pin(PositionedStream::new(test_stream(), 0, 20));

        // When
        ps.skip_to(3).await?;
        ps.skip_to(6).await?;

        assert_eq!(ps.pos, 6);
        assert_stream_eq(ps, into_stream(vec!["789", "012", "34567890"])).await;

        Ok(())
    }

    #[test]
    #[should_panic(expected = "Desired position must be less than current position")]
    fn panic_on_splice_assert() {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 5, 20);

        // When
        ps.splice_bytes(Bytes::new(), 4);

        // Then - panic
    }

    #[test]
    fn splice_not_needed() {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 0, 20);

        // When
        ps.splice_bytes(Bytes::new(), 1);

        // Then
        assert_eq!(ps.pos, 0, "Stream should not have moved");
    }

    #[tokio::test]
    async fn splice_zero_bytes() {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 0, 26);

        // When
        let res = ps.splice_bytes(Bytes::from("ABCDEF"), 0);

        // Then
        assert_eq!(ps.pos, 0, "Stream should not have moved");
        assert_eq!(res, Bytes::from(""));
        assert_stream_eq(
            Box::pin(ps),
            into_stream(vec!["ABCDEF", "12345", "6789", "012", "34567890"]),
        )
        .await;
    }

    #[tokio::test]
    async fn splice_one_byte() {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 0, 26);

        // When
        let res = ps.splice_bytes(Bytes::from("ABCDEF"), 1);

        // Then
        assert_eq!(ps.pos, 1, "Stream should move to 1");
        assert_eq!(res, Bytes::from("A"));
        assert_stream_eq(
            Box::pin(ps),
            into_stream(vec!["BCDEF", "12345", "6789", "012", "34567890"]),
        )
        .await;
    }

    #[tokio::test]
    async fn splice_multiple_bytes() {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 0, 26);

        // When
        let res = ps.splice_bytes(Bytes::from("ABCDEF"), 5);

        // Then
        assert_eq!(ps.pos, 5, "Stream should move to 5");
        assert_eq!(res, Bytes::from("ABCDE"));
        assert_stream_eq(
            Box::pin(ps),
            into_stream(vec!["F", "12345", "6789", "012", "34567890"]),
        )
        .await;
    }

    #[tokio::test]
    async fn splice_multiple_times() {
        // Given
        let mut ps = PositionedStream::new(test_stream(), 0, 30);

        // When
        let res = ps.splice_bytes(Bytes::from("ABCDEF"), 5);

        // Then
        assert_eq!(ps.pos, 5, "Stream should move to 5");
        assert_eq!(res, Bytes::from("ABCDE"));

        // When
        let res = ps.splice_bytes(Bytes::from("ZYXW"), 7);

        // Then
        assert_eq!(ps.pos, 7, "Stream should move to 7");
        assert_eq!(res, Bytes::from("ZY"));
        assert_stream_eq(
            Box::pin(ps),
            into_stream(vec!["XW", "F", "12345", "6789", "012", "34567890"]),
        )
        .await;
    }

    #[tokio::test]
    async fn stops_at_stop_position_zero() {
        // Given
        let ps = PositionedStream::new(test_stream(), 0, 0);

        // When

        // Then
        assert_stream_eq(Box::pin(ps), into_stream(vec![])).await;
    }

    #[tokio::test]
    async fn stops_at_stop_position_one() {
        // Given
        let ps = PositionedStream::new(test_stream(), 0, 1);

        // When

        // Then
        assert_stream_eq(Box::pin(ps), into_stream(vec!["1"])).await;
    }

    #[tokio::test]
    async fn stops_at_stop_position_ten() {
        // Given
        let ps = PositionedStream::new(test_stream(), 0, 10);

        // When

        // Then
        assert_stream_eq(Box::pin(ps), into_stream(vec!["12345", "6789", "0"])).await;
    }

    #[test]
    fn size_hint_should_be_zero() {
        // Given
        let ps = PositionedStream::new(test_stream(), 0, 20);

        // When

        // Then
        assert_eq!(ps.size_hint(), (0, None));
    }
}
