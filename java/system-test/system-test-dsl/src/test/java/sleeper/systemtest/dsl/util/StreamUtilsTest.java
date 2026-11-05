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
package sleeper.systemtest.dsl.util;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamUtilsTest {

    @Test
    void shouldInterleaveTwoStreamsOfSameSize() {
        // Given
        Stream<String> left = Stream.of("1", "2", "3");
        Stream<String> right = Stream.of("a", "b", "c");

        // When / Then
        assertThat(StreamUtils.interleave(left, right))
                .containsExactly("1", "a", "2", "b", "3", "c");
    }

    @Test
    void shouldPutBiggerStreamExcessAtEndWithLeftStreamBigger() {
        // Given
        Stream<String> left = Stream.of("1", "2", "3", "4");
        Stream<String> right = Stream.of("a", "b");

        // When / Then
        assertThat(StreamUtils.interleave(left, right))
                .containsExactly("1", "a", "2", "b", "3", "4");
    }

    @Test
    void shouldPutBiggerStreamExcessAtEndWithRightStreamBigger() {
        // Given
        Stream<String> left = Stream.of("1", "2");
        Stream<String> right = Stream.of("a", "b", "c");

        // When / Then
        assertThat(StreamUtils.interleave(left, right))
                .containsExactly("1", "a", "2", "b", "c");
    }

    @Test
    void shouldInterleaveWithEmptyLeftStream() {
        // Given
        Stream<String> left = Stream.of();
        Stream<String> right = Stream.of("a", "b");

        // When / Then
        assertThat(StreamUtils.interleave(left, right))
                .containsExactly("a", "b");
    }

    @Test
    void shouldInterleaveWithEmptyRightStream() {
        // Given
        Stream<String> left = Stream.of("1", "2");
        Stream<String> right = Stream.of();

        // When / Then
        assertThat(StreamUtils.interleave(left, right))
                .containsExactly("1", "2");
    }

    @Test
    void shouldInterleaveTwoEmptyStreams() {
        // Given
        Stream<String> left = Stream.of();
        Stream<String> right = Stream.of();

        // When / Then
        assertThat(StreamUtils.interleave(left, right))
                .isEmpty();
    }

}
