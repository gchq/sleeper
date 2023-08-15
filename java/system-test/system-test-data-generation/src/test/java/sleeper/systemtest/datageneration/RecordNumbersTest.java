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

package sleeper.systemtest.datageneration;

import org.junit.jupiter.api.Test;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordNumbersTest {
    @Test
    void shouldShuffleRecordsWithFixedRandomSeed() {
        // Given
        LongStream longStream = LongStream.of(1L, 2L, 3L, 4L, 5L);

        // When/Then
        assertThat(RecordNumbers.scrambleNumberedRecords(longStream).stream())
                .containsExactly(5L, 3L, 2L, 4L, 1L);
    }

    @Test
    void shouldGetRangeOfShuffledRecordsWithFixedRandomSeed() {
        // Given
        LongStream longStream = LongStream.of(1L, 2L, 3L, 4L, 5L);
        RecordNumbers recordNumbers = RecordNumbers.scrambleNumberedRecords(longStream);

        // When/Then
        assertThat(recordNumbers.range(1, 4))
                .containsExactly(3L, 2L, 4L);
    }
}
