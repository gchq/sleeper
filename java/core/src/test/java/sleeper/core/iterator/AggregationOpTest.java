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
package sleeper.core.iterator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class AggregationOpTest {

    @SuppressWarnings("unchecked")
    @Nested
    @DisplayName("Sum operation tests")
    class SumOperationTest {
        // Given
        AggregationOp sum = AggregationOp.SUM;

        @Test
        public void shouldApplySumInt() {
            // When / Then
            assertThat(sum.apply(1, 2))
                    .isEqualTo(3);
        }

        @Test
        public void shouldApplySumLong() {
            // When / Then
            assertThat(sum.apply(1L, 2L))
                    .isEqualTo(3L);
        }

        @Test
        public void shouldApplySumString() {
            // When / Then
            assertThat(sum.apply("one string", "two string"))
                    .isEqualTo("one stringtwo string");
        }

        @Test
        public void shouldApplySumByteArray() {
            // When / Then
            assertThat(sum.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10}))
                    .isEqualTo(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        }

        @Test
        public void shouldApplySumMapInt() {
            // When
            Object mapIntResult = sum.apply(Map.of("key1", 1, "key2", 3),
                    Map.of("key2", 4, "key3", 6));
            // Then
            assertThat((Map<String, Integer>) mapIntResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 7, "key3", 6));
        }

        @Test
        public void shouldApplySumMapLong() {
            // When
            Object mapLongResult = sum.apply(Map.of("key1", 1L, "key2", 3L),
                    Map.of("key2", 4L, "key3", 6L));

            //Then
            assertThat((Map<String, Long>) mapLongResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 7L, "key3", 6L));
        }

        @Test
        public void shouldApplySumMapString() {
            // When
            Object mapStringResult = sum.apply(Map.of("key1", "test", "key2", "one string"),
                    Map.of("key2", "two string", "key3", "other"));

            // Then
            assertThat((Map<String, String>) mapStringResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "one stringtwo string", "key3", "other"));
        }

        @Test
        public void shouldApplySumMapByteArray() {
            // When
            Object mapByteArrayResult = sum.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                    Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));
            // THen
            assertThat((Map<String, byte[]>) mapByteArrayResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4, 5, 6}, "key3", new byte[]{7, 8}));
        }
    }

    @SuppressWarnings("unchecked")
    @Nested
    @DisplayName("Min operation tests")
    class MinOperationTest {
        // Given
        AggregationOp min = AggregationOp.MIN;

        @Test
        public void shouldApplyMinInt() {
            // When / Then
            assertThat(min.apply(1, 2))
                    .isEqualTo(1);
        }

        @Test
        public void shouldApplyMinLong() {
            // When / Then
            assertThat(min.apply(1L, 2L))
                    .isEqualTo(1L);
        }

        @Test
        public void shouldApplyMinString() {
            // When / Then
            assertThat(min.apply("one string", "two string"))
                    .isEqualTo("one string");
        }

        @Test
        public void shouldApplyMinByteArray() {
            // When / Then
            assertThat(min.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10}))
                    .isEqualTo(new byte[]{1, 2, 3, 4, 5});
        }

        @Test
        public void shouldApplyMinMapInt() {
            // When
            Object mapIntResult = min.apply(Map.of("key1", 1, "key2", 3),
                    Map.of("key2", 4, "key3", 6));
            // Then
            assertThat((Map<String, Integer>) mapIntResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 3, "key3", 6));
        }

        @Test
        public void shouldApplyMinMapLong() {
            // When
            Object mapLongResult = min.apply(Map.of("key1", 1L, "key2", 3L),
                    Map.of("key2", 4L, "key3", 6L));
            // Then
            assertThat((Map<String, Long>) mapLongResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 3L, "key3", 6L));
        }

        @Test
        public void shouldApplyMinMapString() {
            // When
            Object mapStringResult = min.apply(Map.of("key1", "test", "key2", "one string"),
                    Map.of("key2", "two string", "key3", "other"));
            // Then
            assertThat((Map<String, String>) mapStringResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "one string", "key3", "other"));
        }

        @Test
        public void shouldApplyMinMapByteArray() {
            // When
            Object mapByteArrayResult = min.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                    Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));
            // Then
            assertThat((Map<String, byte[]>) mapByteArrayResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}, "key3", new byte[]{7, 8}));
        }
    }

    @SuppressWarnings("unchecked")
    @Nested
    @DisplayName("Max operation tests")
    class MaxOperationTest {
        // Given
        AggregationOp max = AggregationOp.MAX;

        @Test
        public void shouldApplyMaxInt() {
            // When / Then
            assertThat(max.apply(1, 2))
                    .isEqualTo(2);
        }

        @Test
        public void shouldApplyMaxLong() {
            // When / Then
            assertThat(max.apply(1L, 2L))
                    .isEqualTo(2L);
        }

        @Test
        public void shouldApplyMaxString() {
            // When / Then
            assertThat(max.apply("one string", "two string"))
                    .isEqualTo("two string");
        }

        @Test
        public void shouldApplyMaxByteArray() {
            // When / Then
            assertThat(max.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10}))
                    .isEqualTo(new byte[]{6, 7, 8, 9, 10});
        }

        @Test
        public void shouldApplyMaxMapInt() {
            // When
            Object mapIntResult = max.apply(Map.of("key1", 1, "key2", 3),
                    Map.of("key2", 4, "key3", 6));
            // Then
            assertThat((Map<String, Integer>) mapIntResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 4, "key3", 6));
        }

        @Test
        public void shouldApplyMaxMapLong() {
            // When
            Object mapLongResult = max.apply(Map.of("key1", 1L, "key2", 3L),
                    Map.of("key2", 4L, "key3", 6L));

            // Then
            assertThat((Map<String, Long>) mapLongResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 4L, "key3", 6L));
        }

        @Test
        public void shouldApplyMaxMapString() {
            // When
            Object mapStringResult = max.apply(Map.of("key1", "test", "key2", "one string"),
                    Map.of("key2", "two string", "key3", "other"));

            // Then
            assertThat((Map<String, String>) mapStringResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "two string", "key3", "other"));
        }

        @Test
        public void shouldApplyMaxMapByteArray() {
            // When
            Object mapByteArrayResult = max.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                    Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));
            // Then
            assertThat((Map<String, byte[]>) mapByteArrayResult)
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));
        }
    }

    @Test
    public void shouldThrowOnDifferentOperandTypes() {
        // Given
        AggregationOp max = AggregationOp.MAX;

        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> {
            max.apply(5, "not an integer");
        }).withMessage("different operands, leftHandSide type: class java.lang.Integer rightHandSide type: class java.lang.String");
    }

    @Test
    public void shouldThrowOnIllegalType() {
        // Given
        AggregationOp max = AggregationOp.MAX;

        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> {
            max.apply(new Object(), new Object());
        }).withMessage("Value type not implemented class java.lang.Object");
    }

    @Test
    public void shouldThrowOnIllegalMapType() {
        // Given
        AggregationOp max = AggregationOp.MAX;

        // Then
        assertThatIllegalArgumentException().isThrownBy(() -> {
            max.apply(Map.of("key1", new Object()), Map.of("key2", new Object()));
        }).withMessage("Value type not implemented class java.lang.Object");
    }
}
