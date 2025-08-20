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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

public class AggregationOpTest {
    @SuppressWarnings("unchecked")
    @Test
    public void shouldApplySum() {
        // Given
        AggregationOp sum = AggregationOp.SUM;

        // When
        Object intResult = sum.apply(1, 2);
        Object longResult = sum.apply(1L, 2L);
        Object stringResult = sum.apply("one string", "two string");
        Object byteArrayResult = sum.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10});
        Map<String, Integer> mapIntResult = (Map<String, Integer>) sum.apply(Map.of("key1", 1, "key2", 3), Map.of("key2", 4, "key3", 6));
        Map<String, Long> mapLongResult = (Map<String, Long>) sum.apply(Map.of("key1", 1L, "key2", 3L), Map.of("key2", 4L, "key3", 6L));
        Map<String, String> mapStringResult = (Map<String, String>) sum.apply(Map.of("key1", "test", "key2", "one string"), Map.of("key2", "two string", "key3", "other"));
        Map<String, byte[]> mapByteArrayResult = (Map<String, byte[]>) sum.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));

        // Then
        assertThat(intResult).isEqualTo(3);
        assertThat(longResult).isEqualTo(3L);
        assertThat(stringResult).isEqualTo("one stringtwo string");
        assertThat(byteArrayResult).isEqualTo(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        assertThat(mapIntResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 7, "key3", 6));
        assertThat(mapLongResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 7L, "key3", 6L));
        assertThat(mapStringResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "one stringtwo string", "key3", "other"));
        assertThat(mapByteArrayResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4, 5, 6}, "key3", new byte[]{7, 8}));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldApplyMin() {
        // Given
        AggregationOp min = AggregationOp.MIN;

        // When
        Object intResult = min.apply(1, 2);
        Object longResult = min.apply(1L, 2L);
        Object stringResult = min.apply("one string", "two string");
        Object byteArrayResult = min.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10});
        Map<String, Integer> mapIntResult = (Map<String, Integer>) min.apply(Map.of("key1", 1, "key2", 3), Map.of("key2", 4, "key3", 6));
        Map<String, Long> mapLongResult = (Map<String, Long>) min.apply(Map.of("key1", 1L, "key2", 3L), Map.of("key2", 4L, "key3", 6L));
        Map<String, String> mapStringResult = (Map<String, String>) min.apply(Map.of("key1", "test", "key2", "one string"), Map.of("key2", "two string", "key3", "other"));
        Map<String, byte[]> mapByteArrayResult = (Map<String, byte[]>) min.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));

        // Then
        assertThat(intResult).isEqualTo(1);
        assertThat(longResult).isEqualTo(1L);
        assertThat(stringResult).isEqualTo("one string");
        assertThat(byteArrayResult).isEqualTo(new byte[]{1, 2, 3, 4, 5});
        assertThat(mapIntResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 3, "key3", 6));
        assertThat(mapLongResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 3L, "key3", 6L));
        assertThat(mapStringResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "one string", "key3", "other"));
        assertThat(mapByteArrayResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}, "key3", new byte[]{7, 8}));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldApplyMax() {
        // Given
        AggregationOp max = AggregationOp.MAX;

        // When
        Object intResult = max.apply(1, 2);
        Object longResult = max.apply(1L, 2L);
        Object stringResult = max.apply("one string", "two string");
        Object byteArrayResult = max.apply(new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7, 8, 9, 10});
        Map<String, Integer> mapIntResult = (Map<String, Integer>) max.apply(Map.of("key1", 1, "key2", 3), Map.of("key2", 4, "key3", 6));
        Map<String, Long> mapLongResult = (Map<String, Long>) max.apply(Map.of("key1", 1L, "key2", 3L), Map.of("key2", 4L, "key3", 6L));
        Map<String, String> mapStringResult = (Map<String, String>) max.apply(Map.of("key1", "test", "key2", "one string"), Map.of("key2", "two string", "key3", "other"));
        Map<String, byte[]> mapByteArrayResult = (Map<String, byte[]>) max.apply(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{3, 4}),
                Map.of("key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));

        // Then
        assertThat(intResult).isEqualTo(2);
        assertThat(longResult).isEqualTo(2L);
        assertThat(stringResult).isEqualTo("two string");
        assertThat(byteArrayResult).isEqualTo(new byte[]{6, 7, 8, 9, 10});
        assertThat(mapIntResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1, "key2", 4, "key3", 6));
        assertThat(mapLongResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", 1L, "key2", 4L, "key3", 6L));
        assertThat(mapStringResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", "test", "key2", "two string", "key3", "other"));
        assertThat(mapByteArrayResult).containsExactlyInAnyOrderEntriesOf(Map.of("key1", new byte[]{1, 2}, "key2", new byte[]{5, 6}, "key3", new byte[]{7, 8}));
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
