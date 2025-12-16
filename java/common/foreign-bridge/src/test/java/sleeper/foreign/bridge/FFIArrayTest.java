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
package sleeper.foreign.bridge;

import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatIndexOutOfBoundsException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FFIArrayTest {

    @Test
    void shouldPopulateAndReadBackWithIntegers() {
        Assertions.setMaxStackTraceElementsDisplayed(9999);
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        Integer[] input = {1, 2, 3, 4, 5, null};

        // When
        array.populate(input, true);
        array.validate();
        Integer[] output = (Integer[]) array.readBack(Integer.class, true);

        // Then
        assertArrayEquals(input, output);

        // When & Then (nulls not allowed should throw)
        struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array2 = new FFIArray<>(struct);
        assertThatNullPointerException()
                .isThrownBy(() -> array2.populate(input, false));
    }

    @Test
    void shouldPopulateAndReadBackWithLongs() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        Long[] input = {1L, Long.MAX_VALUE, Long.MIN_VALUE, null};

        // When
        array.populate(input, true);
        array.validate();
        Long[] output = (Long[]) array.readBack(Long.class, true);

        // Then
        assertArrayEquals(input, output);

        // When & Then
        struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array2 = new FFIArray<>(struct);
        assertThatNullPointerException()
                .isThrownBy(() -> array2.populate(input, false));
    }

    @Test
    void shouldPopulateAndReadBackWithBooleans() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        Boolean[] input = {true, false, null};

        // When
        array.populate(input, true);
        array.validate();
        Boolean[] output = (Boolean[]) array.readBack(Boolean.class, true);

        // Then
        assertArrayEquals(input, output);

        // When & Then
        struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array2 = new FFIArray<>(struct);
        assertThatNullPointerException()
                .isThrownBy(() -> array2.populate(input, false));
    }

    @Test
    void shouldPopulateAndReadBackWithStrings() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        String[] input = {"test", "", "string", null, "test string"};

        // When
        array.populate(input, true);
        array.validate();
        String[] output = (String[]) array.readBack(String.class, true);

        // Then
        assertArrayEquals(input, output);

        // When & Then
        struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array2 = new FFIArray<>(struct);
        assertThatNullPointerException()
                .isThrownBy(() -> array2.populate(input, false))
                .withMessageContaining("is null when nulls aren't allowed here");
    }

    @Test
    void shouldPopulateAndReadBackWithByteArrays() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        byte[] a = {1, 2, 3};
        byte[] b = {};
        byte[] c = {127, -128, 0};
        byte[][] input = {a, b, c, null};

        // When
        array.populate(input, true);
        array.validate();
        byte[][] output = (byte[][]) array.readBack(byte[].class, true);

        // Then
        assertEquals(input.length, output.length);
        for (int i = 0; i < input.length; i++) {
            assertArrayEquals(input[i], output[i]);
        }

        // When & Then
        struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array2 = new FFIArray<>(struct);
        assertThatNullPointerException()
                .isThrownBy(() -> array2.populate(input, false));
    }

    @Test
    void shouldPopulateZeroLengthArray() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        Integer[] input = {};

        // When
        array.populate(input, true);

        // Then
        assertEquals(0, array.length());

        // When
        array.validate();
        Integer[] output = (Integer[]) array.readBack(Integer.class, true);

        // Then
        assertEquals(0, output.length);
    }

    @Test
    void shouldThrowOnGetValueOnInvalidIndex() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        Integer[] input = {1, 2, 3};
        array.populate(input, true);

        // When & Then
        assertThatIndexOutOfBoundsException()
                .isThrownBy(() -> array.getValue(-1, Integer.class, true, Runtime.getSystemRuntime()));
        assertThatIndexOutOfBoundsException()
                .isThrownBy(() -> array.getValue(3, Integer.class, true, Runtime.getSystemRuntime()));
    }

    @Test
    void shouldThrowOnGetValueForNullInNonNullableArray() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        Integer[] input = {null};
        array.populate(input, true);

        // When & Then
        assertThatIllegalStateException()
                .isThrownBy(() -> array.getValue(0, Integer.class, false, Runtime.getSystemRuntime()))
                .withMessageContaining("Null found in non-nullable array");
    }

    @Test
    void shouldThrowOnSetAndGetUnsupportedTypeThrowsClassCastException() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);
        Double[] input = {1.1, 2.2};

        // When & Then
        assertThatExceptionOfType(ClassCastException.class)
                .isThrownBy(() -> array.populate(input, true))
                .withMessageContaining("Can't cast");
    }

    @Test
    void shouldReadBackOnUnpopulatedArray() {
        // Given
        Struct struct = new Struct(Runtime.getSystemRuntime()) {
        };
        FFIArray<Object> array = new FFIArray<>(struct);

        // When
        Object[] output = array.readBack(Integer.class, false);

        // Then
        assertThat(output).isEmpty();
    }
}
