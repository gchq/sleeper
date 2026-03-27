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
package sleeper.foreign;

import jnr.ffi.Pointer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class FFIBytesTest {
    private static final jnr.ffi.Runtime RUNTIME = jnr.ffi.Runtime.getSystemRuntime();

    @Test
    void shouldThrowOnNullBytes() {
        // When - Then
        assertThatNullPointerException().isThrownBy(() -> new FFIBytes(RUNTIME, null)).withMessage("data");
    }

    @Test
    void shouldAcceptAndGetZeroBytes() {
        // Given
        FFIBytes zero = new FFIBytes(RUNTIME, new byte[0]);

        // When
        byte[] returned = zero.getData();

        // Then
        assertThat(returned).hasSize(0);
    }

    @Test
    void shouldAcceptAndGetSomeBytes() {
        // Given
        byte[] expected = {0, 1, 2, 3, 4, 5, 6};
        FFIBytes data = new FFIBytes(RUNTIME, expected);

        // When
        byte[] returned = data.getData();

        // Then
        assertThat(returned).isEqualTo(expected);
    }

    @Test
    void shouldWriteToReadFromMemoryZeroBytes() {
        // Given
        FFIBytes zero = new FFIBytes(RUNTIME, new byte[0]);
        Pointer memoryBuffer = RUNTIME.getMemoryManager().allocateTemporary(FFIBytes.size(RUNTIME), true);
        zero.writeTo(memoryBuffer);

        // When
        FFIBytes returned = FFIBytes.readFrom(memoryBuffer);

        // Then
        assertThat(zero).isEqualTo(returned);
    }

    @Test
    void shouldWriteToReadFromMemoryManyBytes() {
        // Given
        FFIBytes data = new FFIBytes(RUNTIME, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        Pointer memoryBuffer = RUNTIME.getMemoryManager().allocateTemporary(FFIBytes.size(RUNTIME), true);
        data.writeTo(memoryBuffer);

        // When
        FFIBytes returned = FFIBytes.readFrom(memoryBuffer);

        // Then
        assertThat(data).isEqualTo(returned);
    }
}
