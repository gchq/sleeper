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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class FFIElementDataTest {
    private jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getSystemRuntime();

    @Test
    void shouldSetInt() {
        // Given
        FFIElementData data = new FFIElementData(runtime);

        // When
        data.set(345678);

        // Then
        assertThat(data.int32.get()).isEqualTo(345678);
    }

    @Test
    void shouldSetLong() {
        // Given
        FFIElementData data = new FFIElementData(runtime);

        // When
        data.set(987654L);

        // Then
        assertThat(data.int64.get()).isEqualTo(987654L);
    }

    @Test
    void shouldSetString() {
        // Given
        FFIElementData data = new FFIElementData(runtime);

        // When
        data.set("hello test test\0test");

        // Then
        assertThat(data.java_holder.getData()).isEqualTo("hello test test\0test".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSetBytes() {
        // Given
        FFIElementData data = new FFIElementData(runtime);

        // When
        data.set(new byte[]{1, 2, 3, 4, 5, 0, 9, 8, 7, 6});

        // Then
        assertThat(data.java_holder.getData()).isEqualTo(new byte[]{1, 2, 3, 4, 5, 0, 9, 8, 7, 6});
    }
}
