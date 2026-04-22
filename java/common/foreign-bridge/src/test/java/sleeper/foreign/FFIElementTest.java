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

public class FFIElementTest {
    private jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getSystemRuntime();

    @Test
    void shouldSetInteger() {
        // Given
        FFIElement element = new FFIElement(runtime);

        // When
        element.set(345);

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIElementType.Int32);
        assertThat(element.java_item.int32.get()).isEqualTo(345);
    }

    @Test
    void shouldSetLong() {
        // Given
        FFIElement element = new FFIElement(runtime);

        // When
        element.set(12345L);

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIElementType.Int64);
        assertThat(element.java_item.int64.get()).isEqualTo(12345L);
    }

    @Test
    void shouldSetString() {
        // Given
        FFIElement element = new FFIElement(runtime);

        // When
        element.set("test\0foo");

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIElementType.String);
        assertThat(element.java_item.java_holder.getData()).isEqualTo("test\0foo".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void shouldSetBytes() {
        // Given
        FFIElement element = new FFIElement(runtime);

        // When
        element.set(new byte[]{4, 5, 6, 7});

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIElementType.ByteArray);
        assertThat(element.java_item.java_holder.getData()).isEqualTo(new byte[]{4, 5, 6, 7});
    }

    @Test
    void shouldSetEmpty() {
        // Given
        FFIElement element = new FFIElement(runtime);

        // When
        element.set(null);

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIElementType.Empty);
    }
}
