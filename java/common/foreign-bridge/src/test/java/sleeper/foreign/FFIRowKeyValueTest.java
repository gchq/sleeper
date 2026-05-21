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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

public class FFIRowKeyValueTest {
    private jnr.ffi.Runtime runtime = jnr.ffi.Runtime.getSystemRuntime();

    @Test
    void shouldSetInteger() {
        // Given
        FFIRowKeyValue element = new FFIRowKeyValue(runtime);

        // When
        element.set(345);

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIRowKeyValueType.Int32);
        assertThat(element.get()).isEqualTo(345);
    }

    @Test
    void shouldSetLong() {
        // Given
        FFIRowKeyValue element = new FFIRowKeyValue(runtime);

        // When
        element.set(12345L);

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIRowKeyValueType.Int64);
        assertThat(element.get()).isEqualTo(12345L);
    }

    @Test
    void shouldSetString() {
        // Given
        FFIRowKeyValue element = new FFIRowKeyValue(runtime);

        // When
        element.set("test\0foo");

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIRowKeyValueType.String);
        assertThat(element.get()).isEqualTo("test\0foo");
    }

    @Test
    void shouldSetBytes() {
        // Given
        FFIRowKeyValue element = new FFIRowKeyValue(runtime);

        // When
        element.set(new byte[]{4, 5, 6, 7});

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIRowKeyValueType.ByteArray);
        assertThat(element.get()).isEqualTo(new byte[]{4, 5, 6, 7});
    }

    @Test
    void shouldSetEmpty() {
        // Given
        FFIRowKeyValue element = new FFIRowKeyValue(runtime);

        // When
        element.set(null);

        // Then
        assertThat(element.contained.get()).isEqualTo(FFIRowKeyValueType.Empty);
        assertNull(element.get());
    }
}
