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
package sleeper.core.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JvmMemoryUseTest {

    @Test
    void shouldPrintMemoryWhenSomeIsUsedAndNoMaxKnown() {
        // When
        JvmMemoryUse memory = new JvmMemoryUse(100, 60, Long.MAX_VALUE);

        // Then
        assertThat(memory.isMaxMemoryKnown()).isFalse();
        assertThat(memory).hasToString("100 bytes allocated to JVM, 60 bytes free in JVM, further memory may be allocated, no known limit");
    }

    @Test
    void shouldPrintMemoryWhenMaxIsKnown() {
        // When
        JvmMemoryUse memory = new JvmMemoryUse(100, 60, 200);

        // Then
        assertThat(memory.isMaxMemoryKnown()).isTrue();
        assertThat(memory).hasToString("100 bytes allocated to JVM, 60 bytes free in JVM, maximum 200 bytes may be allocated to JVM, total available unused memory 160 bytes");
    }

    @Test
    void shouldPrintMemoryInOtherUnits() {
        // When
        JvmMemoryUse memory = new JvmMemoryUse(
                12 * 1024 * 1024,
                34 * 1024,
                56L * 1024 * 1024 * 1024);

        // Then
        assertThat(memory.isMaxMemoryKnown()).isTrue();
        assertThat(memory).hasToString("12 MB allocated to JVM, 34 KB free in JVM, maximum 56 GB may be allocated to JVM, total available unused memory 55 GB");
    }

}
