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

package sleeper.core.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.util.NumberFormatUtils.formatBytes;

public class NumberFormatUtilsTest {
    @Nested
    @DisplayName("Format bytes as string")
    class FormatBytesAsString {
        @Test
        void shouldFormatNumberOfBytesBelow1KB() {
            assertThat(formatBytes(123L))
                    .isEqualTo("123B");
        }

        @Test
        void shouldFormatNumberOfBytesAsKB() {
            assertThat(formatBytes(1_234L))
                    .isEqualTo("1.2KB");
        }

        @Test
        void shouldFormatNumberOfBytesAsKBWithRounding() {
            assertThat(formatBytes(5_678L))
                    .isEqualTo("5.7KB");
        }

        @Test
        void shouldFormatNumberOfBytesEqualTo1KB() {
            assertThat(formatBytes(1_000L))
                    .isEqualTo("1.0KB");
        }

        @Test
        void shouldFormatNumberOfBytesAs10KB() {
            assertThat(formatBytes(10_000L))
                    .isEqualTo("10.0KB");
        }

        @Test
        void shouldFormatNumberOfBytesAsMB() {
            assertThat(formatBytes(1_234_000L))
                    .isEqualTo("1.2MB");
        }

        @Test
        void shouldFormatNumberOfBytesAsGB() {
            assertThat(formatBytes(1_234_000_000L))
                    .isEqualTo("1.2GB");
        }

        @Test
        void shouldFormatNumberOfBytesAbove1TB() {
            assertThat(formatBytes(1_234_000_000_000L))
                    .isEqualTo("1TB");
        }

        @Test
        void shouldFormatNumberOfBytesAbove1000TB() {
            assertThat(formatBytes(1_234_000_000_000_000L))
                    .isEqualTo("1,234TB");
        }
    }
}
