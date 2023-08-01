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

package sleeper.configuration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UtilsTest {

    @Nested
    @DisplayName("Validate lists")
    class ValidateLists {
        @Test
        void shouldValidateListWithUniqueElements() {
            assertThat(Utils.isUniqueList("test-a,test-b,test-c"))
                    .isTrue();
        }

        @Test
        void shouldFailToValidateListWithDuplicates() {
            assertThat(Utils.isUniqueList("test-a,test-b,test-a"))
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("Validate numbers")
    class ValidateNumbers {
        @Test
        void shouldNotThrowExceptionDuringPositiveIntegerCheck() {
            // When/Then
            assertThat(Utils.isPositiveInteger("123"))
                    .isTrue();
            assertThat(Utils.isPositiveInteger("ABC"))
                    .isFalse();
        }

        @Test
        void shouldNotThrowExceptionDuringPositiveLongCheck() {
            // When/Then
            assertThat(Utils.isPositiveLong("123"))
                    .isTrue();
            assertThat(Utils.isPositiveLong("ABC"))
                    .isFalse();
        }

        @Test
        void shouldNotThrowExceptionDuringPositiveDoubleCheck() {
            // When/Then
            assertThat(Utils.isPositiveDouble("123"))
                    .isTrue();
            assertThat(Utils.isPositiveDouble("ABC"))
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("Validate bytes size")
    class ValidateBytesSize {
        @Test
        void shouldReadBytesSize() {
            assertThat(Utils.readBytes("42")).isEqualTo(42L);
        }

        @Test
        void shouldReadKilobytesSize() {
            assertThat(Utils.readBytes("2K")).isEqualTo(2048L);
        }

        @Test
        void shouldReadMegabytesSize() {
            assertThat(Utils.readBytes("2M")).isEqualTo(2 * 1024 * 1024L);
        }

        @Test
        void shouldReadGigabytesSize() {
            assertThat(Utils.readBytes("2G")).isEqualTo(2 * 1024 * 1024 * 1024L);
        }

        @Test
        void shouldReadTerabytesSize() {
            assertThat(Utils.readBytes("2T")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L);
        }

        @Test
        void shouldReadPetabytesSize() {
            assertThat(Utils.readBytes("2P")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L * 1024L);
        }

        @Test
        void shouldReadExabytesSize() {
            assertThat(Utils.readBytes("2E")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L * 1024L * 1024L);
        }
    }
}
