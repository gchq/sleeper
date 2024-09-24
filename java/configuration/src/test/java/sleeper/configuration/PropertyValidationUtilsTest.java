/*
 * Copyright 2022-2024 Crown Copyright
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

class PropertyValidationUtilsTest {

    @Nested
    @DisplayName("Validate numbers")
    class ValidateNumbers {
        @Test
        void shouldNotThrowExceptionDuringPositiveIntegerCheck() {
            // When/Then
            assertThat(PropertyValidationUtils.isPositiveInteger("123"))
                    .isTrue();
            assertThat(PropertyValidationUtils.isPositiveInteger("ABC"))
                    .isFalse();
        }

        @Test
        void shouldNotThrowExceptionDuringPositiveLongCheck() {
            // When/Then
            assertThat(PropertyValidationUtils.isPositiveLong("123"))
                    .isTrue();
            assertThat(PropertyValidationUtils.isPositiveLong("ABC"))
                    .isFalse();
        }

        @Test
        void shouldNotThrowExceptionDuringPositiveDoubleCheck() {
            // When/Then
            assertThat(PropertyValidationUtils.isPositiveDouble("123"))
                    .isTrue();
            assertThat(PropertyValidationUtils.isPositiveDouble("ABC"))
                    .isFalse();
        }

        @Test
        void shouldValidAllCorrectVariantsForIsPositiveIntegerOrNull() {
            // When/Then
            assertThat(PropertyValidationUtils.isPositiveIntegerOrNull(null))
                    .isTrue();
            assertThat(PropertyValidationUtils.isPositiveIntegerOrNull("24"))
                    .isTrue();
            assertThat(PropertyValidationUtils.isPositiveIntegerOrNull("-7"))
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("Validate bytes size")
    class ValidateBytesSize {
        @Test
        void shouldReadBytesSize() {
            assertThat(PropertyValidationUtils.readBytes("42")).isEqualTo(42L);
        }

        @Test
        void shouldReadKilobytesSize() {
            assertThat(PropertyValidationUtils.readBytes("2K")).isEqualTo(2048L);
        }

        @Test
        void shouldReadMegabytesSize() {
            assertThat(PropertyValidationUtils.readBytes("2M")).isEqualTo(2 * 1024 * 1024L);
        }

        @Test
        void shouldReadGigabytesSize() {
            assertThat(PropertyValidationUtils.readBytes("2G")).isEqualTo(2 * 1024 * 1024 * 1024L);
        }

        @Test
        void shouldReadTerabytesSize() {
            assertThat(PropertyValidationUtils.readBytes("2T")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L);
        }

        @Test
        void shouldReadPetabytesSize() {
            assertThat(PropertyValidationUtils.readBytes("2P")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L * 1024L);
        }

        @Test
        void shouldReadExabytesSize() {
            assertThat(PropertyValidationUtils.readBytes("2E")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L * 1024L * 1024L);
        }
    }

    @Nested
    @DisplayName("Validate strings")
    class ValidateStrings {
        @Test
        void shouldFailToValidateNullString() {
            assertThat(PropertyValidationUtils.isNonNullNonEmptyString(null)).isFalse();
        }

        @Test
        void shouldFailToValidateEmptyString() {
            assertThat(PropertyValidationUtils.isNonNullNonEmptyString("")).isFalse();
        }

        @Test
        void shouldValidateNonNullNonEmptyString() {
            assertThat(PropertyValidationUtils.isNonNullNonEmptyString("test")).isTrue();
        }

        @Test
        void shouldValidateStringWhenStringLengthIsLowerThanMaxLength() {
            assertThat(PropertyValidationUtils.isNonNullNonEmptyStringWithMaxLength("test", 5)).isTrue();
        }

        @Test
        void shouldFailToValidateWhenStringLengthExceedsMaxLength() {
            assertThat(PropertyValidationUtils.isNonNullNonEmptyStringWithMaxLength("test", 1)).isFalse();
        }

        @Test
        void shouldValidateStringWhenStringLengthMeetsMaxLength() {
            assertThat(PropertyValidationUtils.isNonNullNonEmptyStringWithMaxLength("test", 4)).isTrue();
        }
    }

    @Nested
    @DisplayName("Validate lists")
    class ValidateLists {
        @Test
        void shouldValidateNullStringAsEmptyList() {
            assertThat(PropertyValidationUtils.isListWithMaxSize(null, 4)).isTrue();
        }

        @Test
        void shouldValidateEmptyString() {
            assertThat(PropertyValidationUtils.isListWithMaxSize("", 4)).isTrue();
        }

        @Test
        void shouldValidateStringWhenListSizeIsLowerThanMaxSize() {
            assertThat(PropertyValidationUtils.isListWithMaxSize("a,b", 4)).isTrue();
        }

        @Test
        void shouldValidateStringWhenListSizeMeetsMaxSize() {
            assertThat(PropertyValidationUtils.isListWithMaxSize("a,b,c,d", 4)).isTrue();
        }

        @Test
        void shouldFailToValidateStringWhenListSizeExceedsMaxSize() {
            assertThat(PropertyValidationUtils.isListWithMaxSize("a,b,c,d,e", 4)).isFalse();
        }
    }

}
