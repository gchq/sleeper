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

package sleeper.core.properties.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SleeperPropertyValueUtilsTest {

    @Nested
    @DisplayName("Validate numbers")
    class ValidateNumbers {
        @Test
        void shouldNotThrowExceptionDuringPositiveIntegerCheck() {
            // When/Then
            assertThat(SleeperPropertyValueUtils.isPositiveInteger("123"))
                    .isTrue();
            assertThat(SleeperPropertyValueUtils.isPositiveInteger("ABC"))
                    .isFalse();
        }

        @Test
        void shouldNotThrowExceptionDuringPositiveLongCheck() {
            // When/Then
            assertThat(SleeperPropertyValueUtils.isPositiveLong("123"))
                    .isTrue();
            assertThat(SleeperPropertyValueUtils.isPositiveLong("ABC"))
                    .isFalse();
        }

        @Test
        void shouldNotThrowExceptionDuringPositiveDoubleCheck() {
            // When/Then
            assertThat(SleeperPropertyValueUtils.isPositiveDouble("123"))
                    .isTrue();
            assertThat(SleeperPropertyValueUtils.isPositiveDouble("ABC"))
                    .isFalse();
        }

        @Test
        void shouldValidAllCorrectVariantsForIsPositiveIntegerOrNull() {
            // When/Then
            assertThat(SleeperPropertyValueUtils.isPositiveIntegerOrNull(null))
                    .isTrue();
            assertThat(SleeperPropertyValueUtils.isPositiveIntegerOrNull("24"))
                    .isTrue();
            assertThat(SleeperPropertyValueUtils.isPositiveIntegerOrNull("-7"))
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("Validate bytes size")
    class ValidateBytesSize {
        @Test
        void shouldReadBytesSize() {
            assertThat(SleeperPropertyValueUtils.readBytes("42")).isEqualTo(42L);
        }

        @Test
        void shouldReadKilobytesSize() {
            assertThat(SleeperPropertyValueUtils.readBytes("2K")).isEqualTo(2048L);
        }

        @Test
        void shouldReadMegabytesSize() {
            assertThat(SleeperPropertyValueUtils.readBytes("2M")).isEqualTo(2 * 1024 * 1024L);
        }

        @Test
        void shouldReadGigabytesSize() {
            assertThat(SleeperPropertyValueUtils.readBytes("2G")).isEqualTo(2 * 1024 * 1024 * 1024L);
        }

        @Test
        void shouldReadTerabytesSize() {
            assertThat(SleeperPropertyValueUtils.readBytes("2T")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L);
        }

        @Test
        void shouldReadPetabytesSize() {
            assertThat(SleeperPropertyValueUtils.readBytes("2P")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L * 1024L);
        }

        @Test
        void shouldReadExabytesSize() {
            assertThat(SleeperPropertyValueUtils.readBytes("2E")).isEqualTo(2 * 1024 * 1024 * 1024L * 1024L * 1024L * 1024L);
        }
    }

    @Nested
    @DisplayName("Validate strings")
    class ValidateStrings {
        @Test
        void shouldFailToValidateNullString() {
            assertThat(SleeperPropertyValueUtils.isNonNullNonEmptyString(null)).isFalse();
        }

        @Test
        void shouldFailToValidateEmptyString() {
            assertThat(SleeperPropertyValueUtils.isNonNullNonEmptyString("")).isFalse();
        }

        @Test
        void shouldValidateNonNullNonEmptyString() {
            assertThat(SleeperPropertyValueUtils.isNonNullNonEmptyString("test")).isTrue();
        }

        @Test
        void shouldValidateStringWhenStringLengthIsLowerThanMaxLength() {
            assertThat(SleeperPropertyValueUtils.isNonNullNonEmptyStringWithMaxLength("test", 5)).isTrue();
        }

        @Test
        void shouldFailToValidateWhenStringLengthExceedsMaxLength() {
            assertThat(SleeperPropertyValueUtils.isNonNullNonEmptyStringWithMaxLength("test", 1)).isFalse();
        }

        @Test
        void shouldValidateStringWhenStringLengthMeetsMaxLength() {
            assertThat(SleeperPropertyValueUtils.isNonNullNonEmptyStringWithMaxLength("test", 4)).isTrue();
        }
    }

    @Nested
    @DisplayName("Validate lists")
    class ValidateLists {
        @Test
        void shouldValidateNullStringAsEmptyList() {
            assertThat(SleeperPropertyValueUtils.isListWithMaxSize(null, 4)).isTrue();
        }

        @Test
        void shouldValidateEmptyString() {
            assertThat(SleeperPropertyValueUtils.isListWithMaxSize("", 4)).isTrue();
        }

        @Test
        void shouldValidateStringWhenListSizeIsLowerThanMaxSize() {
            assertThat(SleeperPropertyValueUtils.isListWithMaxSize("a,b", 4)).isTrue();
        }

        @Test
        void shouldValidateStringWhenListSizeMeetsMaxSize() {
            assertThat(SleeperPropertyValueUtils.isListWithMaxSize("a,b,c,d", 4)).isTrue();
        }

        @Test
        void shouldFailToValidateStringWhenListSizeExceedsMaxSize() {
            assertThat(SleeperPropertyValueUtils.isListWithMaxSize("a,b,c,d,e", 4)).isFalse();
        }

        @Test
        void shouldValidateEmptyListToBeInKeyValueFormat() {
            assertThat(SleeperPropertyValueUtils.isListInKeyValueFormat("")).isTrue();
        }

        @Test
        void shouldValidateListInKeyValueFormat() {
            assertThat(SleeperPropertyValueUtils.isListInKeyValueFormat("key,value,key,value")).isTrue();
        }

        @Test
        void shouldFailToValidateListNotInKeyValueFormat() {
            assertThat(SleeperPropertyValueUtils.isListInKeyValueFormat("key=value")).isFalse();
        }
    }

}
