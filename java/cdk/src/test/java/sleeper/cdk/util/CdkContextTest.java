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
package sleeper.cdk.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CdkContextTest {

    @Test
    void shouldReadTrueBoolean() {
        // Given
        CdkContext context = Map.of("bool", "true")::get;

        // When / Then
        assertThat(context.getBooleanOrDefault("bool", false))
                .isTrue();
    }

    @Test
    void shouldReadFalseBoolean() {
        // Given
        CdkContext context = Map.of("bool", "false")::get;

        // When / Then
        assertThat(context.getBooleanOrDefault("bool", true))
                .isFalse();
    }

    @Test
    void shouldDefaultBooleanToTrue() {
        // Given
        CdkContext context = Map.of("variable", "value")::get;

        // When / Then
        assertThat(context.getBooleanOrDefault("bool", true))
                .isTrue();
    }

    @Test
    void shouldDefaultBooleanToFalse() {
        // Given
        CdkContext context = Map.of("variable", "value")::get;

        // When / Then
        assertThat(context.getBooleanOrDefault("bool", false))
                .isFalse();
    }

    @Test
    void shouldReadTrueBooleanNonCaseSensitive() {
        // Given
        CdkContext context = Map.of("bool", "TrUe")::get;

        // When / Then
        assertThat(context.getBooleanOrDefault("bool", false))
                .isTrue();
    }

    @Test
    void shouldReadFalseBooleanNonCaseSensitive() {
        // Given
        CdkContext context = Map.of("bool", "FaLsE")::get;

        // When / Then
        assertThat(context.getBooleanOrDefault("bool", true))
                .isFalse();
    }

    @Test
    void shouldReadDefaultBooleanWithEmptyString() {
        // Given
        CdkContext context = Map.of("bool", "")::get;

        // When / Then
        assertThat(context.getBooleanOrDefault("bool", true))
                .isTrue();
    }

    @Test
    void shouldReadDefaultBooleanWithWhitespace() {
        // Given
        CdkContext context = Map.of("bool", " \t")::get;

        // When / Then
        assertThat(context.getBooleanOrDefault("bool", true))
                .isTrue();
    }

    @Test
    void shouldNotReadNonBooleanString() {
        // Given
        CdkContext context = Map.of("bool", "yes")::get;

        // When / Then
        assertThatThrownBy(() -> context.getBooleanOrDefault("bool", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected true or false for context variable: bool");
    }

}
