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

package sleeper.clients.admin.properties;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class UpdatePropertiesTextEditorCommandTest {

    @Test
    void shouldDefaultToNanoWhenEnvironmentVariableIsNull() {
        UpdatePropertiesTextEditorCommand config = withEnvironment(Map.of());

        assertThat(config.getCommand())
                .isEqualTo("nano");
    }

    @Test
    void shouldGetSpecifiedEditorWhenEnvironmentVariableIsSet() {
        UpdatePropertiesTextEditorCommand config = withEnvironment(Map.of("EDITOR", "vim"));

        assertThat(config.getCommand())
                .isEqualTo("vim");
    }

    @Test
    void shouldDefaultToNanoWhenEnvironmentVariableIsEmptyString() {
        UpdatePropertiesTextEditorCommand config = withEnvironment(Map.of("EDITOR", ""));

        assertThat(config.getCommand())
                .isEqualTo("nano");
    }

    @Test
    void shouldDefaultToNanoWhenEnvironmentVariableIsOnlyWhitespace() {
        UpdatePropertiesTextEditorCommand config = withEnvironment(Map.of("EDITOR", "  "));

        assertThat(config.getCommand())
                .isEqualTo("nano");
    }

    @Test
    void shouldDefaultToNanoWhenEnvironmentVariableCannotBeRead() {
        UpdatePropertiesTextEditorCommand config = new UpdatePropertiesTextEditorCommand(
                variable -> {
                    throw new IllegalStateException("Cannot read environment variable");
                });

        assertThat(config.getCommand())
                .isEqualTo("nano");
    }

    private UpdatePropertiesTextEditorCommand withEnvironment(Map<String, String> environmentVariables) {
        return new UpdatePropertiesTextEditorCommand(environmentVariables::get);
    }
}
