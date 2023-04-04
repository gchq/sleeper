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

package sleeper.clients.admin;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.ToStringPrintStream;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.TestConsoleInput;

import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;
import static sleeper.clients.admin.AdminCommonPrompts.tryLoadInstanceProperties;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_RETURN_TO_MAIN;
import static sleeper.console.TestConsoleInput.CONFIRM_PROMPT;

public class AdminCommonPromptsTest {
    private final ToStringPrintStream out = new ToStringPrintStream();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());

    @DisplayName("Try load instance properties")
    @Nested
    class TryLoadInstanceProperties {
        private final Supplier<InstanceProperties> failToLoadProperties = () -> {
            throw new AdminClientPropertiesStore.CouldNotLoadInstanceProperties("test-instance",
                    new Exception("Source Exception"));
        };
        private final Supplier<InstanceProperties> successfullyLoadProperties = InstanceProperties::new;

        @Test
        void shouldPromptReturnToMenuWhenFailingToLoadInstanceProperties() {
            // Given
            in.enterNextPrompt(CONFIRM_PROMPT);

            // When
            Optional<InstanceProperties> properties = tryLoadInstanceProperties(out.consoleOut(), in.consoleIn(),
                    failToLoadProperties);

            // Then
            assertThat(properties).isEmpty();
            assertThat(out.toString()).isEqualTo("\n" +
                    "Could not load properties for instance test-instance\n" +
                    "Cause: Source Exception\n" +
                    PROMPT_RETURN_TO_MAIN);
        }

        @Test
        void shouldContinueWhenSuccessfullyLoadingInstanceProperties() {
            // Given / When
            Optional<InstanceProperties> properties = tryLoadInstanceProperties(out.consoleOut(), in.consoleIn(),
                    successfullyLoadProperties);

            // Then
            assertThat(properties).isPresent();
            assertThat(out.toString()).isEmpty();
        }
    }

    @Test
    void shouldPromptReturnToMain() {
        // Given / When
        in.enterNextPrompt(CONFIRM_PROMPT);
        confirmReturnToMainScreen(out.consoleOut(), in.consoleIn());

        // Then
        assertThat(out.toString()).isEqualTo(PROMPT_RETURN_TO_MAIN);
    }
}
