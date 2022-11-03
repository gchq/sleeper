/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

public class UpdatePropertyTest extends AdminClientTestBase {

    private static final String UPDATE_PROPERTY_SCREEN = "\n" +
            "What is the PROPERTY NAME of the property that you would like to update?\n" +
            "\n" +
            "Please enter the PROPERTY NAME now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n";

    private static final String UPDATE_PROPERTY_ENTER_VALUE_SCREEN = "\n" +
            "What is the new PROPERTY VALUE?\n" +
            "\n" +
            "Please enter the PROPERTY VALUE now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n";

    @Test
    public void shouldExitWhenChosenOnUpdatePropertyScreen() throws IOException {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN);

        verify(in.mock, times(2)).promptLine(any());
        verifyNoMoreInteractions(in.mock);
        verifyNoInteractions(store);
    }

    @Test
    public void shouldUpdateInstancePropertyWhenNameAndValueEntered() throws IOException {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.retain.infra.after.destroy", "false", EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                + PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN);

        InOrder order = Mockito.inOrder(in.mock, store);
        order.verify(in.mock, times(3)).promptLine(any());
        order.verify(store).updateInstanceProperty(INSTANCE_ID, "sleeper.retain.infra.after.destroy", "false");
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
