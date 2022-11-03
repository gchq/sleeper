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
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperty;

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

    private static final String UPDATE_PROPERTY_ENTER_TABLE_SCREEN = "\n" +
            "As the property name begins with sleeper.table we also need to know the TABLE you want to update\n" +
            "\n" +
            "Please enter the TABLE NAME now or use the following options:\n" +
            "[0] Exit program\n" +
            "[1] Return to Main Menu\n" +
            "\n";

    protected static final String PROMPT_RETURN_TO_PROPERTY = "" +
            "\n\n----------------------------------\n" +
            "Hit enter to return to the property screen so you can adjust the property and continue\n";

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
                + "sleeper.retain.infra.after.destroy has been updated to false\n"
                + PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN);

        InOrder order = Mockito.inOrder(in.mock, store);
        order.verify(in.mock, times(3)).promptLine(any());
        order.verify(store).updateInstanceProperty(INSTANCE_ID,
                UserDefinedInstanceProperty.RETAIN_INFRA_AFTER_DESTROY, "false");
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldUpdateTablePropertyWhenNameValueAndTableEntered() throws IOException {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.table.iterator.class.name", "SomeIteratorClass", "update-table", EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_TABLE_SCREEN
                + "sleeper.table.iterator.class.name has been updated to SomeIteratorClass\n"
                + PROMPT_RETURN_TO_MAIN + CLEAR_CONSOLE + MAIN_SCREEN);

        InOrder order = Mockito.inOrder(in.mock, store);
        order.verify(in.mock, times(4)).promptLine(any());
        order.verify(store).updateTableProperty(INSTANCE_ID, "update-table",
                TableProperty.ITERATOR_CLASS_NAME, "SomeIteratorClass");
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldRefuseUpdatingInstancePropertyWithInvalidValue() throws IOException {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.retain.infra.after.destroy", "ABC", EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                + "Sleeper property sleeper.retain.infra.after.destroy is invalid\n"
                + PROMPT_RETURN_TO_PROPERTY
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN);

        InOrder order = Mockito.inOrder(in.mock, store);
        order.verify(in.mock, times(3)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldRefuseUpdatingUneditableInstanceProperty() throws IOException {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.config.bucket", "some-bucket", EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                + "Sleeper property sleeper.config.bucket does not exist and cannot be updated\n"
                + PROMPT_RETURN_TO_PROPERTY
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN);

        InOrder order = Mockito.inOrder(in.mock, store);
        order.verify(in.mock, times(3)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldRefuseUpdatingTablePropertyWithInvalidValue() throws IOException {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.table.fs.s3a.readahead.range", "ABC", "update-table", EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_TABLE_SCREEN
                + "Sleeper property sleeper.table.fs.s3a.readahead.range is invalid\n"
                + PROMPT_RETURN_TO_PROPERTY
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN);

        InOrder order = Mockito.inOrder(in.mock, store);
        order.verify(in.mock, times(4)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldRefuseUpdatingNonExistingTableProperty() throws IOException {
        // Given
        in.enterNextPrompts(UPDATE_PROPERTY_OPTION, "sleeper.table.abc", "def", "update-table", EXIT_OPTION);

        // When
        String output = runClientGetOutput();

        // Then
        assertThat(output).isEqualTo(CLEAR_CONSOLE + MAIN_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_VALUE_SCREEN
                + CLEAR_CONSOLE + UPDATE_PROPERTY_ENTER_TABLE_SCREEN
                + "Sleeper property sleeper.table.abc does not exist and cannot be updated\n"
                + PROMPT_RETURN_TO_PROPERTY
                + CLEAR_CONSOLE + UPDATE_PROPERTY_SCREEN);

        InOrder order = Mockito.inOrder(in.mock, store);
        order.verify(in.mock, times(4)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
