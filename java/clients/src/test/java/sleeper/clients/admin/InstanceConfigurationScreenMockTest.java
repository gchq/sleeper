/*
 * Copyright 2022-2026 Crown Copyright
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.admin.properties.PropertiesDiff;
import sleeper.clients.admin.properties.PropertiesEditor;
import sleeper.clients.admin.testutils.AdminClientTestBase;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.SaveChangesScreen;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.ValidateChangesScreen;
import sleeper.clients.admin.testutils.MockProperiesEditorTestHarness;
import sleeper.clients.admin.testutils.RunAdminClient;
import sleeper.common.task.QueueMessageCount;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertyGroup;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.CONFIGURATION_BY_GROUP_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.GROUP_SELECT_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_SAVE_CHANGES_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_VALIDATION_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TEST_TABLE_REPORT_LIST;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.instancePropertyGroupOption;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.tablePropertyGroupOption;
import static sleeper.clients.testutil.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.core.properties.table.TableProperty.STATESTORE_ASYNC_COMMITS_ENABLED;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class InstanceConfigurationScreenMockTest extends AdminClientTestBase {
    private final AdminClientPropertiesStore store = mock(AdminClientPropertiesStore.class);
    private final PropertiesEditor editor = mock(PropertiesEditor.class);
    private final TableIndex tableIndex = new InMemoryTableIndex();

    @DisplayName("Navigate from main screen and back")
    @Nested
    class NavigateFromMainScreen {

        @Test
        void shouldViewInstanceConfiguration() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();

            // When
            String output = viewInstanceConfiguration(properties).exitGetOutput();

            // Then
            assertThat(output).isEqualTo(DISPLAY_MAIN_SCREEN + DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(properties);
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        void shouldDiscardChangesToInstanceConfiguration() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

            // When
            String output = editInstanceConfiguration(before, after)
                    .enterPrompt(SaveChangesScreen.DISCARD_CHANGES_OPTION)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN + DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock, times(2)).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @ParameterizedTest(name = "With return to editor option \"{0}\"")
        @ValueSource(strings = {SaveChangesScreen.RETURN_TO_EDITOR_OPTION, ""})
        void shouldMakeChangesThenReturnToEditorAndRevertChanges(String returnToEditorOption) throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

            // When
            String output = editInstanceConfiguration(before, after) // Apply changes
                    .enterPrompt(returnToEditorOption)
                    .editAgain(after, before) // Revert changes
                    .exitGetOutput();

            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .containsOnlyOnce(PROPERTY_SAVE_CHANGES_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN + DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(after);
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @ParameterizedTest(name = "With return to editor option \"{0}\"")
        @ValueSource(strings = {ValidateChangesScreen.RETURN_TO_EDITOR_OPTION, ""})
        void shouldMakeInvalidChangesThenReturnToEditorAndRevertChanges(String returnToEditorOption) throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "abc");

            // When
            String output = editInstanceConfiguration(before, after) // Apply changes
                    .enterPrompt(returnToEditorOption)
                    .editAgain(after, before) // Revert changes
                    .exitGetOutput();

            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .doesNotContain(PROPERTY_SAVE_CHANGES_SCREEN)
                    .containsOnlyOnce(PROPERTY_VALIDATION_SCREEN)
                    .endsWith(PROPERTY_VALIDATION_SCREEN + DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(after);
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }
    }

    @DisplayName("Save changes")
    @Nested
    class SaveChanges {
        @Test
        void shouldSaveChangesWithStore() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

            // When
            String output = editInstanceConfiguration(before, after)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                            PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN +
                            DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock).promptLine(any());
            order.verify(store).saveInstanceProperties(after, new PropertiesDiff(before, after));
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        void shouldReturnToSaveChangesScreenWhenSavingFails() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");
            doThrow(new AdminClientPropertiesStore.CouldNotSaveInstanceProperties(before.get(ID),
                    new RuntimeException("Something went wrong")))
                    .when(store).saveInstanceProperties(after, new PropertiesDiff(before, after));

            // When
            String output = editInstanceConfiguration(before, after)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, SaveChangesScreen.DISCARD_CHANGES_OPTION)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                            "\n\n" +
                            "----------------------------------\n" +
                            "\n" +
                            "Could not save properties for instance " + instanceId + "\n" +
                            "Cause: Something went wrong\n" +
                            "\n" +
                            PROPERTY_SAVE_CHANGES_SCREEN +
                            DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock).promptLine(any());
            order.verify(store).saveInstanceProperties(after, new PropertiesDiff(before, after));
            order.verify(in.mock, times(2)).promptLine(any());
            order.verifyNoMoreInteractions();
        }
    }

    @DisplayName("Configure table properties")
    @Nested
    class ConfigureTableProperties {
        @Test
        void shouldEditAProperty() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties before = createValidTableProperties(properties);
            TableProperties after = TableProperties.copyOf(before);
            after.set(ITERATOR_CONFIG, "TestIterator");

            // When
            String output = editTableConfiguration(properties, before, after)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN + CLEAR_CONSOLE + "\n" +
                    TEST_TABLE_REPORT_LIST + TABLE_SELECT_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN + PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN + DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            // Mockito was confused that the instance properties are loaded here, needed to split the verify calls
            // See https://github.com/mockito/mockito/issues/2957
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock).promptLine(any());
            order.verify(store).saveTableProperties(properties, after);
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        void shouldReturnToSaveChangesScreenWhenSavingFails() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties before = createValidTableProperties(properties);
            TableProperties after = TableProperties.copyOf(before);
            after.set(ROW_GROUP_SIZE, "123");
            doThrow(new AdminClientPropertiesStore.CouldNotSaveTableProperties(properties.get(ID), TABLE_NAME_VALUE,
                    new RuntimeException("Something went wrong")))
                    .when(store).saveTableProperties(properties, after);

            // When
            String output = editTableConfiguration(properties, before, after)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, SaveChangesScreen.DISCARD_CHANGES_OPTION)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                            "\n\n" +
                            "----------------------------------\n" +
                            "\n" +
                            "Could not save properties for table test-table in instance " + instanceId + "\n" +
                            "Cause: Something went wrong\n" +
                            "\n" +
                            PROPERTY_SAVE_CHANGES_SCREEN +
                            DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            // Mockito was confused that the instance properties are loaded here, needed to split the verify calls
            // See https://github.com/mockito/mockito/issues/2957
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock).promptLine(any());
            order.verify(store).saveTableProperties(properties, after);
            order.verify(in.mock, times(2)).promptLine(any());
            order.verifyNoMoreInteractions();
        }
    }

    @DisplayName("Filter by group")
    @Nested
    class FilterByGroup {
        @Test
        void shouldViewPropertiesThatBelongToSpecificGroup() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();

            // When
            String output = runClient()
                    .enterPrompts(CONFIGURATION_BY_GROUP_OPTION,
                            instancePropertyGroupOption(InstancePropertyGroup.COMMON))
                    .viewInEditorFromStore(properties, InstancePropertyGroup.COMMON)
                    .exitGetOutput();

            // Then
            assertThat(output).isEqualTo(DISPLAY_MAIN_SCREEN +
                    CLEAR_CONSOLE + GROUP_SELECT_SCREEN + DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            // Mockito was confused that the instance properties are loaded here, needed to split the verify calls
            // See https://github.com/mockito/mockito/issues/2957
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(properties, InstancePropertyGroup.COMMON);
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        void shouldEditPropertiesThatBelongToSpecificGroup() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

            // When
            String output = runClient()
                    .enterPrompts(CONFIGURATION_BY_GROUP_OPTION,
                            instancePropertyGroupOption(InstancePropertyGroup.COMMON))
                    .editFromStore(before, after, InstancePropertyGroup.COMMON)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN + CLEAR_CONSOLE + GROUP_SELECT_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                            PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN +
                            DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            // Mockito was confused that the instance properties are loaded here, needed to split the verify calls
            // See https://github.com/mockito/mockito/issues/2957
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before, InstancePropertyGroup.COMMON);
            order.verify(in.mock).promptLine(any());
            order.verify(store).saveInstanceProperties(after, new PropertiesDiff(before, after));
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        void shouldEditTablePropertiesThatBelongToSpecificGroup() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties before = createValidTableProperties(properties);
            before.set(STATESTORE_ASYNC_COMMITS_ENABLED, "false");
            TableProperties after = TableProperties.copyOf(before);
            after.set(STATESTORE_ASYNC_COMMITS_ENABLED, "true");

            // When
            String output = runClient()
                    .enterPrompts(CONFIGURATION_BY_GROUP_OPTION,
                            tablePropertyGroupOption(TablePropertyGroup.METADATA),
                            before.get(TABLE_NAME))
                    .editFromStore(properties, before, after, TablePropertyGroup.METADATA)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN +
                    CLEAR_CONSOLE + GROUP_SELECT_SCREEN + CLEAR_CONSOLE + "\n" +
                    TEST_TABLE_REPORT_LIST + TABLE_SELECT_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                            PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN +
                            DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            // Mockito was confused that the instance properties are loaded here, needed to split the verify calls
            // See https://github.com/mockito/mockito/issues/2957
            order.verify(in.mock, times(2)).promptLine(any());
            order.verify(editor).openPropertiesFile(before, TablePropertyGroup.METADATA);
            order.verify(in.mock).promptLine(any());
            order.verify(store).saveTableProperties(properties, after);
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        void shouldExitWhenOnGroupSelectScreen() throws Exception {
            // Given
            setInstanceProperties(createValidInstanceProperties());

            // When
            String output = runClient()
                    .enterPrompts(CONFIGURATION_BY_GROUP_OPTION, EXIT_OPTION)
                    .exitGetOutput();

            // Then
            assertThat(output).isEqualTo(DISPLAY_MAIN_SCREEN +
                    CLEAR_CONSOLE + GROUP_SELECT_SCREEN);
            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock, times(2)).promptLine(any());
            order.verifyNoMoreInteractions();
        }
    }

    @Override
    protected RunAdminClient runClient() {
        return new RunAdminClient(out, in, this, new MockProperiesEditorTestHarness(editor));
    }

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        super.setInstanceProperties(instanceProperties);
        when(store.loadInstanceProperties(instanceProperties.get(ID))).thenReturn(instanceProperties);
    }

    @Override
    public void saveTableProperties(TableProperties tableProperties) {
        when(store.loadTableProperties(instanceProperties, tableProperties.get(TABLE_NAME)))
                .thenReturn(tableProperties);
        tableIndex.create(tableProperties.getStatus());
    }

    @Override
    public void startClient(AdminClientTrackerFactory trackers, QueueMessageCount.Client queueClient) throws Exception {
        new AdminClient(tableIndex, store, trackers,
                editor, out.consoleOut(), in.consoleIn(),
                queueClient, properties -> Collections.emptyMap())
                .start(instanceId);
    }

}
