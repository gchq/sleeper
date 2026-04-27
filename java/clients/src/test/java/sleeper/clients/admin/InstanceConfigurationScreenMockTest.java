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
import sleeper.clients.admin.properties.PropertiesEditor;
import sleeper.clients.admin.testutils.AdminClientTestBase;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.SaveChangesScreen;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.ValidateChangesScreen;
import sleeper.clients.admin.testutils.MockProperiesEditorTestHarness;
import sleeper.clients.admin.testutils.RunAdminClient;
import sleeper.common.task.QueueMessageCount;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_SAVE_CHANGES_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_VALIDATION_SCREEN;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
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
