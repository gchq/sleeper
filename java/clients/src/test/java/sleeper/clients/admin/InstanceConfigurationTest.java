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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.noChanges;
import static sleeper.clients.admin.UpdatePropertiesRequestTestHelper.withChanges;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.EXIT_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_CONFIGURATION_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_SAVE_CHANGES_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_VALIDATION_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.SaveChangesScreen;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_CONFIGURATION_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.ValidateChangesScreen;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_PAGE_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.DEFAULT_S3A_READAHEAD_RANGE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.console.ConsoleOutput.CLEAR_CONSOLE;

class InstanceConfigurationTest extends AdminClientMockStoreBase {

    @DisplayName("Navigate from main screen and back")
    @Nested
    class NavigateFromMainScreen {

        @Test
        void shouldViewInstanceConfiguration() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            setInstanceProperties(properties);
            in.enterNextPrompts(INSTANCE_CONFIGURATION_OPTION, EXIT_OPTION);
            when(editor.openPropertiesFile(properties))
                    .thenReturn(noChanges(properties));

            // When
            String output = runClientGetOutput();

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
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

            // When
            String output = updatePropertiesDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN + DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock, times(2)).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @ParameterizedTest(name = "With option of \"{0}\"")
        @ValueSource(strings = {SaveChangesScreen.RETURN_TO_EDITOR_OPTION, ""})
        void shouldMakeChangesThenReturnToEditorAndRevertChanges(String option) throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

            when(editor.openPropertiesFile(before))
                    .thenReturn(withChanges(before, after)); // Apply changes
            when(editor.openPropertiesFile(after))
                    .thenReturn(withChanges(after, before)); // Revert changes

            // When
            String output = updatePropertiesAndExitGetOutput(before, after, option);

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
    }

    @DisplayName("Display changes to edited properties")
    @Nested
    class DisplayChanges {

        @Test
        void shouldEditAProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(MAXIMUM_CONNECTIONS_TO_S3, "123");
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

            // When
            String output = updatePropertiesDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithSaveChangesDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.s3.max-connections\n" +
                    "Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration.\n" +
                    "Before: 123\n" +
                    "After: 456\n" +
                    "\n"));
        }

        @Test
        void shouldSetADefaultedProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "123");

            // When
            String output = updatePropertiesDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithSaveChangesDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.s3.max-connections\n" +
                    "Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration.\n" +
                    "Unset before, default value: 25\n" +
                    "After: 123\n" +
                    "\n"));
        }

        @Test
        void shouldSetAnUnknownProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.loadFromString("unknown.property=abc");

            // When
            String output = updatePropertiesDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithSaveChangesDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "unknown.property\n" +
                    "Unknown property, no description available\n" +
                    "Unset before\n" +
                    "After: abc\n" +
                    "\n"));
        }

        @Test
        void shouldEditPropertyWithLongDescription() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.set(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, "123");

            // When
            String output = updatePropertiesDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithSaveChangesDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.ingest.partition.refresh.period\n" +
                    "The frequency in seconds with which ingest tasks refresh their view of the partitions.\n" +
                    "(NB Refreshes only happen once a batch of data has been written so this is a lower bound on the\n" +
                    "refresh frequency.)\n" +
                    "Unset before, default value: 120\n" +
                    "After: 123\n" +
                    "\n"));
        }

        @Test
        void shouldOrderKnownPropertiesInTheOrderTheyAreDefinedInTheirGroups() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.set(OPTIONAL_STACKS, "CompactionStack");
            after.set(DEFAULT_S3A_READAHEAD_RANGE, "123");
            after.set(DEFAULT_PAGE_SIZE, "456");

            // When
            String output = updatePropertiesDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).containsSubsequence(
                    "sleeper.optional.stacks",
                    "sleeper.default.fs.s3a.readahead.range",
                    "sleeper.default.page.size");
        }

        @Test
        void shouldOrderUnknownPropertiesAfterKnownProperties() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.set(OPTIONAL_STACKS, "CompactionStack");
            after.loadFromString("" +
                    "some.unknown.property=a-value\n" +
                    "an.unknown.property=other-value");

            // When
            String output = updatePropertiesDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).containsSubsequence(
                    "sleeper.optional.stacks",
                    "an.unknown.property",
                    "some.unknown.property");
        }
    }

    @DisplayName("Display validation failures")
    @Nested
    class DisplayValidationFailures {
        @Test
        void shouldShowValidationFailure() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "abc");

            // When
            String output = updateInvalidPropertiesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.s3.max-connections\n" +
                    "Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration.\n" +
                    "Unset before, default value: 25\n" +
                    "After (not valid, please change): abc\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.s3.max-connections\n" +
                    "\n"));
        }

        @Test
        void shouldShowValidationScreen() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "abc");

            // When
            String output = updateInvalidPropertiesGetOutput(before, after);

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_VALIDATION_SCREEN + DISPLAY_MAIN_SCREEN);
        }

        @Test
        void shouldShowValidationFailuresForMultipleProperties() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "abc");
            after.set(DEFAULT_S3A_READAHEAD_RANGE, "def");

            // When
            String output = updateInvalidPropertiesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.s3.max-connections\n" +
                    "Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration.\n" +
                    "Unset before, default value: 25\n" +
                    "After (not valid, please change): abc\n" +
                    "\n" +
                    "sleeper.default.fs.s3a.readahead.range\n" +
                    "The readahead range set on the Hadoop configuration when reading Parquet files in a query\n" +
                    "(see https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html).\n" +
                    "Unset before, default value: 64K\n" +
                    "After (not valid, please change): def\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.s3.max-connections\n" +
                    "sleeper.default.fs.s3a.readahead.range\n" +
                    "\n"));
        }

        @Test
        void shouldRejectAChangeToAnUneditableProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(VPC_ID, "before-vpc");
            InstanceProperties after = createValidInstanceProperties();
            after.set(VPC_ID, "after-vpc");

            // When
            String output = updateInvalidPropertiesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.vpc\n" +
                    "The id of the VPC to deploy to.\n" +
                    "Before: before-vpc\n" +
                    "After (cannot be changed, please undo): after-vpc\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.vpc\n" +
                    "\n"));
        }

        @Test
        void shouldRejectAChangeToAnUneditablePropertyAndAnInvalidProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(VPC_ID, "before-vpc");
            InstanceProperties after = createValidInstanceProperties();
            after.set(VPC_ID, "after-vpc");
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "abc");

            // When
            String output = updateInvalidPropertiesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.vpc\n" +
                    "The id of the VPC to deploy to.\n" +
                    "Before: before-vpc\n" +
                    "After (cannot be changed, please undo): after-vpc\n" +
                    "\n" +
                    "sleeper.s3.max-connections\n" +
                    "Used to set the value of fs.s3a.connection.maximum on the Hadoop configuration.\n" +
                    "Unset before, default value: 25\n" +
                    "After (not valid, please change): abc\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.vpc\n" +
                    "sleeper.s3.max-connections\n" +
                    "\n"));
        }

        @Test
        void shouldRejectAChangeToASystemDefinedProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = createValidInstanceProperties();
            after.set(CONFIG_BUCKET, "changed-bucket");

            // When
            String output = updateInvalidPropertiesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.config.bucket\n" +
                    "The S3 bucket name used to store configuration files.\n" +
                    "Before: sleeper-test-instance-config\n" +
                    "After (cannot be changed, please undo): changed-bucket\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.config.bucket\n" +
                    "\n"));
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
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");

            // When
            String output = updatePropertiesAndSaveGetOutput(before, after);

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
            InstanceProperties after = createValidInstanceProperties();
            after.set(MAXIMUM_CONNECTIONS_TO_S3, "456");
            doThrow(new AdminConfigStore.CouldNotSaveInstanceProperties(INSTANCE_ID,
                    new RuntimeException("Something went wrong")))
                    .when(store).saveInstanceProperties(after, new PropertiesDiff(before, after));

            setInstanceProperties(before);
            in.enterNextPrompts(
                    INSTANCE_CONFIGURATION_OPTION,
                    SaveChangesScreen.SAVE_CHANGES_OPTION,
                    SaveChangesScreen.DISCARD_CHANGES_OPTION,
                    EXIT_OPTION);
            when(editor.openPropertiesFile(before))
                    .thenReturn(withChanges(before, after));

            // When
            String output = runClientGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                            "\n\n" +
                            "----------------------------------\n" +
                            "\n" +
                            "Could not save properties for instance test-instance\n" +
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
            TableProperties after = createValidTableProperties(properties);
            after.set(ROW_GROUP_SIZE, "123");

            // When
            String output = updateTablePropertiesAndExitGetOutput(properties, before, after,
                    SaveChangesScreen.SAVE_CHANGES_OPTION);

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN + CLEAR_CONSOLE + TABLE_SELECT_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN + PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN + DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock, times(2)).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock).promptLine(any());
            order.verify(store).saveTableProperties(INSTANCE_ID, after, new PropertiesDiff(before, after));
            order.verify(in.mock).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        void shouldReturnToSaveChangesScreenWhenSavingFails() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties before = createValidTableProperties(properties);
            TableProperties after = createValidTableProperties(properties);
            after.set(ROW_GROUP_SIZE, "123");
            doThrow(new AdminConfigStore.CouldNotSaveTableProperties(INSTANCE_ID, TABLE_NAME_VALUE,
                    new RuntimeException("Something went wrong")))
                    .when(store).saveTableProperties(INSTANCE_ID, after, new PropertiesDiff(before, after));

            // When
            String output = updateTablePropertiesAndExitGetOutput(properties, before, after,
                    SaveChangesScreen.SAVE_CHANGES_OPTION, SaveChangesScreen.DISCARD_CHANGES_OPTION);

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN +
                            "\n\n" +
                            "----------------------------------\n" +
                            "\n" +
                            "Could not save properties for table test-table in instance test-instance\n" +
                            "Cause: Something went wrong\n" +
                            "\n" +
                            PROPERTY_SAVE_CHANGES_SCREEN +
                            DISPLAY_MAIN_SCREEN);

            InOrder order = Mockito.inOrder(in.mock, editor, store);
            order.verify(in.mock, times(2)).promptLine(any());
            order.verify(editor).openPropertiesFile(before);
            order.verify(in.mock).promptLine(any());
            order.verify(store).saveTableProperties(INSTANCE_ID, after, new PropertiesDiff(before, after));
            order.verify(in.mock, times(2)).promptLine(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        void shouldRejectAChangeToASystemDefinedProperty() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties before = createValidTableProperties(properties);
            before.set(DATA_BUCKET, "bucket-created-by-cdk");
            TableProperties after = createValidTableProperties(properties);
            after.set(DATA_BUCKET, "changed-bucket");

            // When
            String output = updateTablePropertiesAndExitGetOutput(properties, before, after,
                    ValidateChangesScreen.DISCARD_CHANGES_OPTION);

            // Then
            assertThat(output).isEqualTo(DISPLAY_MAIN_SCREEN + CLEAR_CONSOLE + TABLE_SELECT_SCREEN +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.table.data.bucket\n" +
                    "The S3 bucket name where table data is stored.\n" +
                    "Before: bucket-created-by-cdk\n" +
                    "After (cannot be changed, please undo): changed-bucket\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.table.data.bucket\n" +
                    "\n" +
                    PROPERTY_VALIDATION_SCREEN + DISPLAY_MAIN_SCREEN);
        }
    }

    private String updatePropertiesDiscardChangesGetOutput(InstanceProperties before, InstanceProperties after) throws Exception {
        return updatePropertiesAndExitGetOutput(before, after, SaveChangesScreen.DISCARD_CHANGES_OPTION);
    }

    private String updateInvalidPropertiesGetOutput(InstanceProperties before, InstanceProperties after) throws Exception {
        return updatePropertiesAndExitGetOutput(before, after, ValidateChangesScreen.DISCARD_CHANGES_OPTION);
    }

    private String updatePropertiesAndSaveGetOutput(InstanceProperties before, InstanceProperties after) throws Exception {
        return updatePropertiesAndExitGetOutput(before, after, SaveChangesScreen.SAVE_CHANGES_OPTION);
    }

    private String updatePropertiesAndExitGetOutput(InstanceProperties before, InstanceProperties after,
                                                    String... saveOrValidateScreenOptions) throws Exception {
        return editInstanceConfiguration(before, after)
                .enterPrompts(saveOrValidateScreenOptions)
                .exitGetOutput();
    }

    private String updateTablePropertiesAndExitGetOutput(InstanceProperties properties,
                                                         TableProperties before, TableProperties after,
                                                         String... saveOrValidateScreenOptions) throws Exception {
        return editTableConfiguration(properties, before, after)
                .enterPrompt(before.get(TABLE_NAME))
                .enterPrompts(saveOrValidateScreenOptions)
                .exitGetOutput();
    }

    private static String outputWithSaveChangesDisplayWhenDiscardingChanges(String expectedSaveChangesDisplay) {
        return DISPLAY_MAIN_SCREEN + expectedSaveChangesDisplay + PROPERTY_SAVE_CHANGES_SCREEN + DISPLAY_MAIN_SCREEN;
    }

    private static String outputWithValidationDisplayWhenDiscardingChanges(String expectedValidationDisplay) {
        return DISPLAY_MAIN_SCREEN + expectedValidationDisplay + PROPERTY_VALIDATION_SCREEN + DISPLAY_MAIN_SCREEN;
    }

    private RunClient editInstanceConfiguration(InstanceProperties before, InstanceProperties after)
            throws Exception {
        setInstanceProperties(before);
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));
        return new RunClient()
                .enterPrompt(INSTANCE_CONFIGURATION_OPTION);
    }

    private RunClient editTableConfiguration(InstanceProperties instanceProperties,
                                             TableProperties before, TableProperties after)
            throws Exception {
        setInstanceProperties(instanceProperties, before);
        when(editor.openPropertiesFile(before))
                .thenReturn(withChanges(before, after));
        return new RunClient()
                .enterPrompt(TABLE_CONFIGURATION_OPTION);
    }

    private class RunClient {

        private RunClient enterPrompt(String entered) {
            in.enterNextPrompt(entered);
            return this;
        }

        private RunClient enterPrompts(String... entered) {
            in.enterNextPrompts(entered);
            return this;
        }

        private String exitGetOutput() throws Exception {
            in.enterNextPrompt(EXIT_OPTION);
            return runClientGetOutput();
        }

    }
}
