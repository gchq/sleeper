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
import sleeper.clients.admin.testutils.AdminClientMockStoreBase;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.SaveChangesScreen;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.ValidateChangesScreen;
import sleeper.core.properties.SleeperPropertiesPrettyPrinter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertyGroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
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
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_COMPRESSION_CODEC;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.core.properties.table.TableProperty.STATESTORE_ASYNC_COMMITS_ENABLED;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

class InstanceConfigurationScreenTest extends AdminClientMockStoreBase {

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

    @DisplayName("Display changes to edited properties")
    @Nested
    class DisplayChanges {

        @Test
        void shouldEditAProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(FARGATE_VERSION, "1.4.1");
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(FARGATE_VERSION, "1.4.2");

            // When
            String output = editConfigurationDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithSaveChangesDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.fargate.version\n" +
                    "The version of Fargate to use.\n" +
                    "Before: 1.4.1\n" +
                    "After: 1.4.2\n" +
                    "\n"));
        }

        @Test
        void shouldSetADefaultedProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(FARGATE_VERSION, "1.4.1");

            // When
            String output = editConfigurationDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithSaveChangesDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.fargate.version\n" +
                    "The version of Fargate to use.\n" +
                    "Unset before, default value: 1.4.0\n" +
                    "After: 1.4.1\n" +
                    "\n"));
        }

        @Test
        void shouldSetAnUnknownProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.getProperties().setProperty("unknown.property", "abc");

            // When
            String output = editConfigurationDiscardChangesGetOutput(before, after);

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
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, "123");

            // When
            String output = editConfigurationDiscardChangesGetOutput(before, after);

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
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);
            after.set(DEFAULT_PAGE_SIZE, "456");
            after.set(DEFAULT_COMPRESSION_CODEC, "zstd");

            // When
            String output = editConfigurationDiscardChangesGetOutput(before, after);

            // Then
            assertThat(output).containsSubsequence(
                    "sleeper.optional.stacks",
                    "sleeper.default.table.page.size",
                    "sleeper.default.table.compression.codec");
        }

        @Test
        void shouldOrderUnknownPropertiesAfterKnownProperties() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.setEnum(OPTIONAL_STACKS, OptionalStack.CompactionStack);
            after.getProperties().setProperty("some.unknown.property", "a-value");
            after.getProperties().setProperty("an.unknown.property", "other-value");

            // When
            String output = editConfigurationDiscardChangesGetOutput(before, after);

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
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS, "abc");

            // When
            String output = editConfigurationDiscardInvalidChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.compaction.job.creation.timeout.seconds\n" +
                    "The timeout for the lambda that creates compaction jobs in seconds.\n" +
                    "Unset before, default value: 900\n" +
                    "After (not valid, please change): abc\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.compaction.job.creation.timeout.seconds\n" +
                    "\n"));
        }

        @Test
        void shouldShowValidationScreen() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS, "abc");

            // When
            String output = editConfigurationDiscardInvalidChangesGetOutput(before, after);

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN)
                    .endsWith(PROPERTY_VALIDATION_SCREEN + DISPLAY_MAIN_SCREEN);
        }

        @Test
        void shouldShowValidationFailuresForMultipleProperties() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS, "abc");
            after.set(DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, "def");

            // When
            String output = editConfigurationDiscardInvalidChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.compaction.job.creation.timeout.seconds\n" +
                    "The timeout for the lambda that creates compaction jobs in seconds.\n" +
                    "Unset before, default value: 900\n" +
                    "After (not valid, please change): abc\n" +
                    "\n" +
                    "sleeper.default.table.parquet.columnindex.truncate.length\n" +
                    "Used to set parquet.columnindex.truncate.length, see documentation here:\n" +
                    "https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md\n" +
                    "The length in bytes to truncate binary values in a column index.\n" +
                    "Unset before, default value: 128\n" +
                    "After (not valid, please change): def\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.compaction.job.creation.timeout.seconds\n" +
                    "sleeper.default.table.parquet.columnindex.truncate.length\n" +
                    "\n"));
        }

        @Test
        void shouldRejectAChangeToAnUneditableProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            before.set(VPC_ID, "before-vpc");
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(VPC_ID, "after-vpc");

            // When
            String output = editConfigurationDiscardInvalidChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.vpc\n" +
                    SleeperPropertiesPrettyPrinter.formatDescription("", VPC_ID.getDescription()) + "\n" +
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
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(VPC_ID, "after-vpc");
            after.set(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS, "abc");

            // When
            String output = editConfigurationDiscardInvalidChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.vpc\n" +
                    SleeperPropertiesPrettyPrinter.formatDescription("", VPC_ID.getDescription()) + "\n" +
                    "Before: before-vpc\n" +
                    "After (cannot be changed, please undo): after-vpc\n" +
                    "\n" +
                    "sleeper.compaction.job.creation.timeout.seconds\n" +
                    "The timeout for the lambda that creates compaction jobs in seconds.\n" +
                    "Unset before, default value: 900\n" +
                    "After (not valid, please change): abc\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.vpc\n" +
                    "sleeper.compaction.job.creation.timeout.seconds\n" +
                    "\n"));
        }

        @Test
        void shouldRejectAChangeToASystemDefinedProperty() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(CONFIG_BUCKET, "changed-bucket");

            // When
            String output = editConfigurationDiscardInvalidChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.config.bucket\n" +
                    "The S3 bucket name used to store configuration files.\n" +
                    "Before: sleeper-" + instanceId + "-config\n" +
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

        @Test
        void shouldEditAPropertyThatWasPreviouslyUnsetButHadADefaultProperty() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties before = createValidTableProperties(properties);
            TableProperties after = TableProperties.copyOf(before);
            after.set(ROW_GROUP_SIZE, "123");

            // When
            String output = editTableConfiguration(properties, before, after)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN + CLEAR_CONSOLE + "\n" +
                    TEST_TABLE_REPORT_LIST + TABLE_SELECT_SCREEN)
                    .contains("Found changes to properties:\n" +
                            "\n" +
                            "sleeper.table.rowgroup.size\n" +
                            "Maximum number of bytes to write in a Parquet row group " +
                            "(defaults to value set in instance\n" +
                            "properties). This property is NOT used by DataFusion data engine.\n" +
                            "Unset before, default value: 8388608\n" +
                            "After: 123\n")
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN + PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN + DISPLAY_MAIN_SCREEN);
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

    private String editConfigurationDiscardChangesGetOutput(InstanceProperties before, InstanceProperties after) throws Exception {
        return editInstanceConfiguration(before, after)
                .enterPrompts(SaveChangesScreen.DISCARD_CHANGES_OPTION)
                .exitGetOutput();
    }

    private String editConfigurationDiscardInvalidChangesGetOutput(InstanceProperties before, InstanceProperties after) throws Exception {
        return editInstanceConfiguration(before, after)
                .enterPrompts(ValidateChangesScreen.DISCARD_CHANGES_OPTION)
                .exitGetOutput();
    }

    private static String outputWithSaveChangesDisplayWhenDiscardingChanges(String expectedSaveChangesDisplay) {
        return DISPLAY_MAIN_SCREEN + expectedSaveChangesDisplay + PROPERTY_SAVE_CHANGES_SCREEN + DISPLAY_MAIN_SCREEN;
    }

    private static String outputWithValidationDisplayWhenDiscardingChanges(String expectedValidationDisplay) {
        return DISPLAY_MAIN_SCREEN + expectedValidationDisplay + PROPERTY_VALIDATION_SCREEN + DISPLAY_MAIN_SCREEN;
    }
}
