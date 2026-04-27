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

import sleeper.clients.admin.testutils.AdminClientInMemoryTestBase;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.SaveChangesScreen;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.ValidateChangesScreen;
import sleeper.core.properties.SleeperPropertiesPrettyPrinter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_SAVE_CHANGES_AUTO_CDK_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_SAVE_CHANGES_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_VALIDATION_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TABLE_SELECT_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.TEST_TABLE_REPORT_LIST;
import static sleeper.clients.testutil.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;
import static sleeper.clients.util.console.ConsoleOutput.CLEAR_CONSOLE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CDK_APP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.core.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_COMPRESSION_CODEC;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.ROW_GROUP_SIZE;
import static sleeper.core.properties.table.TableProperty.SCHEMA;

class InstanceConfigurationScreenTest extends AdminClientInMemoryTestBase {

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
                    "sleeper.default.table.parquet.page.size",
                    "sleeper.default.table.parquet.compression.codec");
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
            after.set(FORCE_RELOAD_PROPERTIES, "abc");

            // When
            String output = editConfigurationDiscardInvalidChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.properties.force.reload\n" +
                    "If true, properties will be reloaded every time a long running job is started or a lambda is run.\n" +
                    "This will mainly be used in test scenarios to ensure properties are up to date.\n" +
                    "Unset before, default value: false\n" +
                    "After (not valid, please change): abc\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.properties.force.reload\n" +
                    "\n"));
        }

        @Test
        void shouldShowValidationScreen() throws Exception {
            // Given
            InstanceProperties before = createValidInstanceProperties();
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(FORCE_RELOAD_PROPERTIES, "abc");

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
            after.set(FORCE_RELOAD_PROPERTIES, "abc");
            after.set(DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH, "def");

            // When
            String output = editConfigurationDiscardInvalidChangesGetOutput(before, after);

            // Then
            assertThat(output).isEqualTo(outputWithValidationDisplayWhenDiscardingChanges("" +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.properties.force.reload\n" +
                    "If true, properties will be reloaded every time a long running job is started or a lambda is run.\n" +
                    "This will mainly be used in test scenarios to ensure properties are up to date.\n" +
                    "Unset before, default value: false\n" +
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
                    "sleeper.properties.force.reload\n" +
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
            after.set(FORCE_RELOAD_PROPERTIES, "abc");

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
                    "sleeper.properties.force.reload\n" +
                    "If true, properties will be reloaded every time a long running job is started or a lambda is run.\n" +
                    "This will mainly be used in test scenarios to ensure properties are up to date.\n" +
                    "Unset before, default value: false\n" +
                    "After (not valid, please change): abc\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.vpc\n" +
                    "sleeper.properties.force.reload\n" +
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
                    "Before: sleeper-" + instanceId + "-config-test-account\n" +
                    "After (cannot be changed, please undo): changed-bucket\n" +
                    "\n" +
                    "Found invalid properties:\n" +
                    "sleeper.config.bucket\n" +
                    "\n"));
        }
    }

    @DisplayName("Configure table properties")
    @Nested
    class ConfigureTableProperties {

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
                            "sleeper.table.parquet.rowgroup.size\n" +
                            "Maximum number of bytes to write in a Parquet row group " +
                            "(defaults to value set in instance\n" +
                            "properties). This property is NOT used by DataFusion data engine.\n" +
                            "Unset before, default value: 8388608\n" +
                            "After: 123\n")
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN + PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN + DISPLAY_MAIN_SCREEN);
        }

        @Test
        void shouldFailToSetInvalidSchemaInEditor() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties before = createValidTableProperties(properties);
            TableProperties after = TableProperties.copyOf(before);
            after.set(SCHEMA, "{}");

            // When
            String output = editTableConfiguration(properties, before, after)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN + CLEAR_CONSOLE + "\n" +
                    TEST_TABLE_REPORT_LIST + TABLE_SELECT_SCREEN)
                    .contains("Found changes to properties:\n" +
                            "\n" +
                            "sleeper.table.schema\n" +
                            "The schema representing the structure of this table. This should be set in a separate schema.json\n" +
                            "file, and cannot be edited once the table has been created.\n" +
                            "See https://github.com/gchq/sleeper/blob/develop/docs/deployment/instance-configuration.md for\n" +
                            "further details.\n" +
                            "Before: {\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}],\"sortKeyFields\":[],\"valueFields\":[{\"name\":\"value\",\"type\":\"StringType\"}]}\n" +
                            "After (cannot be changed, please undo): {}\n" +
                            "\n" +
                            "Found invalid properties:\n" +
                            "sleeper.table.schema\n" +
                            "\n");
        }

        @Test
        void shouldRecoverFromInvalidSchema() throws Exception {
            // Given
            InstanceProperties properties = createValidInstanceProperties();
            TableProperties before = createValidTableProperties(properties);
            TableProperties after = TableProperties.copyOf(before);
            before.set(SCHEMA, "{}");

            // When
            String output = editTableConfiguration(properties, before, after)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then
            assertThat(output).startsWith(DISPLAY_MAIN_SCREEN + CLEAR_CONSOLE + "\n" +
                    TEST_TABLE_REPORT_LIST + TABLE_SELECT_SCREEN)
                    .contains("Found changes to properties:\n" +
                            "\n" +
                            "sleeper.table.schema\n" +
                            "The schema representing the structure of this table. This should be set in a separate schema.json\n" +
                            "file, and cannot be edited once the table has been created.\n" +
                            "See https://github.com/gchq/sleeper/blob/develop/docs/deployment/instance-configuration.md for\n" +
                            "further details.\n" +
                            "Before: {}\n" +
                            "After: {\"rowKeyFields\":[{\"name\":\"key\",\"type\":\"StringType\"}],\"sortKeyFields\":[],\"valueFields\":[{\"name\":\"value\",\"type\":\"StringType\"}]}\n" +
                            "\n")
                    .endsWith(PROPERTY_SAVE_CHANGES_SCREEN + PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN + DISPLAY_MAIN_SCREEN);
        }
    }

    @Nested
    @DisplayName("Handle properties requiring CDK deployment")
    class CdkDeployment {

        @Test
        void shouldRedeployStandardCdkAppWhenCdkDeployedPropertyIsUpdated() throws Exception {
            // Given an instance that was deployed with the standard CDK app
            InstanceProperties before = createValidInstanceProperties();
            before.setEnum(CDK_APP, SleeperInternalCdkApp.STANDARD);
            // And a property change to force a redeploy
            InstanceProperties after = InstanceProperties.copyOf(before);
            after.set(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS, "100");

            // When we apply the change
            String output = editInstanceConfiguration(before, after)
                    .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                    .exitGetOutput();

            // Then the properties are not saved to the instance, as the CDK will apply the change
            assertThat(clientProperties.loadInstancePropertiesNoValidation(after.get(ID)))
                    .isEqualTo(before);
            // And the properties are saved to the local directory, to be read by the CDK
            assertThat(clientProperties.getLocalInstanceProperties()).isEqualTo(after);
            // And the CDK is invoked
            assertThat(cdkCommandsThatRan).containsExactly(pipeline(command(
                    "cdk",
                    "-a", "java -cp \"./test/jars/cdk-1.2.3.jar\" sleeper.cdk.SleeperCdkApp",
                    "deploy",
                    "--require-approval", "never",
                    "-c", "propertiesfile=./test/generated/instance.properties",
                    "*")));
            // And this is displayed to the user
            assertThat(output).isEqualTo(DISPLAY_MAIN_SCREEN +
                    "Found changes to properties:\n" +
                    "\n" +
                    "sleeper.compaction.job.creation.timeout.seconds\n" +
                    "The timeout for the lambda that creates compaction jobs in seconds.\n" +
                    "Unset before, default value: 900\n" +
                    "After: 100\n" +
                    "Note that a change to this property requires redeployment of the instance.\n" +
                    "\n" +
                    PROPERTY_SAVE_CHANGES_AUTO_CDK_SCREEN +
                    PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN +
                    DISPLAY_MAIN_SCREEN);
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
