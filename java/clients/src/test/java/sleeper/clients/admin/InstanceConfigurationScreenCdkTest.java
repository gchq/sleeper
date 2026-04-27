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

import org.junit.jupiter.api.Test;

import sleeper.clients.admin.testutils.AdminClientInMemoryTestBase;
import sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.SaveChangesScreen;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.DISPLAY_MAIN_SCREEN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.INSTANCE_CONFIGURATION_OPTION;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROMPT_SAVE_SUCCESSFUL_RETURN_TO_MAIN;
import static sleeper.clients.admin.testutils.ExpectedAdminConsoleValues.PROPERTY_SAVE_CHANGES_AUTO_CDK_SCREEN;
import static sleeper.clients.testutil.TestConsoleInput.CONFIRM_PROMPT;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CDK_APP;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class InstanceConfigurationScreenCdkTest extends AdminClientInMemoryTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @Test
    void shouldRedeployStandardCdkAppWhenCdkDeployedPropertyIsUpdated() throws Exception {
        // Given an instance that was deployed with the standard CDK app
        instanceProperties.setEnum(CDK_APP, SleeperInternalCdkApp.STANDARD);
        setInstanceProperties(instanceProperties);
        // And a property change to force a redeploy
        InstanceProperties propertiesBefore = InstanceProperties.copyOf(instanceProperties);
        instanceProperties.set(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS, "100");

        // When we apply the change
        String output = runClient().enterPrompt(INSTANCE_CONFIGURATION_OPTION)
                .editFromStore(propertiesBefore, instanceProperties)
                .enterPrompts(SaveChangesScreen.SAVE_CHANGES_OPTION, CONFIRM_PROMPT)
                .exitGetOutput();

        // Then the properties are not saved to the instance, as the CDK will apply the change
        assertThat(clientProperties.loadInstancePropertiesNoValidation(instanceProperties.get(ID)))
                .isEqualTo(propertiesBefore);
        // And the properties are saved to the local directory, to be read by the CDK
        assertThat(clientProperties.getLocalInstanceProperties()).isEqualTo(instanceProperties);
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
