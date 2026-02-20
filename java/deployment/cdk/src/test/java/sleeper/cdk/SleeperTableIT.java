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
package sleeper.cdk;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;
import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;

import sleeper.cdk.artefacts.SleeperArtefactsFromProperties;
import sleeper.cdk.artefacts.SleeperJarVersionIdProvider;
import sleeper.cdk.stack.core.TableDefinerStack;
import sleeper.cdk.testutil.SleeperInstancePrinter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SleeperTableIT {

    InstanceProperties instanceProperties = createTestInstancePropertiesWithId("test-instance");
    SleeperInstancePrinter printer = new SleeperInstancePrinter();

    @Test
    void shouldGenerateSleeperTableObjectWithDefaultOptions() {
        // Given
        Stack stack = createSleeperTableAsRootStack();

        TableDefinerStack definerStack = new TableDefinerStack(stack, "TableDefiner", instanceProperties,
                new SleeperArtefactsFromProperties(instanceProperties, jarVersionIds()));

        // When
        TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        tableProperties.set(TABLE_ID, "table-id");
        tableProperties.set(TABLE_NAME, "table-name");
        definerStack.createCustomResource(tableProperties);

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .withReporter((receieved, approved) -> false)
                .forFile().withName("default-table", ".json"));

    }

    private Stack createSleeperTableAsRootStack() {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account("test-account")
                .region("test-region")
                .build();
        StackProps stackProps = StackProps.builder()
                .stackName("test-table")
                .env(environment)
                .build();

        return new Stack(app, "TestTable", stackProps);
    }

    private SleeperJarVersionIdProvider jarVersionIds() {
        return new SleeperJarVersionIdProvider(jar -> jar.getArtifactId() + "-test-version");
    }

    /*
     * SleeperTable sleeperTable = SleeperTable.builder()
     * .tableName("tableName")
     * .instanceId("instanceId")
     * .schema(createSchemaWithKey("key"))
     * .build();
     */
}
