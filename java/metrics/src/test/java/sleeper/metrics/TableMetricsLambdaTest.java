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
package sleeper.metrics;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.cloudwatchlogs.emf.config.Configuration;
import software.amazon.cloudwatchlogs.emf.config.EnvironmentConfigurationProvider;
import software.amazon.cloudwatchlogs.emf.environment.Environments;

import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.metrics.MetricsTestUtils.example;

public class TableMetricsLambdaTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties,
            createSchemaWithKey("key"));
    private final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore
            .createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());
    private final PrintStream standardOut = System.out;
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();

    @BeforeEach
    void setUp() throws Exception {
        System.setOut(new PrintStream(outputStreamCaptor));
        Configuration emfConfig = new Configuration();
        emfConfig.setEnvironmentOverride(Environments.Local);
        Field emfConfigField = EnvironmentConfigurationProvider.class.getDeclaredField("config");
        emfConfigField.setAccessible(true);
        emfConfigField.set(null, emfConfig);
        instanceProperties.set(ID, "test-instance");
        tableProperties.set(TABLE_NAME, "test-table");
    }

    @AfterEach
    void tearDown() {
        System.setOut(standardOut);
    }

    @Test
    void shouldGenerateMetrics() {
        // Given
        update(stateStore()).initialise(tableProperties);
        update(stateStore()).addFile(fileFactory().rootFile("test", 123L));

        // When
        invokeForSingleTable();

        // Then
        assertThatJson(outputStreamCaptor.toString())
                .whenIgnoringPaths("_aws.Timestamp")
                .isEqualTo(example("example/one-file.json"));
    }

    private void invokeForSingleTable() {
        lambda().handleRequest(event(messageForTableWithId(tableProperties, "test-message")), null);
    }

    private StateStore stateStore() {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(instanceProperties, tableProperties, stateStore());
    }

    private TableMetricsLambda lambda() {
        return new TableMetricsLambda(instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                stateStoreProvider, PropertiesReloader.neverReload());
    }

    private SQSEvent event(SQSMessage... messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private SQSMessage messageForTableWithId(TableProperties tableProperties, String messageId) {
        SQSMessage message = new SQSMessage();
        message.setBody(tableProperties.get(TABLE_ID));
        message.setMessageId(messageId);
        return message;
    }

}
