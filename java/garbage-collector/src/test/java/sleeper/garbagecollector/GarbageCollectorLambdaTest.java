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
package sleeper.garbagecollector;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.garbagecollector.FailedGarbageCollectionException.TableFailures;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class GarbageCollectorLambdaTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    private List<List<TableProperties>> garbageCollectorRuns = new ArrayList<>();
    private Consumer<List<TableProperties>> garbageCollector = garbageCollectorRuns::add;

    @Test
    void shouldGC() {
        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                messageForTableWithId(tableProperties.get(TABLE_ID), "test-message")), null);

        // Then
        assertThat(garbageCollectorRuns).containsExactly(List.of(tableProperties));
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    @Test
    void shouldReportFailedGC() {
        // Given
        garbageCollector = tables -> {
            throw new FailedGarbageCollectionException(List.of(
                    new TableFailures(tableProperties.getStatus(), new RuntimeException("GC failed"), List.of(), List.of())));
        };

        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                messageForTableWithId(tableProperties.get(TABLE_ID), "test-message")), null);

        // Then
        assertThat(response.getBatchItemFailures()).singleElement()
                .extracting(BatchItemFailure::getItemIdentifier)
                .isEqualTo("test-message");
    }

    @Test
    void shouldIgnoreTableThatWasDeleted() {
        // When
        SQSBatchResponse response = lambda().handleRequest(event(
                messageForTableWithId("missing-table", "test-message")), null);

        // Then
        assertThat(garbageCollectorRuns).containsExactly(List.of());
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    private GarbageCollectorLambda lambda() {
        return new GarbageCollectorLambda(
                new FixedTablePropertiesProvider(tableProperties), PropertiesReloader.neverReload(),
                garbageCollector);
    }

    private SQSEvent event(SQSMessage... messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private SQSMessage messageForTableWithId(String tableId, String messageId) {
        SQSMessage message = new SQSMessage();
        message.setBody(tableId);
        message.setMessageId(messageId);
        return message;
    }

}
