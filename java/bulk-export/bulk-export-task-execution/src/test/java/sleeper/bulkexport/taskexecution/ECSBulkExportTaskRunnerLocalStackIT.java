/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.bulkexport.taskexecution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuerySerDe;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.properties.testutils.InstancePropertiesTestHelper;
import sleeper.core.properties.testutils.TablePropertiesTestHelper;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.util.ObjectFactoryException;
import sleeper.localstack.test.LocalStackTestBase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class ECSBulkExportTaskRunnerLocalStackIT extends LocalStackTestBase {
    private InstanceProperties instanceProperties = InstancePropertiesTestHelper.createTestInstanceProperties();
    private final Field field = new Field("key", new IntType());
    private final Schema schema = Schema.builder()
            .rowKeyFields(field)
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();
    private TablePropertiesProvider tablePropertiesProvider;
    private BulkExportLeafPartitionQuerySerDe bulkExportSerDe;
    private TableProperties tableProperties;

    @BeforeEach
    void setUp() {
        String jobQueueUrl = sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl();
        instanceProperties.set(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL, jobQueueUrl);
        instanceProperties.setNumber(BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS, 0);
        tableProperties = TablePropertiesTestHelper.createTestTableProperties(instanceProperties, schema);

    }

    @Test
    public void shouldProcessesMessages() throws IOException, IteratorCreationException, ObjectFactoryException {
        // Given
        String tableId = "t-id";
        tableProperties.set(TABLE_ID, tableId);
        tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        bulkExportSerDe = new BulkExportLeafPartitionQuerySerDe(tablePropertiesProvider);
        BulkExportLeafPartitionQuery exportTaskMessage = createMessage(tableId);
        String messageBody = bulkExportSerDe.toJson(exportTaskMessage);

        // When
        sqsClient.sendMessage(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL), messageBody);

        // Run the ECS bulk export task runner
        ECSBulkExportTaskRunner.runECSBulkExportTaskRunner(sqsClientV2, s3ClientV2, dynamoClientV2, instanceProperties, tablePropertiesProvider);

        // Then
        // Verify the queue is empty
        List<String> messages = getMessagesFromQueue(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL));
        assertThat(messages).isEmpty();
    }

    private BulkExportLeafPartitionQuery createMessage(String tableId) {
        String exportId = "e-id";
        String subExportId = "se-id";
        String leafPartitionId = "lp-id";
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange(field, 1, true, 10, true));
        Region partitionRegion = new Region(rangeFactory.createRange(field, 0, 1000));
        return BulkExportLeafPartitionQuery.builder()
                .tableId(tableId)
                .exportId(exportId)
                .subExportId(subExportId)
                .regions(List.of(region1))
                .leafPartitionId(leafPartitionId)
                .partitionRegion(partitionRegion)
                .files(Collections.singletonList("/test/file.parquet"))
                .build();
    }

    private List<String> getMessagesFromQueue(String queueUrl) {
        return sqsClientV2.receiveMessage(ReceiveMessageRequest.builder().queueUrl(queueUrl).build())
                .messages()
                .stream()
                .map(Message::body)
                .toList();
    }
}
