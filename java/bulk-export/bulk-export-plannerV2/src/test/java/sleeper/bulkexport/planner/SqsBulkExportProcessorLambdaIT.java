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
package sleeper.bulkexport.planner;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuerySerDe;
import sleeper.bulkexport.core.model.BulkExportQueryValidationException;
import sleeper.bulkexport.plannerV2.SqsBulkExportProcessorLambda;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactoryException;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestorev2.StateStoreFactory;
import sleeper.statestorev2.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class SqsBulkExportProcessorLambdaIT extends LocalStackTestBase {

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
    SqsBulkExportProcessorLambda sqsBulkExportProcessorLambda;
    StateStore stateStore;

    @BeforeEach
    public void setUp() throws ObjectFactoryException, IOException {
        instanceProperties.set(BULK_EXPORT_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.set(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL, createSqsQueueGetUrl());
        tableProperties.set(TABLE_NAME, "test-table");
        s3ClientV2.createBucket(CreateBucketRequest.builder().bucket(instanceProperties.get(CONFIG_BUCKET)).build());
        S3InstanceProperties.saveToS3(s3ClientV2, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClientV2, instanceProperties);
        S3TableProperties.createStore(instanceProperties, s3ClientV2, dynamoClientV2).save(tableProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClientV2).create();
        stateStore = new StateStoreFactory(instanceProperties, s3ClientV2, dynamoClientV2, null).getStateStore(tableProperties);
        sqsBulkExportProcessorLambda = new SqsBulkExportProcessorLambda(sqsClientV2, s3ClientV2, dynamoClientV2, instanceProperties.get(CONFIG_BUCKET));
    }

    @Test
    public void shouldSplitBulkExportToLeafPartitionMessages() {
        // Given
        PartitionTree partitions = new PartitionsBuilder(KEY_VALUE_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "0---eee", "eee---zzz", "eee")
                .buildTree();
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        update(stateStore).initialise(partitions.getAllPartitions());
        update(stateStore).addFilesWithReferences(List.of(
                fileWithReferences(List.of(fileFactory.rootFile("file1.parquet", 100))),
                fileWithReferences(List.of(fileFactory.rootFile("file2.parquet", 200))),
                fileWithNoReferences("file3.parquet")));
        SQSEvent event = createEvent("{\"exportId\":\"test-export\",\"tableName\":\"test-table\"}");

        // When
        sqsBulkExportProcessorLambda.handleRequest(event, null);

        // Then
        assertThat(receiveLeafPartitionQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("subExportId")
                .containsExactlyInAnyOrder(
                        leafPartitionQuery(
                                partitions.getPartition("0---eee"),
                                List.of("file1.parquet", "file2.parquet")),
                        leafPartitionQuery(
                                partitions.getPartition("eee---zzz"),
                                List.of("file1.parquet", "file2.parquet")));
    }

    @Test
    public void shouldThrowExceptionForMalformedJson() {
        // Given
        SQSEvent event = createEvent("not json");

        // When Then
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected BEGIN_OBJECT but was STRING");
    }

    @Test
    public void shouldThrowExceptionWhenTableIsNotSet() {
        // Given
        SQSEvent event = createEvent("{\"invalidField\":\"value\"}");

        // When Then
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(BulkExportQueryValidationException.class)
                .hasMessageContaining("Query validation failed: tableId or tableName field must be provided");
    }

    private BulkExportLeafPartitionQuery leafPartitionQuery(Partition partition, List<String> files) {
        return BulkExportLeafPartitionQuery.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .exportId("test-export")
                .subExportId("ignored")
                .regions(List.of(partition.getRegion()))
                .partitionRegion(partition.getRegion())
                .leafPartitionId(partition.getId())
                .files(files)
                .build();
    }

    private SQSEvent createEvent(String messageBody) {
        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody(messageBody);
        event.setRecords(Collections.singletonList(message));
        return event;
    }

    private List<BulkExportLeafPartitionQuery> receiveLeafPartitionQueries() {
        ReceiveMessageResponse response = sqsClientV2.receiveMessage(request -> request
                .queueUrl(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL))
                .maxNumberOfMessages(10)
                .waitTimeSeconds(1));
        BulkExportLeafPartitionQuerySerDe serDe = new BulkExportLeafPartitionQuerySerDe(KEY_VALUE_SCHEMA);
        return response.messages().stream()
                .map(message -> serDe.fromJson(message.body()))
                .toList();
    }
}
