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
package sleeper.query.runner.output;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.core.iterator.WrappedIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.SleeperRow;
import sleeper.core.record.ResultsBatch;
import sleeper.core.record.serialiser.JSONResultsBatchSerialiser;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.model.QueryProcessingConfig;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SQSResultsOutputIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = createSchemaWithKey("key", new StringType());
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();

    @BeforeEach
    void setUp() {
        instanceProperties.set(QUERY_RESULTS_QUEUE_URL, createSqsQueueGetUrl());
    }

    @Test
    void shouldSendResultsTOSQS() {
        // Given
        List<SleeperRow> records = List.of(
                new SleeperRow(Map.of("key", "value-1")),
                new SleeperRow(Map.of("key", "value-2")));

        // When
        output().publish(queryWithId("test-query"), new WrappedIterator<>(records.iterator()));

        // Then
        assertThat(receiveResults()).containsExactly(
                new ResultsBatch("test-query", schema, records));
    }

    private SQSResultsOutput output() {
        return new SQSResultsOutput(instanceProperties, sqsClient, tableProperties.getSchema(), Map.of());
    }

    private QueryOrLeafPartitionQuery queryWithId(String id) {
        return new QueryOrLeafPartitionQuery(LeafPartitionQuery.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .queryId(id)
                .subQueryId(UUID.randomUUID().toString())
                .regions(List.of(partitions.getRootPartition().getRegion()))
                .processingConfig(QueryProcessingConfig.none())
                .leafPartitionId(partitions.getRootPartition().getId())
                .partitionRegion(partitions.getRootPartition().getRegion())
                .files(List.of("test.parquet"))
                .build());
    }

    private List<ResultsBatch> receiveResults() {
        ReceiveMessageResponse response = sqsClient.receiveMessage(request -> request
                .queueUrl(instanceProperties.get(QUERY_RESULTS_QUEUE_URL))
                .waitTimeSeconds(1));
        JSONResultsBatchSerialiser serDe = new JSONResultsBatchSerialiser();
        return response.messages().stream()
                .map(Message::body)
                .map(serDe::deserialise)
                .toList();
    }

}
