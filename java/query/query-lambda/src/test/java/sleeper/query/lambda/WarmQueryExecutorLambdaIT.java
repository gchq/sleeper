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
package sleeper.query.lambda;

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactoryException;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutputConstants;
import sleeper.query.runner.tracker.DynamoDBQueryTrackerCreator;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.query.runner.output.NoResultsOutput.NO_RESULTS_OUTPUT;

public class WarmQueryExecutorLambdaIT extends LocalStackTestBase {

    private QuerySerDe querySerDe;

    @TempDir
    public Path tempDir;
    private InstanceProperties instanceProperties;
    protected static WarmQueryExecutorLambda lambda;

    @BeforeEach
    void setUp() throws IOException, ObjectFactoryException {
        String dataDir = createTempDirectory(tempDir, null).toString();
        createInstanceProperties(dataDir);
        lambda = new WarmQueryExecutorLambda(s3Client, sqsClient, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
    }

    @Test
    public void shouldCreateAQueryWithKeyTypeOfString() throws Exception {
        // Given
        Schema schema = getStringKeySchema();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        createTable(tableProperties);
        querySerDe = new QuerySerDe(schema);

        // When
        lambda.handleRequest(new ScheduledEvent(), null);

        // Then
        ReceiveMessageResult result = sqsClient.receiveMessage(new ReceiveMessageRequest(instanceProperties.get(QUERY_QUEUE_URL)));
        assertThat(result.getMessages()).hasSize(1);

        Query query = querySerDe.fromJson(result.getMessages().get(0).getBody());
        Query expected = buildExpectedQuery(query.getQueryId(), tableProperties.get(TABLE_NAME), schema,
                new Field("test-key", new StringType()), "a");

        assertThat(query).isEqualTo(expected);
    }

    private Schema getStringKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("test-key", new StringType()))
                .sortKeyFields(new Field("test-sort", new StringType()))
                .valueFields(new Field("test-value", new StringType()))
                .build();
    }

    private Query buildExpectedQuery(String id, String tableName, Schema schema, Field rowKey, Object value) {
        Region region = new Region(List.of(new Range.RangeFactory(schema)
                .createExactRange(rowKey, value)));
        return Query.builder()
                .queryId(id)
                .tableName(tableName)
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .resultsPublisherConfig(Map.of(ResultsOutputConstants.DESTINATION, NO_RESULTS_OUTPUT))
                        .statusReportDestinations(Collections.emptyList())
                        .build())
                .build();
    }

    private void createInstanceProperties(String dir) throws IOException {
        instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, dir);
        instanceProperties.set(CONFIG_BUCKET, "testing");
        instanceProperties.set(QUERY_QUEUE_URL, sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(LEAF_PARTITION_QUERY_QUEUE_URL,
                sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(QUERY_RESULTS_QUEUE_URL,
                sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(QUERY_RESULTS_BUCKET, dir + "/query-results");

        createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);

        new DynamoDBQueryTrackerCreator(instanceProperties, dynamoClient).create();
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
    }

    private void createTable(TableProperties tableProperties) {
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient, hadoopConf)
                .getStateStore(tableProperties);
        update(stateStore).initialise(tableProperties.getSchema());
    }
}
