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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.util.ObjectFactoryException;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutput;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.query.runner.output.NoResultsOutput.NO_RESULTS_OUTPUT;

public class WarmQueryExecutorLambdaIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = Schema.builder()
            .rowKeyFields(new Field("test-key", new StringType()))
            .sortKeyFields(new Field("test-sort", new StringType()))
            .valueFields(new Field("test-value", new StringType()))
            .build();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException, ObjectFactoryException {
        instanceProperties.set(QUERY_QUEUE_URL, createSqsQueueGetUrl());
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        tablePropertiesStore().save(tableProperties);
    }

    @Test
    public void shouldCreateAQueryWithKeyTypeOfString() throws Exception {

        // When
        lambda().handleRequest(new ScheduledEvent(), null);

        // Then
        assertThat(receiveQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("queryId")
                .containsExactly(createQueryForFieldExactValue("test-key", "a"));
    }

    private WarmQueryExecutorLambda lambda() throws Exception {
        return new WarmQueryExecutorLambda(s3Client, sqsClient, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
    }

    private List<Query> receiveQueries() {
        return receiveMessages(instanceProperties.get(QUERY_QUEUE_URL))
                .map(new QuerySerDe(schema)::fromJson)
                .toList();
    }

    private Query createQueryForFieldExactValue(String keyField, Object value) {
        Region region = new Region(List.of(new Range.RangeFactory(schema).createExactRange(keyField, value)));
        return Query.builder()
                .queryId("test-id")
                .tableName(tableProperties.get(TABLE_NAME))
                .regions(List.of(region))
                .processingConfig(QueryProcessingConfig.builder()
                        .resultsPublisherConfig(Map.of(ResultsOutput.DESTINATION, NO_RESULTS_OUTPUT))
                        .statusReportDestinations(Collections.emptyList())
                        .build())
                .build();
    }

    private TablePropertiesStore tablePropertiesStore() {
        return S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
    }
}
