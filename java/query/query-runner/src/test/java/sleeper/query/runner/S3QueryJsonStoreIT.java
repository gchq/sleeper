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
package sleeper.query.runner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.local.aws.test.LocalAwsTestBase;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryMessageSerDe;
import sleeper.query.core.model.QuerySerDe;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class S3QueryJsonStoreIT extends LocalAwsTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key", new LongType());
    private final RangeFactory rangeFactory = new RangeFactory(schema);
    private final QuerySerDe querySerDe = new QuerySerDe(schema);

    @BeforeEach
    void setUp() {
        instanceProperties.set(QUERY_RESULTS_BUCKET, UUID.randomUUID().toString());
        createBucket(instanceProperties.get(QUERY_RESULTS_BUCKET));
    }

    @Test
    void shouldStoreAndLoadQueryTooLargeForSqsMessage() {
        // Given
        QueryMessageSerDe messageSerDe = S3QueryJsonStore.createMessageSerDe(instanceProperties, querySerDe, s3Client);
        Query query = Query.builder()
                .queryId("large-query")
                .tableName("test-table")
                .regions(IntStream.range(0, 5000)
                        .mapToObj(i -> new Region(rangeFactory.createRange("key", i * 10L, i * 10L + 5)))
                        .toList())
                .build();
        String json = querySerDe.toJson(query);
        assertThat(json.length()).isGreaterThan(256 * 1024);

        // When
        String message = messageSerDe.toMessage(query);

        // Then
        assertThat(message).startsWith("{\"queryJsonS3Key\":\"queries/large-query/query-").hasSizeLessThan(200);
        assertThat(listObjectKeys(instanceProperties.get(QUERY_RESULTS_BUCKET)))
                .singleElement().satisfies(key -> assertThat(key).startsWith("queries/large-query/query-"));
        assertThat(messageSerDe.fromMessage(message).asParentQuery()).isEqualTo(query);
    }

    @Test
    void shouldNotStoreSmallQuery() {
        // Given
        QueryMessageSerDe messageSerDe = S3QueryJsonStore.createMessageSerDe(instanceProperties, querySerDe, s3Client);
        Query query = Query.builder()
                .queryId("small-query")
                .tableName("test-table")
                .regions(List.of(new Region(rangeFactory.createRange("key", 1L, 2L))))
                .build();

        // When
        String message = messageSerDe.toMessage(query);

        // Then
        assertThat(message).isEqualTo(querySerDe.toJson(query));
        assertThat(listObjectKeys(instanceProperties.get(QUERY_RESULTS_BUCKET))).isEmpty();
    }
}
