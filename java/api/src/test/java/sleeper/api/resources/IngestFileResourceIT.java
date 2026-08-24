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
package sleeper.api.resources;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequestSerDe;

import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

@QuarkusTest
@TestProfile(IngestFileResourceIT.Profile.class)
class IngestFileResourceIT {

    static final String INSTANCE_ID = "ingest-file-it";
    static final String ACCOUNT_NAME = "test-account";
    static final String DATA_BUCKET = "ingest-file-it-source";

    @Inject
    S3Client s3Client;
    @Inject
    DynamoDbClient dynamoDbClient;
    @Inject
    SqsClient sqsClient;

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "sleeper.instance.id", INSTANCE_ID,
                    "sleeper.account.name", ACCOUNT_NAME,
                    "sleeper.api.ingest-file.max-files-per-path", "2");
        }
    }

    @BeforeEach
    void clearState() {
        // The localstack container is shared across tests, so wipe its state between them.
        LocalStackTestResources.deleteDynamoTables(dynamoDbClient);
        LocalStackTestResources.deleteS3Buckets(s3Client);
        LocalStackTestResources.deleteSqsQueues(sqsClient);
    }

    private InstanceProperties setUpInstance(boolean batcherEnabled) {
        InstanceProperties instanceProperties = createTestInstancePropertiesWithId(INSTANCE_ID);
        instanceProperties.setEnumList(OPTIONAL_STACKS,
                batcherEnabled ? List.of(OptionalStack.IngestBatcherStack) : List.of());
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(instanceProperties.get(CONFIG_BUCKET))
                .build());
        if (batcherEnabled) {
            String queueUrl = sqsClient.createQueue(builder -> builder.queueName("ingest-file-it-submit")).queueUrl();
            instanceProperties.set(INGEST_BATCHER_SUBMIT_QUEUE_URL, queueUrl);
        }
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDbClient, instanceProperties);
        return instanceProperties;
    }

    private TableProperties createTable(InstanceProperties instanceProperties, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        tableProperties.set(TABLE_NAME, tableName);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoDbClient).createTable(tableProperties);
        return tableProperties;
    }

    private void putObject(String key) {
        s3Client.putObject(builder -> builder.bucket(DATA_BUCKET).key(key), RequestBody.fromString("data"));
    }

    // ── /expand ──

    @Test
    void shouldExpandAPrefixToTheParquetFilesUnderIt() {
        setUpInstance(true);
        s3Client.createBucket(builder -> builder.bucket(DATA_BUCKET));
        putObject("data/part-0.parquet");
        putObject("data/part-1.parquet");
        putObject("data/_SUCCESS");

        given().contentType("application/json")
                .body(Map.of("paths", List.of(DATA_BUCKET + "/data")))
                .when().post("/api/ingest-file/expand")
                .then()
                .statusCode(200)
                .body("paths[0].prefix", is(true))
                .body("paths[0].empty", is(false))
                .body("paths[0].files.file", contains(
                        DATA_BUCKET + "/data/part-0.parquet",
                        DATA_BUCKET + "/data/part-1.parquet"))
                .body("missingPaths", hasSize(0));
    }

    @Test
    void shouldReportSingleFileAndMissingPath() {
        setUpInstance(true);
        s3Client.createBucket(builder -> builder.bucket(DATA_BUCKET));
        putObject("solo.parquet");

        given().contentType("application/json")
                .body(Map.of("paths", List.of(DATA_BUCKET + "/solo.parquet", DATA_BUCKET + "/does-not-exist")))
                .when().post("/api/ingest-file/expand")
                .then()
                .statusCode(200)
                .body("paths[0].prefix", is(false))
                .body("paths[0].files.file", contains(DATA_BUCKET + "/solo.parquet"))
                .body("paths[1].empty", is(true))
                .body("missingPaths", contains(DATA_BUCKET + "/does-not-exist"));
    }

    @Test
    void shouldReturnNoFilesWhenPrefixHasTooManyFiles() {
        setUpInstance(true);
        s3Client.createBucket(builder -> builder.bucket(DATA_BUCKET));
        putObject("big/part-0.parquet");
        putObject("big/part-1.parquet");
        putObject("big/part-2.parquet");

        given().contentType("application/json")
                .body(Map.of("paths", List.of(DATA_BUCKET + "/big")))
                .when().post("/api/ingest-file/expand")
                .then()
                .statusCode(200)
                .body("paths[0].tooMany", is(true))
                .body("paths[0].files", hasSize(0))
                .body("maxFilesPerPath", is(2))
                .body("missingPaths", hasSize(0));
    }

    @Test
    void shouldRejectExpandWithNoPaths() {
        setUpInstance(true);

        given().contentType("application/json")
                .body(Map.of("paths", List.of()))
                .when().post("/api/ingest-file/expand")
                .then()
                .statusCode(400);
    }

    // ── /inspect ──

    @Test
    void shouldRejectInspectWithNoFiles() {
        setUpInstance(true);

        given().contentType("application/json")
                .body(Map.of("files", List.of()))
                .when().post("/api/ingest-file/inspect")
                .then()
                .statusCode(400);
    }

    // ── /submit ──

    @Test
    void shouldSubmitFilesToTheIngestBatcherQueue() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        String tableId = table.get(TABLE_ID);
        List<String> files = List.of(DATA_BUCKET + "/data/part-0.parquet");

        given().contentType("application/json")
                .body(Map.of("files", files, "tableIds", List.of(tableId), "method", "ingest_batcher"))
                .when().post("/api/ingest-file/submit")
                .then()
                .statusCode(201)
                .body("method", is("ingest_batcher"))
                .body("submitted[0].tableName", is("table-1"))
                .body("submitted[0].fileCount", is(1));

        String queueUrl = instanceProperties.get(INGEST_BATCHER_SUBMIT_QUEUE_URL);
        List<Message> messages = sqsClient.receiveMessage(builder -> builder.queueUrl(queueUrl).maxNumberOfMessages(10))
                .messages();
        assertThat(messages).hasSize(1);
        IngestBatcherSubmitRequest request = new IngestBatcherSubmitRequestSerDe().fromJson(messages.get(0).body());
        assertThat(request).isEqualTo(new IngestBatcherSubmitRequest("table-1", files));
    }

    @Test
    void shouldReturn404OnSubmitWhenBatcherNotEnabled() {
        InstanceProperties instanceProperties = setUpInstance(false);
        TableProperties table = createTable(instanceProperties, "table-1");

        given().contentType("application/json")
                .body(Map.of("files", List.of(DATA_BUCKET + "/f.parquet"),
                        "tableIds", List.of(table.get(TABLE_ID)), "method", "ingest_batcher"))
                .when().post("/api/ingest-file/submit")
                .then()
                .statusCode(404)
                .body("error", is("ingest_batcher_not_enabled"));
    }

    @Test
    void shouldRejectSubmitWithUnknownMethod() {
        setUpInstance(true);

        given().contentType("application/json")
                .body(Map.of("files", List.of(DATA_BUCKET + "/f.parquet"),
                        "tableIds", List.of("t-1"), "method", "magic"))
                .when().post("/api/ingest-file/submit")
                .then()
                .statusCode(400);
    }
}
