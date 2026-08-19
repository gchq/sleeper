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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.IngestQueue;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStoreCreator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static sleeper.core.properties.instance.BatcherProperty.INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

@QuarkusTest
@TestProfile(IngestBatcherResourceIT.Profile.class)
class IngestBatcherResourceIT {

    static final String INSTANCE_ID = "batcher-it";
    static final String ACCOUNT_NAME = "test-account";

    // Fixed reference point so ordering is deterministic without hard-coding a calendar year.
    private static final Instant BASE_TIME = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);

    @Inject
    S3Client s3Client;
    @Inject
    DynamoDbClient dynamoDbClient;

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "sleeper.instance.id", INSTANCE_ID,
                    "sleeper.account.name", ACCOUNT_NAME);
        }
    }

    @BeforeEach
    void clearState() {
        // The localstack container is shared across tests, so wipe DynamoDB tables and S3 buckets between them.
        dynamoDbClient.listTables().tableNames()
                .forEach(name -> dynamoDbClient.deleteTable(builder -> builder.tableName(name)));
        s3Client.listBuckets().buckets().forEach(bucket -> {
            String name = bucket.name();
            s3Client.listObjectsV2Paginator(builder -> builder.bucket(name)).contents().forEach(obj -> s3Client
                    .deleteObject(builder -> builder.bucket(name).key(obj.key())));
            s3Client.deleteBucket(builder -> builder.bucket(name));
        });
    }

    private InstanceProperties setUpInstance(boolean batcherEnabled) {
        InstanceProperties instanceProperties = createTestInstancePropertiesWithId(INSTANCE_ID);
        instanceProperties.setEnumList(OPTIONAL_STACKS,
                batcherEnabled ? List.of(OptionalStack.IngestBatcherStack) : List.of());
        instanceProperties.set(INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES, "5");
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(instanceProperties.get(CONFIG_BUCKET))
                .build());
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

    private void addFile(InstanceProperties instanceProperties, List<TableProperties> tables,
            String path, String tableId, Instant receivedTime, String jobId) {
        DynamoDBIngestBatcherStoreCreator.create(instanceProperties, dynamoDbClient);
        new DynamoDBIngestBatcherStore(dynamoDbClient, instanceProperties, new FixedTablePropertiesProvider(tables))
                .addFile(IngestBatcherTrackedFile.builder()
                        .file(path)
                        .fileSizeBytes(1024)
                        .tableId(tableId)
                        .receivedTime(receivedTime)
                        .jobId(jobId)
                        .build());
    }

    private static Instant daysFromBase(int days) {
        return BASE_TIME.plus(Duration.ofDays(days));
    }

    @Test
    void shouldReturn404WhenBatcherNotEnabled() {
        setUpInstance(false);

        given()
                .when().get("/api/ingest-batcher/files")
                .then()
                .statusCode(404)
                .body("error", is("ingest_batcher_not_enabled"));
    }

    @Test
    void shouldReturnPendingFilesOldestFirstByDefault() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        String tableId = table.get(TABLE_ID);
        addFile(instanceProperties, List.of(table), "s3a://bucket/newer.parquet", tableId, daysFromBase(1), null);
        addFile(instanceProperties, List.of(table), "s3a://bucket/older.parquet", tableId, daysFromBase(0), null);

        given()
                .when().get("/api/ingest-batcher/files")
                .then()
                .statusCode(200)
                .body("files.file", contains("s3a://bucket/older.parquet", "s3a://bucket/newer.parquet"))
                .body("hasMore", is(false));
    }

    @Test
    void shouldExcludeAssignedFilesFromPendingButIncludeInAllMode() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        String tableId = table.get(TABLE_ID);
        addFile(instanceProperties, List.of(table), "s3a://bucket/pending.parquet", tableId, daysFromBase(0), null);
        addFile(instanceProperties, List.of(table), "s3a://bucket/assigned.parquet", tableId, daysFromBase(1), "job-1");

        given()
                .when().get("/api/ingest-batcher/files?mode=pending")
                .then()
                .statusCode(200)
                .body("files.file", contains("s3a://bucket/pending.parquet"));

        given()
                .when().get("/api/ingest-batcher/files?mode=all")
                .then()
                .statusCode(200)
                .body("files", hasSize(2));
    }

    @Test
    void shouldFilterByTableAndPath() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table1 = createTable(instanceProperties, "table-1");
        TableProperties table2 = createTable(instanceProperties, "table-2");
        String tableId1 = table1.get(TABLE_ID);
        String tableId2 = table2.get(TABLE_ID);
        addFile(instanceProperties, List.of(table1, table2), "s3a://bucket/alpha.parquet", tableId1, daysFromBase(0), null);
        addFile(instanceProperties, List.of(table1, table2), "s3a://bucket/beta.parquet", tableId2, daysFromBase(1), null);

        given()
                .when().get("/api/ingest-batcher/files?tableId=" + tableId1)
                .then()
                .statusCode(200)
                .body("files.file", contains("s3a://bucket/alpha.parquet"));

        given()
                .when().get("/api/ingest-batcher/files?path=BETA")
                .then()
                .statusCode(200)
                .body("files.file", contains("s3a://bucket/beta.parquet"));
    }

    @Test
    void shouldReturnWholePrefixUpToLimit() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        String tableId = table.get(TABLE_ID);
        for (int i = 0; i < 3; i++) {
            addFile(instanceProperties, List.of(table), "s3a://bucket/file-" + i + ".parquet", tableId, daysFromBase(i), null);
        }

        given()
                .when().get("/api/ingest-batcher/files?limit=2")
                .then()
                .statusCode(200)
                .body("files.file", contains("s3a://bucket/file-0.parquet", "s3a://bucket/file-1.parquet"))
                .body("hasMore", is(true));

        given()
                .when().get("/api/ingest-batcher/files?limit=4")
                .then()
                .statusCode(200)
                .body("files", hasSize(3))
                .body("hasMore", is(false));
    }

    @Test
    void shouldReturnTableBatchConfigWhenTableIdGiven() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        table.set(INGEST_BATCHER_MIN_JOB_FILES, "7");
        table.setEnum(INGEST_BATCHER_INGEST_QUEUE, IngestQueue.BULK_IMPORT_EMR);
        TablePropertiesStore store = S3TableProperties.createStore(instanceProperties, s3Client, dynamoDbClient);
        store.save(table);
        String tableId = table.get(TABLE_ID);

        given()
                .when().get("/api/ingest-batcher/config?tableId=" + tableId)
                .then()
                .statusCode(200)
                .body("tableId", is(tableId))
                .body("minJobFiles", is("7"))
                .body("ingestQueue", is("bulk_import_emr"));
    }

    @Test
    void shouldReturnDefaultBatchConfigWhenNoTableId() {
        setUpInstance(true);

        given()
                .when().get("/api/ingest-batcher/config")
                .then()
                .statusCode(200)
                .body("tableId", is(nullValue()))
                .body("jobCreationPeriodMinutes", is("5"));
    }

}