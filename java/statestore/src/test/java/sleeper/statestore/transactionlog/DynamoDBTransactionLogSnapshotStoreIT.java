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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.dynamodb.tools.DynamoDBContainer;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class DynamoDBTransactionLogSnapshotStoreIT {
    @Container
    private static DynamoDBContainer dynamoDb = new DynamoDBContainer();
    private static AmazonDynamoDB dynamoDBClient;
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private TransactionLogSnapshotStore store = snapshotStore();

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, dynamoDb.getDynamoPort(), AmazonDynamoDBClientBuilder.standard());
    }

    @BeforeEach
    void setup() {
        new DynamoDBTransactionLogSnapshotStoreCreator(instanceProperties, dynamoDBClient).create();
    }

    @Nested
    @DisplayName("Files snapshots")
    class FilesSnapshots {
        @Test
        void shouldStoreFilesSnapshot() throws Exception {
            // Given / When
            store.saveFiles("snapshot/1-files.parquet", 1);

            // Then
            assertThat(store.getFilesSnapshots())
                    .containsExactly(filesSnapshot("snapshot/1-files.parquet", 1));
        }

        @Test
        void shouldRetrieveFilesSnapshotsLatestFirst() throws Exception {
            // Given / When
            store.saveFiles("snapshot/1-files.parquet", 1);
            store.saveFiles("snapshot/2-files.parquet", 2);
            store.saveFiles("snapshot/3-files.parquet", 3);

            // Then
            assertThat(store.getFilesSnapshots())
                    .containsExactly(
                            filesSnapshot("snapshot/3-files.parquet", 3),
                            filesSnapshot("snapshot/2-files.parquet", 2),
                            filesSnapshot("snapshot/1-files.parquet", 1));
        }
    }

    @Nested
    @DisplayName("Partitions snapshot")
    class PartitionsSnapshot {
        @Test
        void shouldStorePartitionsSnapshot() throws Exception {
            // Given / When
            store.savePartitions("snapshot/1-partitions.parquet", 1);

            // Then
            assertThat(store.getFilesSnapshots()).isEmpty();
            assertThat(store.getPartitionsSnapshots())
                    .containsExactly(partitionSnapshot("snapshot/1-partitions.parquet", 1));
        }

        @Test
        void shouldRetrievePartitionsSnapshotsLatestFirst() throws Exception {
            // Given / When
            store.savePartitions("snapshot/1-partitions.parquet", 1);
            store.savePartitions("snapshot/2-partitions.parquet", 2);
            store.savePartitions("snapshot/3-partitions.parquet", 3);

            // Then
            assertThat(store.getPartitionsSnapshots())
                    .containsExactly(
                            partitionSnapshot("snapshot/3-partitions.parquet", 3),
                            partitionSnapshot("snapshot/2-partitions.parquet", 2),
                            partitionSnapshot("snapshot/1-partitions.parquet", 1));
        }
    }

    private TransactionLogSnapshotStore snapshotStore() {
        return new DynamoDBTransactionLogSnapshotStore(instanceProperties, tableProperties, dynamoDBClient, Instant::now);
    }

    private TransactionLogSnapshot filesSnapshot(String path, long transactionNumber) {
        return TransactionLogSnapshot.forFiles(path, transactionNumber);
    }

    private TransactionLogSnapshot partitionSnapshot(String path, long transactionNumber) {
        return TransactionLogSnapshot.forPartitions(path, transactionNumber);
    }
}
