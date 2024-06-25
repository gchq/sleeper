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
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotMetadataStore.LatestSnapshots;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class DynamoDBTransactionLogSnapshotMetadataStoreIT {
    @Container
    private static DynamoDBContainer dynamoDb = new DynamoDBContainer();
    private static AmazonDynamoDB dynamoDBClient;
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final DynamoDBTransactionLogSnapshotMetadataStore store = snapshotStore();

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, dynamoDb.getDynamoPort(), AmazonDynamoDBClientBuilder.standard());
    }

    @BeforeEach
    void setup() {
        new DynamoDBTransactionLogSnapshotMetadataStoreCreator(instanceProperties, dynamoDBClient).create();
    }

    @Nested
    @DisplayName("Files snapshots")
    class FilesSnapshots {
        @Test
        void shouldStoreFilesSnapshot() throws Exception {
            // Given / When
            store.saveSnapshot(filesSnapshot(1));

            // Then
            assertThat(store.getFilesSnapshots())
                    .containsExactly(filesSnapshot(1));
        }

        @Test
        void shouldRetrieveFilesSnapshotsLatestFirst() throws Exception {
            // Given / When
            store.saveSnapshot(filesSnapshot(1));
            store.saveSnapshot(filesSnapshot(2));
            store.saveSnapshot(filesSnapshot(3));

            // Then
            assertThat(store.getFilesSnapshots())
                    .containsExactly(
                            filesSnapshot(1),
                            filesSnapshot(2),
                            filesSnapshot(3));
        }

        @Test
        void shouldFailToAddFilesSnapshotIfSnapshotAlreadyExists() throws Exception {
            // Given / When
            store.saveSnapshot(filesSnapshot(1));

            // Then
            assertThatThrownBy(() -> store.saveSnapshot(filesSnapshot(1)))
                    .isInstanceOf(DuplicateSnapshotException.class);
        }
    }

    @Nested
    @DisplayName("Partitions snapshot")
    class PartitionsSnapshot {
        @Test
        void shouldStorePartitionsSnapshot() throws Exception {
            // Given / When
            store.saveSnapshot(partitionsSnapshot(1));

            // Then
            assertThat(store.getPartitionsSnapshots())
                    .containsExactly(partitionsSnapshot(1));
        }

        @Test
        void shouldRetrievePartitionsSnapshotsLatestFirst() throws Exception {
            // Given / When
            store.saveSnapshot(partitionsSnapshot(1));
            store.saveSnapshot(partitionsSnapshot(2));
            store.saveSnapshot(partitionsSnapshot(3));

            // Then
            assertThat(store.getPartitionsSnapshots())
                    .containsExactly(
                            partitionsSnapshot(1),
                            partitionsSnapshot(2),
                            partitionsSnapshot(3));
        }

        @Test
        void shouldFailToAddPartitionsSnapshotIfSnapshotAlreadyExists() throws Exception {
            // Given / When
            store.saveSnapshot(partitionsSnapshot(1));

            // Then
            assertThatThrownBy(() -> store.saveSnapshot(partitionsSnapshot(1)))
                    .isInstanceOf(DuplicateSnapshotException.class);
        }
    }

    @Test
    void shouldRetrieveLatestSnapshots() throws Exception {
        // Given / When
        store.saveSnapshot(filesSnapshot(1));
        store.saveSnapshot(filesSnapshot(2));
        store.saveSnapshot(filesSnapshot(3));
        store.saveSnapshot(partitionsSnapshot(1));
        store.saveSnapshot(partitionsSnapshot(2));
        store.saveSnapshot(partitionsSnapshot(3));

        // Then
        assertThat(store.getLatestSnapshots()).isEqualTo(
                new LatestSnapshots(
                        filesSnapshot(3),
                        partitionsSnapshot(3)));
    }

    @Test
    void shouldRetrieveLatestSnapshotsWhenNoFilesSnapshotsArePresent() throws Exception {
        // Given / When
        store.saveSnapshot(partitionsSnapshot(1));
        store.saveSnapshot(partitionsSnapshot(2));
        store.saveSnapshot(partitionsSnapshot(3));

        // Then
        assertThat(store.getLatestSnapshots()).isEqualTo(
                new LatestSnapshots(
                        null,
                        partitionsSnapshot(3)));
    }

    @Test
    void shouldRetrieveLatestSnapshotsWhenNoPartitionsSnapshotsArePresent() throws Exception {
        // Given / When
        store.saveSnapshot(filesSnapshot(1));
        store.saveSnapshot(filesSnapshot(2));
        store.saveSnapshot(filesSnapshot(3));

        // Then
        assertThat(store.getLatestSnapshots()).isEqualTo(
                new LatestSnapshots(
                        filesSnapshot(3),
                        null));
    }

    @Test
    void shouldRetrieveLatestSnapshotsWhenNoSnapshotsArePresent() throws Exception {
        // Given / When / Then
        assertThat(store.getLatestSnapshots()).isEqualTo(
                LatestSnapshots.empty());
    }

    @Test
    void shouldSaveAndLoadSnapshotsForMultipleTables() throws Exception {
        TableProperties table1 = createTable();
        TableProperties table2 = createTable();
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore1 = snapshotStore(table1);
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore2 = snapshotStore(table2);

        // When
        snapshotStore1.saveSnapshot(filesSnapshot(table1, 1));
        snapshotStore1.saveSnapshot(filesSnapshot(table1, 2));
        snapshotStore1.saveSnapshot(partitionsSnapshot(table1, 1));
        snapshotStore1.saveSnapshot(partitionsSnapshot(table1, 2));
        snapshotStore2.saveSnapshot(filesSnapshot(table2, 1));
        snapshotStore2.saveSnapshot(filesSnapshot(table2, 2));
        snapshotStore2.saveSnapshot(partitionsSnapshot(table2, 1));
        snapshotStore2.saveSnapshot(partitionsSnapshot(table2, 2));

        // Then
        assertThat(snapshotStore1.getFilesSnapshots())
                .containsExactly(
                        filesSnapshot(table1, 1),
                        filesSnapshot(table1, 2));
        assertThat(snapshotStore1.getPartitionsSnapshots())
                .containsExactly(
                        partitionsSnapshot(table1, 1),
                        partitionsSnapshot(table1, 2));
        assertThat(snapshotStore2.getFilesSnapshots())
                .containsExactly(
                        filesSnapshot(table2, 1),
                        filesSnapshot(table2, 2));
        assertThat(snapshotStore2.getPartitionsSnapshots())
                .containsExactly(
                        partitionsSnapshot(table2, 1),
                        partitionsSnapshot(table2, 2));
        assertThat(snapshotStore1.getLatestSnapshots()).isEqualTo(
                new LatestSnapshots(
                        filesSnapshot(table1, 2),
                        partitionsSnapshot(table1, 2)));
        assertThat(snapshotStore2.getLatestSnapshots()).isEqualTo(
                new LatestSnapshots(
                        filesSnapshot(table2, 2),
                        partitionsSnapshot(table2, 2)));
    }

    @Nested
    @DisplayName("Get expired snapshots")
    class GetExpiredSnapshots {
        @Test
        void shouldGetSnapshotsThatAreOldEnough() throws Exception {
            // Given
            DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(List.of(
                    Instant.parse("2024-04-24T15:45:00Z"),
                    Instant.parse("2024-04-25T15:15:00Z"),
                    Instant.parse("2024-04-26T15:45:00Z"),
                    Instant.parse("2024-04-26T16:00:00Z")).iterator()::next);
            snapshotStore.saveSnapshot(filesSnapshot(1));
            snapshotStore.saveSnapshot(filesSnapshot(2));
            snapshotStore.saveSnapshot(filesSnapshot(3));
            snapshotStore.saveSnapshot(filesSnapshot(4));

            // When / Then
            assertThat(snapshotStore.getExpiredSnapshots(Instant.parse("2024-04-25T16:30:00Z")))
                    .containsExactly(filesSnapshot(1), filesSnapshot(2));
        }

        @Test
        void shouldIgnoreLatestSnapshotEvenIfItIsOldEnough() throws Exception {
            // Given
            DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(() -> Instant.parse("2024-04-24T15:45:00Z"));
            snapshotStore.saveSnapshot(filesSnapshot(1));

            // When / Then
            assertThat(snapshotStore.getExpiredSnapshots(Instant.parse("2024-04-25T16:00:00Z")))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Get latest snapshots with a minimum age")
    class GetLatestSnapshotsWithMinAge {

        @Test
        void shouldGetLatestSnapshotsThatAreOldEnough() throws Exception {
            // Given
            DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(List.of(
                    Instant.parse("2024-04-24T15:25:00Z"),
                    Instant.parse("2024-04-24T15:35:00Z"),
                    Instant.parse("2024-04-24T15:45:00Z"),
                    Instant.parse("2024-04-24T15:55:00Z")).iterator()::next);
            snapshotStore.saveSnapshot(filesSnapshot(1));
            snapshotStore.saveSnapshot(partitionsSnapshot(1));
            snapshotStore.saveSnapshot(filesSnapshot(2));
            snapshotStore.saveSnapshot(partitionsSnapshot(2));

            // When / Then
            assertThat(snapshotStore.getLatestSnapshotsBefore(Instant.parse("2024-04-24T16:00:00Z")))
                    .isEqualTo(new LatestSnapshots(filesSnapshot(2), partitionsSnapshot(2)));
        }

        @Test
        void shouldGetLatestSnapshotsThatAreOldEnoughWhenSomeAreTooRecent() throws Exception {
            // Given
            DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(List.of(
                    Instant.parse("2024-04-24T15:25:00Z"),
                    Instant.parse("2024-04-24T15:35:00Z"),
                    Instant.parse("2024-04-24T15:45:00Z"),
                    Instant.parse("2024-04-24T15:55:00Z")).iterator()::next);
            snapshotStore.saveSnapshot(filesSnapshot(1));
            snapshotStore.saveSnapshot(partitionsSnapshot(1));
            snapshotStore.saveSnapshot(filesSnapshot(2));
            snapshotStore.saveSnapshot(partitionsSnapshot(2));

            // When / Then
            assertThat(snapshotStore.getLatestSnapshotsBefore(Instant.parse("2024-04-24T15:40:00Z")))
                    .isEqualTo(new LatestSnapshots(filesSnapshot(1), partitionsSnapshot(1)));
        }
    }

    private TableProperties createTable() {
        return createTestTableProperties(instanceProperties, schema);
    }

    private DynamoDBTransactionLogSnapshotMetadataStore snapshotStore() {
        return snapshotStore(Instant::now);
    }

    private DynamoDBTransactionLogSnapshotMetadataStore snapshotStore(Supplier<Instant> timeSupplier) {
        return snapshotStore(tableProperties, timeSupplier);
    }

    private DynamoDBTransactionLogSnapshotMetadataStore snapshotStore(TableProperties tableProperties) {
        return snapshotStore(tableProperties, Instant::now);
    }

    private DynamoDBTransactionLogSnapshotMetadataStore snapshotStore(TableProperties tableProperties, Supplier<Instant> timeSupplier) {
        return new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamoDBClient, timeSupplier);
    }

    private TransactionLogSnapshotMetadata filesSnapshot(long transactionNumber) {
        return filesSnapshot(tableProperties, transactionNumber);
    }

    private TransactionLogSnapshotMetadata filesSnapshot(TableProperties tableProperties, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forFiles(tableProperties.get(TABLE_ID), transactionNumber);
    }

    private TransactionLogSnapshotMetadata partitionsSnapshot(long transactionNumber) {
        return partitionsSnapshot(tableProperties, transactionNumber);
    }

    private TransactionLogSnapshotMetadata partitionsSnapshot(TableProperties tableProperties, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forPartitions(tableProperties.get(TABLE_ID), transactionNumber);
    }
}
