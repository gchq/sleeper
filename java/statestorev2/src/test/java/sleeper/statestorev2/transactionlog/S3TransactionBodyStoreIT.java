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
package sleeper.statestorev2.transactionlog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.log.StoreTransactionBodyResult;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class S3TransactionBodyStoreIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    String tableId = tableProperties.get(TABLE_ID);

    @BeforeEach
    void setUp() {
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
    }

    @Nested
    @DisplayName("Support all transactions")
    class SupportAllTransactions {

        TransactionBodyStore store = storeAllTransactions();

        @Test
        void shouldSaveAndLoadPartitionTransactionByTableProperties() {
            // Given
            PartitionTransaction transaction = new InitialisePartitionsTransaction(
                    new PartitionsBuilder(tableProperties).singlePartition("root").buildList());

            // When
            StateStoreTransaction<?> found = saveAndLoad(store, transaction);

            // Then
            assertThat(found).isEqualTo(transaction);
        }

        @Test
        void shouldSaveAndLoadPartitionTransactionByTableId() {
            // Given
            PartitionTransaction transaction = new InitialisePartitionsTransaction(
                    new PartitionsBuilder(tableProperties).singlePartition("root").buildList());

            // When
            StateStoreTransaction<?> found = saveAndLoad(store, transaction);

            // Then
            assertThat(found).isEqualTo(transaction);
        }
    }

    @Nested
    @DisplayName("Support only file transactions")
    class SupportOnlyFileTransactions {

        TransactionBodyStore store = storeAllFileTransactions();

        @Test
        void shouldStoreFileTransaction() {
            // Given
            PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
            FileReferenceTransaction transaction = AddFilesTransaction.fromReferences(List.of(
                    FileReferenceFactory.from(partitions).rootFile("test.parquet", 100)));

            // When
            StateStoreTransaction<?> found = saveAndLoad(store, transaction);

            // Then
            assertThat(found).isEqualTo(transaction);
        }

        @Test
        void shouldFailPartitionTransaction() {
            // Given
            PartitionTransaction transaction = new InitialisePartitionsTransaction(
                    new PartitionsBuilder(tableProperties).singlePartition("root").buildList());

            // When / Then
            assertThatThrownBy(() -> saveGetKey(store, transaction))
                    .isInstanceOf(UnsupportedOperationException.class);
            String key = saveGetKey(storeAllTransactions(), transaction);
            assertThatThrownBy(() -> store.getBody(key, tableId, TransactionType.INITIALISE_PARTITIONS))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("Retain serialised transaction")
    class RetainSerialisedTransaction {

        TransactionBodyStore store = storeTransactionsWithJsonLength(300);

        @Test
        void shouldNotStoreTransactionSmallerThanThreshold() {
            // Given
            PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
            AddFilesTransaction transaction = AddFilesTransaction.fromReferences(List.of(
                    FileReferenceFactory.from(partitions).rootFile("test.parquet", 100)));

            // When
            StoreTransactionBodyResult result = store.storeIfTooBig(tableId, transaction);

            // Then
            assertThat(result.getBodyKey()).isEmpty();
            assertThat(result.getSerialisedTransaction())
                    .map(json -> (AddFilesTransaction) TransactionSerDe.forFileTransactions().toTransaction(TransactionType.ADD_FILES, json))
                    .contains(transaction);
        }

        @Test
        void shouldStoreTransactionLargerThanThreshold() {
            // Given
            PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
            AddFilesTransaction transaction = AddFilesTransaction.fromReferences(List.of(
                    FileReferenceFactory.from(partitions).rootFile("test1.parquet", 100),
                    FileReferenceFactory.from(partitions).rootFile("test2.parquet", 200),
                    FileReferenceFactory.from(partitions).rootFile("test3.parquet", 300)));

            // When
            StoreTransactionBodyResult result = store.storeIfTooBig(tableId, transaction);

            // Then
            assertThat(result.getBodyKey()).isNotEmpty();
            assertThat(result.getSerialisedTransaction()).isEmpty();
        }
    }

    private TransactionBodyStore storeAllTransactions() {
        return new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties), 0);
    }

    private TransactionBodyStore storeAllFileTransactions() {
        return new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forFileTransactions(), 0);
    }

    private TransactionBodyStore storeTransactionsWithJsonLength(int jsonLengthToStore) {
        return new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties), jsonLengthToStore);
    }

    private StateStoreTransaction<?> saveAndLoad(TransactionBodyStore store, StateStoreTransaction<?> transaction) {
        StoreTransactionBodyResult found = store.storeIfTooBig(tableId, transaction);
        String foundKey = found.getBodyKey().orElseThrow();
        return store.getBody(foundKey, tableId, TransactionType.getType(transaction));
    }

    private String saveGetKey(TransactionBodyStore store, StateStoreTransaction<?> transaction) {
        return store.storeIfTooBig(tableId, transaction).getBodyKey().orElseThrow();
    }

}
