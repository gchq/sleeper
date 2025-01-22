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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.PartitionTransaction;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.TransactionBodyStoreProvider;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.statestore.testutil.LocalStackTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class S3TransactionBodyStoreIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    String tableId = tableProperties.get(TABLE_ID);

    @BeforeEach
    void setUp() {
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
    }

    @Nested
    @DisplayName("Support all transactions")
    class SupportAllTransactions {

        private final TransactionBodyStoreProvider provider = S3TransactionBodyStore.createProvider(instanceProperties, s3Client);
        private final PartitionTransaction transaction = new InitialisePartitionsTransaction(
                new PartitionsBuilder(tableProperties.getSchema()).singlePartition("root").buildList());

        @Test
        void shouldSaveAndLoadPartitionTransactionByTableProperties() {
            // Given
            TransactionBodyStore store = provider.getTransactionBodyStore(tableProperties);
            String key = TransactionBodyStore.createObjectKey(tableProperties);

            // When
            store.store(key, transaction);
            StateStoreTransaction<?> found = store.getBody(key, TransactionType.INITIALISE_PARTITIONS);

            // Then
            assertThat(found).isEqualTo(transaction);
        }

        @Test
        @Disabled
        void shouldSaveAndLoadPartitionTransactionByTableId() {
            // Given
            TransactionBodyStore store = provider.getTransactionBodyStore(tableId);
            String key = TransactionBodyStore.createObjectKey(tableId);

            // When
            store.store(key, transaction);
            StateStoreTransaction<?> found = store.getBody(key, TransactionType.INITIALISE_PARTITIONS);

            // Then
            assertThat(found).isEqualTo(transaction);
        }
    }

    @Nested
    @DisplayName("Support only file transactions")
    class SupportOnlyFileTransactions {

    }

}
