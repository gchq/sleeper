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

import org.junit.jupiter.api.Test;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TransactionLogTransactionDeleterIT extends TransactionLogStateStoreOneTableTestBase {
    private final Schema schema = schemaWithKey("key", new StringType());

    @Test
    void shouldDeleteOldTransactions() throws Exception {
        // Given
        createSnapshotWithFreshStateAtTransactionNumber(1, stateStore -> {
            stateStore.initialise();
            stateStore.addFile(factory.rootFile("file1.parquet", 100L));
        });
        initialiseWithSchema(schema);
        store.addFile(factory.rootFile("file2.parquet", 123L));
        store.addFile(factory.rootFile("file3.parquet", 456L));
        store.addFile(factory.rootFile("file4.parquet", 789L));
    }
}
