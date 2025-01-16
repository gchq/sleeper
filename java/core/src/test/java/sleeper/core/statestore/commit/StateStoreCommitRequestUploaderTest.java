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
package sleeper.core.statestore.commit;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.transactionlog.InMemoryTransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class StateStoreCommitRequestUploaderTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final String tableId = tableProperties.get(TABLE_ID);
    private final InMemoryTransactionBodyStore transactionBodyStore = new InMemoryTransactionBodyStore();

    @Test
    void shouldUploadTransactionWhenTooBig() {
        // Given
        StateStoreCommitRequest request = StateStoreCommitRequest.create(tableId, new ClearFilesTransaction());
        StateStoreCommitRequestUploader uploader = uploaderWithMaxLength(10);

        // When
        String resultJson = uploader.serialiseAndUploadIfTooBig(request);

        // Then
        StateStoreCommitRequest found = new StateStoreCommitRequestSerDe(tableProperties).fromJson(resultJson);
        assertThat(found)
                .isEqualTo(StateStoreCommitRequest.create(tableId, found.getBodyKey(), TransactionType.CLEAR_FILES));
        assertThat(transactionBodyStore.<ClearFilesTransaction>getBody(found.getBodyKey(), TransactionType.CLEAR_FILES))
                .isEqualTo(new ClearFilesTransaction());
    }

    @Test
    void shouldNotUploadTransactionWhenNotTooBig() {
        // Given
        StateStoreCommitRequest request = StateStoreCommitRequest.create(tableId, new ClearFilesTransaction());
        StateStoreCommitRequestUploader uploader = uploaderWithMaxLength(100);

        // When
        String resultJson = uploader.serialiseAndUploadIfTooBig(request);

        // Then
        assertThat(new StateStoreCommitRequestSerDe(tableProperties).fromJson(resultJson))
                .isEqualTo(request);
        assertThat(transactionBodyStore.getTransactionByKey()).isEmpty();
    }

    private StateStoreCommitRequestUploader uploaderWithMaxLength(int maxLength) {
        return new StateStoreCommitRequestUploader(new FixedTablePropertiesProvider(tableProperties), transactionBodyStore, maxLength);
    }

}
