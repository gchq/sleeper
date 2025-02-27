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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.Record;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamViewType;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.testutils.InMemoryTransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;

public class DynamoDBStreamTransactionLogEntryMapperTest {
    Schema schema = schemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    InMemoryTransactionBodyStore transactionBodyStore = new InMemoryTransactionBodyStore();

    @Test
    void shouldReadEntryWithBodyFromDynamodbStreamRecord() {
        // Given
        tableProperties.set(TABLE_ID, "3b31edf9");
        FileReference outputFile = FileReferenceFactory.from(partitions).rootFile("output.parquet", 100);
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(List.of(
                replaceJobFileReferences("test-job", List.of("input.parquet"), outputFile)));
        Record record = new DynamodbStreamRecord()
                .withEventName("INSERT")
                .withEventVersion("1.1")
                .withAwsRegion("eu-west-2")
                .withDynamodb(new StreamRecord()
                        .withKeys(Map.of(DynamoDBTransactionLogStore.TABLE_ID, new AttributeValue("3b31edf9"),
                                DynamoDBTransactionLogStore.TRANSACTION_NUMBER, new AttributeValue().withN("120")))
                        .withNewImage(Map.of(
                                DynamoDBTransactionLogStore.TABLE_ID, new AttributeValue("3b31edf9"),
                                DynamoDBTransactionLogStore.UPDATE_TIME, new AttributeValue().withN("1740587429688"),
                                DynamoDBTransactionLogStore.BODY, new AttributeValue(new TransactionSerDe(schema).toJson(transaction)),
                                DynamoDBTransactionLogStore.TRANSACTION_NUMBER, new AttributeValue().withN("120"),
                                DynamoDBTransactionLogStore.TYPE, new AttributeValue("REPLACE_FILE_REFERENCES")))
                        .withSequenceNumber("12000000000006169888197")
                        .withSizeBytes(148709L)
                        .withStreamViewType(StreamViewType.NEW_IMAGE));

        // When
        TransactionLogEntry entry = mapper().toTransactionLogEntry(record);

        // Then
        assertThat(entry).isEqualTo(new TransactionLogEntry(120, Instant.parse("2025-02-26T16:30:29.688Z"), transaction));
    }

    @Test
    void shouldReadEntryWithBodyKeyFromDynamodbStreamRecord() {
        // Given
        tableProperties.set(TABLE_ID, "3b31edf9");
        Record record = new DynamodbStreamRecord()
                .withEventName("INSERT")
                .withEventVersion("1.1")
                .withAwsRegion("eu-west-2")
                .withDynamodb(new StreamRecord()
                        .withKeys(Map.of(DynamoDBTransactionLogStore.TABLE_ID, new AttributeValue("3b31edf9"),
                                DynamoDBTransactionLogStore.TRANSACTION_NUMBER, new AttributeValue().withN("120")))
                        .withNewImage(Map.of(
                                DynamoDBTransactionLogStore.TABLE_ID, new AttributeValue("3b31edf9"),
                                DynamoDBTransactionLogStore.UPDATE_TIME, new AttributeValue().withN("1740587429688"),
                                DynamoDBTransactionLogStore.BODY_S3_KEY, new AttributeValue("transaction/test"),
                                DynamoDBTransactionLogStore.TRANSACTION_NUMBER, new AttributeValue().withN("120"),
                                DynamoDBTransactionLogStore.TYPE, new AttributeValue("REPLACE_FILE_REFERENCES")))
                        .withSequenceNumber("12000000000006169888197")
                        .withSizeBytes(148709L)
                        .withStreamViewType(StreamViewType.NEW_IMAGE));

        // When
        TransactionLogEntry entry = mapper().toTransactionLogEntry(record);

        // Then
        assertThat(entry).isEqualTo(new TransactionLogEntry(120, Instant.parse("2025-02-26T16:30:29.688Z"), TransactionType.REPLACE_FILE_REFERENCES, "transaction/test"));
    }

    private DynamoDBStreamTransactionLogEntryMapper mapper() {
        return new DynamoDBStreamTransactionLogEntryMapper(TransactionSerDeProvider.forOneTable(tableProperties));
    }

}
