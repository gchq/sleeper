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
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamRecord;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.StreamViewType;
import org.junit.jupiter.api.Test;

import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;

import java.util.Map;

public class DynamoDBStreamTransactionLogEntryMapperTest {

    @Test
    void shouldConvertEntryToFromDynamoStreamEvent() {
        new DynamodbStreamRecord()
                .withEventName("INSERT")
                .withEventVersion("1.1")
                .withAwsRegion("eu-west-2")
                .withDynamodb(new StreamRecord()
                        .withKeys(Map.of(DynamoDBTransactionLogStore.TABLE_ID, new AttributeValue("3b31edf9"),
                                DynamoDBTransactionLogStore.TRANSACTION_NUMBER, new AttributeValue().withN("120")))
                        .withNewImage(Map.of(
                                DynamoDBTransactionLogStore.TABLE_ID, new AttributeValue("3b31edf9"),
                                DynamoDBTransactionLogStore.UPDATE_TIME, new AttributeValue().withN("1740587429688"),
                                DynamoDBTransactionLogStore.BODY, new AttributeValue(),
                                DynamoDBTransactionLogStore.TRANSACTION_NUMBER, new AttributeValue().withN("120"),
                                DynamoDBTransactionLogStore.TYPE, new AttributeValue("REPLACE_FILE_REFERENCES")))
                        .withSequenceNumber("12000000000006169888197")
                        .withSizeBytes(148709L)
                        .withStreamViewType(StreamViewType.NEW_IMAGE));
    }

    private DynamoDBStreamTransactionLogEntryMapper mapper() {
        return new DynamoDBStreamTransactionLogEntryMapper();
    }

}
