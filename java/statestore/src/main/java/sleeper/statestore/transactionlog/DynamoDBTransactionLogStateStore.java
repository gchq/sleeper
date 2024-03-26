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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;

public class DynamoDBTransactionLogStateStore extends TransactionLogStateStore {
    public static final String TABLE_ID = "TABLE_ID";
    public static final String TRANSACTION_NUMBER = "TRANSACTION_NUMBER";

    public DynamoDBTransactionLogStateStore(
            InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamoDB) {
        super(tableProperties.getSchema(), new DynamoDBTransactionLogStore(instanceProperties, tableProperties, dynamoDB));
    }

}
