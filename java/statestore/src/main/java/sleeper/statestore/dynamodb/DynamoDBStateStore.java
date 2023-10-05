/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.DelegatingStateStore;

import java.time.Instant;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;

/**
 * An implementation of StateStore that uses DynamoDB to store the state.
 */
public class DynamoDBStateStore extends DelegatingStateStore {

    public static final String FILE_NAME = DynamoDBFileInfoFormat.NAME;
    public static final String PARTITION_ID = DynamoDBPartitionFormat.ID;
    public static final String PARTITION_AND_FILENAME = DynamoDBFileInfoFormat.PARTITION_AND_FILENAME;
    public static final String TABLE_NAME = "TableName";

    public DynamoDBStateStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamoDB) {
        super(DynamoDBFileInfoStore.builder()
                        .dynamoDB(dynamoDB).schema(tableProperties.getSchema())
                        .activeTableName(instanceProperties.get(ACTIVE_FILEINFO_TABLENAME))
                        .readyForGCTableName(instanceProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                        .sleeperTableName(tableProperties.get(TableProperty.TABLE_NAME))
                        .stronglyConsistentReads(tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS))
                        .garbageCollectorDelayBeforeDeletionInMinutes(tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION))
                        .build(),
                DynamoDBPartitionStore.builder()
                        .dynamoDB(dynamoDB).schema(tableProperties.getSchema())
                        .dynamoTableName(instanceProperties.get(PARTITION_TABLENAME))
                        .sleeperTableName(tableProperties.get(TableProperty.TABLE_NAME))
                        .stronglyConsistentReads(tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS))
                        .build());
    }

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param now Time to set to be the current time
     */
    public void fixTime(Instant now) {
        ((DynamoDBFileInfoStore) fileInfoStore).fixTime(now);
    }
}
