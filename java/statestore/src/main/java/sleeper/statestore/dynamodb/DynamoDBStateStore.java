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
import sleeper.core.schema.Schema;
import sleeper.core.statestore.DelegatingStateStore;

import java.time.Instant;

import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;

/**
 * An implementation of StateStore that uses DynamoDB to store the state.
 */
public class DynamoDBStateStore extends DelegatingStateStore {

    public static final String FILE_NAME = DynamoDBFileInfoFormat.NAME;
    public static final String PARTITION_ID = DynamoDBPartitionFormat.ID;
    public static final String TABLE_NAME = "TableName";

    public DynamoDBStateStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamoDB) {
        this(tableProperties.get(ACTIVE_FILEINFO_TABLENAME),
                tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME),
                tableProperties.get(PARTITION_TABLENAME),
                tableProperties.getSchema(),
                tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION),
                tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS),
                dynamoDB);
    }

    public DynamoDBStateStore(
            String activeFileInfoTablename,
            String readyForGCFileInfoTablename,
            String partitionTablename,
            Schema schema,
            int garbageCollectorDelayBeforeDeletionInMinutes,
            boolean stronglyConsistentReads,
            AmazonDynamoDB dynamoDB) {
        super(DynamoDBFileInfoStore.builder()
                .dynamoDB(dynamoDB).schema(schema)
                .activeTablename(activeFileInfoTablename).readyForGCTablename(readyForGCFileInfoTablename)
                .stronglyConsistentReads(stronglyConsistentReads)
                .garbageCollectorDelayBeforeDeletionInMinutes(garbageCollectorDelayBeforeDeletionInMinutes)
                .build(), DynamoDBPartitionStore.builder()
                .dynamoDB(dynamoDB).schema(schema)
                .tableName(partitionTablename).stronglyConsistentReads(stronglyConsistentReads)
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
