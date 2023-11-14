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

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;

/**
 * An implementation of StateStore that uses DynamoDB to store the state.
 */
public class DynamoDBStateStore extends DelegatingStateStore {

    public static final String FILE_NAME = DynamoDBFileInfoFormat.FILENAME;
    public static final String PARTITION_ID = DynamoDBPartitionFormat.ID;
    public static final String PARTITION_ID_AND_FILENAME = DynamoDBFileInfoFormat.PARTITION_ID_AND_FILENAME;
    public static final String TABLE_ID = "TableId";
    public static final int DEFAULT_PAGE_LIMIT = 1024 * 1024;

    public DynamoDBStateStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamoDB) {
        this(instanceProperties, tableProperties, dynamoDB, DEFAULT_PAGE_LIMIT);
    }

    public DynamoDBStateStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamoDB, int pageLimit) {
        super(DynamoDBFileInfoStore.builder()
                        .dynamoDB(dynamoDB)
                        .activeTableName(instanceProperties.get(ACTIVE_FILEINFO_TABLENAME))
                        .readyForGCTableName(instanceProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                        .sleeperTableId(tableProperties.get(TableProperty.TABLE_ID))
                        .stronglyConsistentReads(tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS))
                        .garbageCollectorDelayBeforeDeletionInMinutes(tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION))
                        .pageLimit(pageLimit)
                        .build(),
                DynamoDBPartitionStore.builder()
                        .dynamoDB(dynamoDB).schema(tableProperties.getSchema())
                        .dynamoTableName(instanceProperties.get(PARTITION_TABLENAME))
                        .sleeperTableId(tableProperties.get(TableProperty.TABLE_ID))
                        .stronglyConsistentReads(tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS))
                        .build());
    }
}
