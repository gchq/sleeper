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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.statestore.DelegatingStateStore;

import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.table.TableProperty.FILE_IN_PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.FILE_LIFECYCLE_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;

/**
 * An implementation of {@link StateStore} that uses DynamoDB to store the state.
 */
public class DynamoDBStateStore extends DelegatingStateStore {
    public static final String FILE_NAME = DynamoDBFileInfoFormat.NAME;
    public static final String PARTITION_ID = DynamoDBPartitionFormat.ID;


    public DynamoDBStateStore(TableProperties tableProperties, AmazonDynamoDB dynamoDB) {
        this(tableProperties.get(FILE_IN_PARTITION_TABLENAME),
                tableProperties.get(FILE_LIFECYCLE_TABLENAME),
                tableProperties.get(PARTITION_TABLENAME),
                tableProperties.getSchema(),
                tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION),
                tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS),
                dynamoDB);
    }

    public DynamoDBStateStore(
            String fileInPartitionTablename,
            String fileLifecycleTablename,
            String partitionTablename,
            Schema schema,
            int garbageCollectorDelayBeforeDeletionInSeconds,
            boolean stronglyConsistentReads,
            AmazonDynamoDB dynamoDB) {
        super(DynamoDBFileInfoStore.builder()
                .dynamoDB(dynamoDB).schema(schema)
                .fileInPartitionTablename(fileInPartitionTablename)
                .fileLifecycleTablename(fileLifecycleTablename)
                .stronglyConsistentReads(stronglyConsistentReads)
                .garbageCollectorDelayBeforeDeletionInSeconds(garbageCollectorDelayBeforeDeletionInSeconds)
                .build(),
            DynamoDBPartitionStore.builder()
                .dynamoDB(dynamoDB).schema(schema)
                .tableName(partitionTablename)
                .stronglyConsistentReads(stronglyConsistentReads)
                .build());
    }
}
