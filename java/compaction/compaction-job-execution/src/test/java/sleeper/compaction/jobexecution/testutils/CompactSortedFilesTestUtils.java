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
package sleeper.compaction.jobexecution.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.assertj.core.groups.Tuple;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.jobexecution.CompactSortedFiles;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.tuple;

public class CompactSortedFilesTestUtils {

    private CompactSortedFilesTestUtils() {
    }

    public static Schema createSchemaWithTypesForKeyAndTwoValues(PrimitiveType keyType, Type value1Type, Type value2Type) {
        return createSchemaWithTwoTypedValuesAndKeyFields(value1Type, value2Type, new Field("key", keyType));
    }

    public static Schema createSchemaWithTwoTypedValuesAndKeyFields(Type value1Type, Type value2Type, Field... rowKeyFields) {
        return Schema.builder()
                .rowKeyFields(rowKeyFields)
                .valueFields(new Field("value1", value1Type), new Field("value2", value2Type))
                .build();
    }

    public static Schema createSchemaWithKeyTimestampValue() {
        return createSchemaWithKeyTimestampValue(new Field("key", new LongType()));
    }

    public static Schema createSchemaWithKeyTimestampValue(Field key) {
        return Schema.builder()
                .rowKeyFields(key)
                .valueFields(new Field("timestamp", new LongType()), new Field("value", new LongType()))
                .build();
    }

    public static StateStore createInitStateStore(String tablenameStub, Schema schema, AmazonDynamoDB dynamoDBClient) throws StateStoreException {
        StateStore dynamoStateStore = createStateStore(tablenameStub, schema, dynamoDBClient);
        dynamoStateStore.initialise();
        return dynamoStateStore;
    }

    public static StateStore createStateStore(String tablenameStub, Schema schema, AmazonDynamoDB dynamoDBClient) throws StateStoreException {
        return new DynamoDBStateStoreCreator(tablenameStub, schema, dynamoDBClient).create();
    }

    public static CompactSortedFiles createCompactSortedFiles(
            Schema schema, CompactionJob compactionJob, StateStore stateStore, String taskId) {
        return createCompactSortedFiles(schema, compactionJob, stateStore, CompactionJobStatusStore.NONE, taskId);
    }

    public static CompactSortedFiles createCompactSortedFiles(
            Schema schema, CompactionJob compactionJob, StateStore stateStore, CompactionJobStatusStore jobStatusStore, String taskId) {
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        return new CompactSortedFiles(instanceProperties, tableProperties, ObjectFactory.noUserJars(),
                compactionJob, stateStore, jobStatusStore, taskId);
    }

    public static void assertReadyForGC(StateStore dynamoStateStore, FileInfo... files) {
        assertReadyForGC(dynamoStateStore, Arrays.asList(files));
    }

    public static void assertReadyForGC(StateStore dynamoStateStore, List<FileInfo> files) {
        try {
            assertThat(dynamoStateStore.getReadyForGCFiles()).toIterable()
                    .extracting(
                            FileInfo::getFilename,
                            FileInfo::getRowKeyTypes,
                            FileInfo::getPartitionId,
                            FileInfo::getFileStatus)
                    .containsExactlyInAnyOrder(files.stream()
                            .map(file -> tuple(file.getFilename(), file.getRowKeyTypes(), file.getPartitionId(),
                                    FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION))
                            .toArray(Tuple[]::new));
        } catch (StateStoreException e) {
            fail("StateStoreException generated: " + e.getMessage());
        }
    }
}
