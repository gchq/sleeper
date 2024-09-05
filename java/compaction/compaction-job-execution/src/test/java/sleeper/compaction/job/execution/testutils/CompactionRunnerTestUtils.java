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
package sleeper.compaction.job.execution.testutils;

import sleeper.compaction.job.CompactionJob;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.Type;
import sleeper.core.statestore.StateStore;

import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;

public class CompactionRunnerTestUtils {

    private CompactionRunnerTestUtils() {
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

    public static void assignJobIdToInputFiles(StateStore stateStore, CompactionJob job) throws Exception {
        assignJobIdsToInputFiles(stateStore, job);
    }

    public static void assignJobIdsToInputFiles(StateStore stateStore, CompactionJob... jobs) throws Exception {
        stateStore.assignJobIds(Stream.of(jobs)
                .map(job -> assignJobOnPartitionToFiles(job.getId(), job.getPartitionId(), job.getInputFiles()))
                .collect(toUnmodifiableList()));
    }
}
