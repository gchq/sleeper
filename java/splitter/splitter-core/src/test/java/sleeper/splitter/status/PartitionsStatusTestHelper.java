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

package sleeper.splitter.status;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.inmemory.StateStoreTestBuilder;

import java.util.List;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

public class PartitionsStatusTestHelper {
    private static final Schema DEFAULT_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .build();

    private PartitionsStatusTestHelper() {
    }

    public static StateStoreTestBuilder createRootPartitionWithNoChildren() {
        return StateStoreTestBuilder.from(createPartitionsBuilder().singlePartition("root"));
    }

    public static StateStoreTestBuilder createRootPartitionWithTwoChildren() {
        return StateStoreTestBuilder.from(PartitionsBuilderSplitsFirst.leavesWithSplits(DEFAULT_SCHEMA,
                List.of("A", "B"), List.of("aaa"))
                .parentJoining("parent", "A", "B"));
    }

    public static PartitionsBuilder createPartitionsBuilder() {
        return new PartitionsBuilder(DEFAULT_SCHEMA);
    }

    public static TableProperties createTablePropertiesWithSplitThreshold(long splitThreshold) {
        return createTablePropertiesWithSplitThreshold(DEFAULT_SCHEMA, splitThreshold);
    }

    public static TableProperties createTablePropertiesWithSplitThreshold(Schema schema, long splitThreshold) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, splitThreshold);
        return tableProperties;
    }

    public static TableProperties createTableProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        return createTestTableProperties(instanceProperties, DEFAULT_SCHEMA);
    }
}
