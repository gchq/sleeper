/*
 * Copyright 2022 Crown Copyright
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

package sleeper.status.report.partitions;

import sleeper.ToStringPrintStream;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.inmemory.StateStoreTestBuilder;

import java.util.Arrays;
import java.util.Collections;

import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class PartitionStatusReportTestHelper {
    private static final Long TEST_THRESHOLD = 10L;
    private static final Schema DEFAULT_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .build();

    private PartitionStatusReportTestHelper() {
    }

    public static StateStoreTestBuilder createRootPartitionWithNoChildren() {
        return StateStoreTestBuilder.from(createPartitionsBuilder().singlePartition("root"))
                .singleFileInEachLeafPartitionWithRecords(5);
    }

    public static StateStoreTestBuilder createRootPartitionWithTwoChildrenBelowSplitThreshold() {
        return createRootPartitionWithTwoChildren()
                .singleFileInEachLeafPartitionWithRecords(5);
    }

    public static StateStoreTestBuilder createRootPartitionWithTwoChildrenAboveSplitThreshold() {
        return createRootPartitionWithTwoChildren()
                .singleFileInEachLeafPartitionWithRecords(100);
    }

    public static StateStoreTestBuilder createRootPartitionWithTwoChildren() {
        return StateStoreTestBuilder.from(createPartitionsBuilder()
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList("aaa"))
                .parentJoining("parent", "A", "B"));
    }

    public static StateStoreTestBuilder createRootPartitionWithTwoChildrenSplitOnByteArray() {
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .build();
        return StateStoreTestBuilder.from(new PartitionsBuilder(schema)
                        .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(new byte[42]))
                        .parentJoining("parent", "A", "B"))
                .singleFileInEachLeafPartitionWithRecords(5);
    }

    public static StateStoreTestBuilder createRootPartitionWithTwoChildrenSplitOnLongString() {
        return StateStoreTestBuilder.from(createPartitionsBuilder()
                        .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(
                                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"))
                        .parentJoining("parent", "A", "B"))
                .singleFileInEachLeafPartitionWithRecords(5);
    }

    public static PartitionsBuilder createPartitionsBuilder() {
        return new PartitionsBuilder(DEFAULT_SCHEMA);
    }

    public static String getStandardReport(StateStore stateStore) throws StateStoreException {
        ToStringPrintStream output = new ToStringPrintStream();
        PartitionsStatusReporter reporter = new PartitionsStatusReporter(output.getPrintStream());
        reporter.report(PartitionsStatus.from(createTableProperties(), stateStore));
        return output.toString();
    }

    public static TableProperties createTableProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "test-table");
        setTestSplitThreshold(tableProperties);
        return tableProperties;
    }

    public static void setTestSplitThreshold(TableProperties tableProperties) {
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, TEST_THRESHOLD);
    }
}
