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
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class PartitionStatusReportTestHelper {
    private static final Long TEST_THRESHOLD = 10L;

    private PartitionStatusReportTestHelper() {
    }

    public static List<Partition> createRootPartitionWithTwoChildren() {
        PartitionFactory partitionFactory = createPartitionFactory();
        Partition a = partitionFactory.partition("A", "", "aaa");
        Partition b = partitionFactory.partition("B", "aaa", null);
        Partition parent = partitionFactory.parentJoining("parent", a, b);
        return Arrays.asList(parent, a, b);
    }

    public static List<Partition> createRootPartitionWithNoChildren() {
        PartitionFactory partitionFactory = createPartitionFactory();
        Partition root = partitionFactory.partition("root", "", null);
        return Collections.singletonList(root);
    }

    public static PartitionFactory createPartitionFactory() {
        Field key = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(key).build();
        return new PartitionFactory(schema);
    }

    public static Map<String, Long> setNumberOfRecordsForPartitionsNonSplitting(List<Partition> partitions) {
        return setNumberOfRecordsForPartitions(partitions, 10L);
    }

    public static Map<String, Long> setNumberOfRecordsForPartitionsSplitting(List<Partition> partitions) {
        return setNumberOfRecordsForPartitions(partitions, 10000L);
    }

    public static Map<String, Long> setNumberOfRecordsForPartitions(List<Partition> partitions, Long records) {
        Map<String, Long> recordsToPartition = new HashMap<>();
        partitions.stream()
                .filter(Partition::isLeafPartition)
                .forEach(partition -> recordsToPartition.put(partition.getId(), records));
        return recordsToPartition;
    }

    public static String getStandardReport(PartitionsQuery queryType, List<Partition> partitionList,
                                           Map<String, Long> recordsPerPartitions, int splittingPartitionCount) {
        ToStringPrintStream output = new ToStringPrintStream();
        StandardPartitionsStatusReporter reporter = new StandardPartitionsStatusReporter(output.getPrintStream());
        reporter.report(queryType, partitionList, recordsPerPartitions, splittingPartitionCount);
        return output.toString();
    }

    public static List<FileInfo> createFileInfosNonSplitting(List<Partition> partitions) {
        return createFileInfos(partitions, 5L);
    }

    public static List<FileInfo> createFileInfosSplitting(List<Partition> partitions) {
        return createFileInfos(partitions, 100L);
    }

    public static List<FileInfo> createFileInfos(List<Partition> partitions, Long records) {
        List<FileInfo> fileInfos = new ArrayList<>();
        partitions.forEach(partition -> {
            if (partition.isLeafPartition()) {
                fileInfos.add(FileInfo.builder()
                        .numberOfRecords(records)
                        .filename("test" + partition.getId() + ".parquet")
                        .partitionId(partition.getId())
                        .jobId("test-job")
                        .build());
            }
        });
        return fileInfos;
    }

    public static TableProperties createTableProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "test-table");
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, TEST_THRESHOLD);
        return tableProperties;
    }
}
