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

package sleeper.core.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableStatus;

import java.util.Collection;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetrics.class);

    private final String instanceId;
    private final String tableName;
    private final int fileCount;
    private final long recordCount;
    private final int partitionCount;
    private final int leafPartitionCount;
    private final double averageFileReferencesPerPartition;

    private TableMetrics(Builder builder) {
        instanceId = builder.instanceId;
        tableName = builder.tableName;
        fileCount = builder.fileCount;
        recordCount = builder.recordCount;
        partitionCount = builder.partitionCount;
        leafPartitionCount = builder.leafPartitionCount;
        averageFileReferencesPerPartition = builder.averageFileReferencesPerPartition;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TableMetrics from(String instanceId, TableStatus table, StateStore stateStore) {

        LOGGER.info("Querying state store for table {} for files", table);
        AllReferencesToAllFiles files = stateStore.getAllFilesWithMaxUnreferenced(0);
        Collection<AllReferencesToAFile> referencedFiles = files.getFilesWithReferences();
        List<FileReference> fileReferences = files.listFileReferences();
        LOGGER.info("Found {} files for table {}", referencedFiles.size(), table);
        LOGGER.info("Found {} file references for table {}", fileReferences.size(), table);
        long recordCount = fileReferences.stream().mapToLong(FileReference::getNumberOfRecords).sum();
        LOGGER.info("Total number of records in table {} is {}", table, recordCount);

        Map<String, Long> fileCountByPartitionId = fileReferences.stream()
                .collect(Collectors.groupingBy(FileReference::getPartitionId, Collectors.counting()));
        LongSummaryStatistics filesPerPartitionStats = fileCountByPartitionId.values().stream()
                .mapToLong(value -> value).summaryStatistics();
        LOGGER.info("Files per partition for table {}: {}", table, filesPerPartitionStats);

        LOGGER.info("Querying state store for table {} for partitions", table);
        List<Partition> partitions = stateStore.getAllPartitions();
        int partitionCount = partitions.size();
        int leafPartitionCount = (int) partitions.stream().filter(Partition::isLeafPartition).count();
        LOGGER.info("Found {} partitions and {} leaf partitions for table {}", partitionCount, leafPartitionCount, table);

        return TableMetrics.builder()
                .instanceId(instanceId)
                .tableName(table.getTableName())
                .partitionCount(partitionCount)
                .leafPartitionCount(leafPartitionCount)
                .fileCount(referencedFiles.size())
                .recordCount(recordCount)
                .averageFileReferencesPerPartition(filesPerPartitionStats.getAverage())
                .build();
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getTableName() {
        return tableName;
    }

    public int getFileCount() {
        return fileCount;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getLeafPartitionCount() {
        return leafPartitionCount;
    }

    public double getAverageFileReferencesPerPartition() {
        return averageFileReferencesPerPartition;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TableMetrics that = (TableMetrics) object;
        return fileCount == that.fileCount && recordCount == that.recordCount && partitionCount == that.partitionCount && leafPartitionCount == that.leafPartitionCount
                && Double.compare(averageFileReferencesPerPartition, that.averageFileReferencesPerPartition) == 0 && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, tableName, fileCount, recordCount, partitionCount, leafPartitionCount, averageFileReferencesPerPartition);
    }

    @Override
    public String toString() {
        return "TableMetrics{" +
                "instanceId='" + instanceId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", fileCount=" + fileCount +
                ", recordCount=" + recordCount +
                ", partitionCount=" + partitionCount +
                ", leafPartitionCount=" + leafPartitionCount +
                ", averageFileReferencesPerPartition=" + averageFileReferencesPerPartition +
                '}';
    }

    public static final class Builder {
        private String instanceId;
        private String tableName;
        private int fileCount;
        private long recordCount;
        private int partitionCount;
        private int leafPartitionCount;
        private double averageFileReferencesPerPartition;

        private Builder() {
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder fileCount(int fileCount) {
            this.fileCount = fileCount;
            return this;
        }

        public Builder recordCount(long recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public Builder partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        public Builder leafPartitionCount(int leafPartitionCount) {
            this.leafPartitionCount = leafPartitionCount;
            return this;
        }

        public Builder averageFileReferencesPerPartition(double averageFileReferencesPerPartition) {
            this.averageFileReferencesPerPartition = averageFileReferencesPerPartition;
            return this;
        }

        public TableMetrics build() {
            return new TableMetrics(this);
        }
    }
}
