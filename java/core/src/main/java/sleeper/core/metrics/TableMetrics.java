/*
 * Copyright 2022-2025 Crown Copyright
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

/**
 * Metrics on the state store for a Sleeper table.
 */
public class TableMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMetrics.class);

    private final String instanceId;
    private final String tableName;
    private final int fileCount;
    private final long rowCount;
    private final int partitionCount;
    private final int leafPartitionCount;
    private final double averageFileReferencesPerPartition;

    private TableMetrics(Builder builder) {
        instanceId = builder.instanceId;
        tableName = builder.tableName;
        fileCount = builder.fileCount;
        rowCount = builder.rowCount;
        partitionCount = builder.partitionCount;
        leafPartitionCount = builder.leafPartitionCount;
        averageFileReferencesPerPartition = builder.averageFileReferencesPerPartition;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates metrics for a Sleeper table. Queries the state store and computes statistics.
     *
     * @param  instanceId the Sleeper instance ID
     * @param  table      the Sleeper table status
     * @param  stateStore the Sleeper table's state store
     * @return            the metrics
     */
    public static TableMetrics from(String instanceId, TableStatus table, StateStore stateStore) {

        LOGGER.info("Querying state store for table {} for files", table);
        AllReferencesToAllFiles files = stateStore.getAllFilesWithMaxUnreferenced(0);
        Collection<AllReferencesToAFile> referencedFiles = files.getFilesWithReferences();
        List<FileReference> fileReferences = files.streamFileReferences().toList();
        LOGGER.info("Found {} files for table {}", referencedFiles.size(), table);
        LOGGER.info("Found {} file references for table {}", fileReferences.size(), table);
        long rowCount = fileReferences.stream().mapToLong(FileReference::getNumberOfRows).sum();
        LOGGER.info("Total number of rows in table {} is {}", table, rowCount);

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
                .rowCount(rowCount)
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

    public long getRowCount() {
        return rowCount;
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
        return fileCount == that.fileCount && rowCount == that.rowCount && partitionCount == that.partitionCount && leafPartitionCount == that.leafPartitionCount
                && Double.compare(averageFileReferencesPerPartition, that.averageFileReferencesPerPartition) == 0 && Objects.equals(instanceId, that.instanceId)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, tableName, fileCount, rowCount, partitionCount, leafPartitionCount, averageFileReferencesPerPartition);
    }

    @Override
    public String toString() {
        return "TableMetrics{" +
                "instanceId='" + instanceId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", fileCount=" + fileCount +
                ", rowCount=" + rowCount +
                ", partitionCount=" + partitionCount +
                ", leafPartitionCount=" + leafPartitionCount +
                ", averageFileReferencesPerPartition=" + averageFileReferencesPerPartition +
                '}';
    }

    /**
     * A builder for table metrics.
     */
    public static final class Builder {
        private String instanceId;
        private String tableName;
        private int fileCount;
        private long rowCount;
        private int partitionCount;
        private int leafPartitionCount;
        private double averageFileReferencesPerPartition;

        private Builder() {
        }

        /**
         * Sets the Sleeper instance ID.
         *
         * @param  instanceId the instance ID
         * @return            this builder
         */
        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        /**
         * Sets the Sleeper table name.
         *
         * @param  tableName the table name
         * @return           this builder
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Sets the number of files in the Sleeper table. This does not include files with no references, that are
         * waiting for garbage collection.
         *
         * @param  fileCount the number of files in the table
         * @return           this builder
         */
        public Builder fileCount(int fileCount) {
            this.fileCount = fileCount;
            return this;
        }

        /**
         * Sets the number of rows in the Sleeper table. This may include estimated counts.
         *
         * @param  rowCount the number of rows in the table
         * @return          this builder
         */
        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        /**
         * Sets the number of partitions in the Sleeper table. This includes all leaf partitions, and all joins in the
         * partition tree, up to the root node.
         *
         * @param  partitionCount the number of partitions in the table
         * @return                this builder
         */
        public Builder partitionCount(int partitionCount) {
            this.partitionCount = partitionCount;
            return this;
        }

        /**
         * Sets the number of leaf partitions in the Sleeper table. This only includes partitions that have not been
         * split. Note that not all files may be on leaf partitions.
         *
         * @param  leafPartitionCount the number of leaf partitions in the table
         * @return                    this builder
         */
        public Builder leafPartitionCount(int leafPartitionCount) {
            this.leafPartitionCount = leafPartitionCount;
            return this;
        }

        /**
         * Sets the average number of file references per partition. This excludes partitions with no references, but
         * may include partitions higher in the partition tree, potentially with very few references as files are
         * compacted down the tree.
         *
         * @param  averageFileReferencesPerPartition the average number of file references per partition
         * @return                                   this builder
         */
        public Builder averageFileReferencesPerPartition(double averageFileReferencesPerPartition) {
            this.averageFileReferencesPerPartition = averageFileReferencesPerPartition;
            return this;
        }

        public TableMetrics build() {
            return new TableMetrics(this);
        }
    }
}
