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
package sleeper.clients.report.filestatus;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;

import java.util.Collection;

/**
 * A report on the status of files within a Sleeper table. This includes statistics on the number of partitions, how
 * many files have no references, etc. This is presented to the user by {@link FileStatusReporter} implementations.
 */
public class TableFilesStatus {

    private final boolean moreThanMax;
    private final int leafPartitionCount;
    private final int nonLeafPartitionCount;

    private final TableFilesStatistics statistics;

    private final AllReferencesToAllFiles files;

    private TableFilesStatus(Builder builder) {
        leafPartitionCount = builder.leafPartitionCount;
        nonLeafPartitionCount = builder.nonLeafPartitionCount;
        statistics = builder.statistics;
        files = builder.files;
        moreThanMax = files.isMoreThanMax();
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getLeafPartitionCount() {
        return leafPartitionCount;
    }

    public int getNonLeafPartitionCount() {
        return nonLeafPartitionCount;
    }

    public int getFileCount() {
        return statistics.getFileCount();
    }

    public long getFileReferenceCount() {
        return statistics.getFileReferenceCount();
    }

    public long getReferencesInLeafPartitions() {
        return statistics.getReferencesInLeafPartitions();
    }

    public long getReferencesInNonLeafPartitions() {
        return statistics.getReferencesInNonLeafPartitions();
    }

    public boolean isMoreThanMax() {
        return moreThanMax;
    }

    public FileReferencesStats getLeafPartitionFileReferenceStats() {
        return statistics.getLeafPartitionFileReferenceStats();
    }

    public FileReferencesStats getNonLeafPartitionFileReferenceStats() {
        return statistics.getNonLeafPartitionFileReferenceStats();
    }

    public Collection<AllReferencesToAFile> getFilesWithReferences() {
        return files.getFilesWithReferences();
    }

    public Collection<AllReferencesToAFile> getFilesWithNoReferences() {
        return files.getFilesWithNoReferences();
    }

    public long getTotalRows() {
        return statistics.getTotalRows();
    }

    public long getTotalRowsInLeafPartitions() {
        return statistics.getTotalRowsInLeafPartitions();
    }

    public long getTotalRowsApprox() {
        return statistics.getTotalRowsApprox();
    }

    public long getTotalRowsInLeafPartitionsApprox() {
        return statistics.getTotalRowsInLeafPartitionsApprox();
    }

    public long getTotalRowsInNonLeafPartitions() {
        return statistics.getTotalRowsInNonLeafPartitions();
    }

    public long getTotalRowsInNonLeafPartitionsApprox() {
        return statistics.getTotalRowsInNonLeafPartitionsApprox();
    }

    /**
     * A builder for the status of files in a Sleeper table.
     */
    public static final class Builder {
        private int leafPartitionCount;
        private int nonLeafPartitionCount;
        private TableFilesStatistics statistics;
        private AllReferencesToAllFiles files;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        /**
         * Sets the count of leaf partitions in the Sleeper table. A partition is a leaf partition if it has no child
         * partitions, and so the space of row keys in the Sleeper table has not been split up beyond this level.
         *
         * @param  leafPartitionCount the number of leaf partitions in the table
         * @return                    this builder
         */
        public Builder leafPartitionCount(int leafPartitionCount) {
            this.leafPartitionCount = leafPartitionCount;
            return this;
        }

        /**
         * Sets the count of non-leaf partitions in the Sleeper table. A partition is a non-leaf partition if the region
         * of row keys that it covers has been split up into smaller partitions, i.e. its child partitions.
         *
         * @param  nonLeafPartitionCount the number of leaf partitions in the table
         * @return                       this builder
         */
        public Builder nonLeafPartitionCount(int nonLeafPartitionCount) {
            this.nonLeafPartitionCount = nonLeafPartitionCount;
            return this;
        }

        /**
         * Sets the statistics on files in the Sleeper table.
         *
         * @param  statistics the statistics
         * @return            this builder
         */
        public Builder statistics(TableFilesStatistics statistics) {
            this.statistics = statistics;
            return this;
        }

        /**
         * Sets the files in the Sleeper table.
         *
         * @param  files the files
         * @return       this builder
         */
        public Builder files(AllReferencesToAllFiles files) {
            this.files = files;
            return this;
        }

        public TableFilesStatus build() {
            return new TableFilesStatus(this);
        }
    }
}
