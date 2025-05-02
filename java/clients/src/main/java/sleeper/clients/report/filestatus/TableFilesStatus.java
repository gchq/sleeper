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
 * A data structure to hold information about the status of files within Sleeper
 * i.e. details on the file partitions there are (leaf and non leaf), how many files have no references etc
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

    public long getTotalRecords() {
        return statistics.getTotalRecords();
    }

    public long getTotalRecordsInLeafPartitions() {
        return statistics.getTotalRecordsInLeafPartitions();
    }

    public long getTotalRecordsApprox() {
        return statistics.getTotalRecordsApprox();
    }

    public long getTotalRecordsInLeafPartitionsApprox() {
        return statistics.getTotalRecordsInLeafPartitionsApprox();
    }

    public long getTotalRecordsInNonLeafPartitions() {
        return statistics.getTotalRecordsInNonLeafPartitions();
    }

    public long getTotalRecordsInNonLeafPartitionsApprox() {
        return statistics.getTotalRecordsInNonLeafPartitionsApprox();
    }

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

        public Builder leafPartitionCount(int leafPartitionCount) {
            this.leafPartitionCount = leafPartitionCount;
            return this;
        }

        public Builder nonLeafPartitionCount(int nonLeafPartitionCount) {
            this.nonLeafPartitionCount = nonLeafPartitionCount;
            return this;
        }

        public Builder statistics(TableFilesStatistics statistics) {
            this.statistics = statistics;
            return this;
        }

        public Builder files(AllReferencesToAllFiles files) {
            this.files = files;
            return this;
        }

        public TableFilesStatus build() {
            return new TableFilesStatus(this);
        }
    }
}
