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
package sleeper.clients.status.report.filestatus;

import sleeper.core.statestore.FileReference;

import java.util.Collection;
import java.util.Set;

/**
 * A data structure to hold information about the status of files within Sleeper
 * i.e. details on the file partitions there are (leaf and non leaf), how many files have no references etc
 */
public class TableFilesStatus {

    private final boolean moreThanMax;
    private final long leafPartitionCount;
    private final long nonLeafPartitionCount;

    private final TableFilesStatistics statistics;

    private final Collection<FileReference> fileReferences;
    private final Set<String> filesWithNoReferences;

    private TableFilesStatus(Builder builder) {
        moreThanMax = builder.moreThanMax;
        leafPartitionCount = builder.leafPartitionCount;
        nonLeafPartitionCount = builder.nonLeafPartitionCount;
        statistics = builder.statistics;
        fileReferences = builder.fileReferences;
        filesWithNoReferences = builder.filesWithNoReferences;
    }

    public static Builder builder() {
        return new Builder();
    }

    public long getLeafPartitionCount() {
        return leafPartitionCount;
    }

    public long getNonLeafPartitionCount() {
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

    public FileReferenceStats getLeafPartitionFileReferenceStats() {
        return statistics.getLeafPartitionFileReferenceStats();
    }

    public FileReferenceStats getNonLeafPartitionFileReferenceStats() {
        return statistics.getNonLeafPartitionFileReferenceStats();
    }

    public Collection<FileReference> getFileReferences() {
        return fileReferences;
    }

    public Set<String> getFilesWithNoReferences() {
        return filesWithNoReferences;
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
        private boolean moreThanMax;
        private long leafPartitionCount;
        private long nonLeafPartitionCount;
        private TableFilesStatistics statistics;
        private Collection<FileReference> fileReferences;
        private Set<String> filesWithNoReferences;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder moreThanMax(boolean moreThanMax) {
            this.moreThanMax = moreThanMax;
            return this;
        }

        public Builder leafPartitionCount(long leafPartitionCount) {
            this.leafPartitionCount = leafPartitionCount;
            return this;
        }

        public Builder nonLeafPartitionCount(long nonLeafPartitionCount) {
            this.nonLeafPartitionCount = nonLeafPartitionCount;
            return this;
        }

        public Builder statistics(TableFilesStatistics statistics) {
            this.statistics = statistics;
            return this;
        }

        public Builder fileReferences(Collection<FileReference> fileReferences) {
            this.fileReferences = fileReferences;
            return this;
        }

        public Builder filesWithNoReferences(Set<String> filesWithNoReferences) {
            this.filesWithNoReferences = filesWithNoReferences;
            return this;
        }

        public TableFilesStatus build() {
            return new TableFilesStatus(this);
        }
    }
}
