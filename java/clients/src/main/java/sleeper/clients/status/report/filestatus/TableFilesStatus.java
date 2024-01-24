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
    private final long totalRecords;
    private final long totalRecordsApprox;
    private final long totalRecordsInLeafPartitions;
    private final long totalRecordsInLeafPartitionsApprox;

    private final boolean moreThanMax;
    private final long leafPartitionCount;
    private final long nonLeafPartitionCount;
    private final long activeFilesCount;

    private final FileReferenceStats leafFileReferenceStats;
    private final FileReferenceStats nonLeafFileReferenceStats;

    private final Collection<FileReference> fileReferences;
    private final Set<String> filesWithNoReferences;

    private TableFilesStatus(Builder builder) {
        this.totalRecords = builder.totalRecords;
        this.totalRecordsApprox = builder.totalRecordsApprox;
        this.totalRecordsInLeafPartitions = builder.totalRecordsInLeafPartitions;
        this.totalRecordsInLeafPartitionsApprox = builder.totalRecordsInLeafPartitionsApprox;
        this.moreThanMax = builder.moreThanMax;
        this.leafPartitionCount = builder.leafPartitionCount;
        this.nonLeafPartitionCount = builder.nonLeafPartitionCount;
        this.activeFilesCount = builder.activeFilesCount;
        this.leafFileReferenceStats = builder.leafFileReferenceStats;
        this.nonLeafFileReferenceStats = builder.nonLeafFileReferenceStats;
        this.fileReferences = builder.fileReferences;
        this.filesWithNoReferences = builder.filesWithNoReferences;
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

    public long getActiveFilesCount() {
        return activeFilesCount;
    }

    public long getReferencesInLeafPartitions() {
        return leafFileReferenceStats.getTotalReferences();
    }

    public long getReferencesInNonLeafPartitions() {
        return nonLeafFileReferenceStats.getTotalReferences();
    }

    public boolean isMoreThanMax() {
        return moreThanMax;
    }

    public FileReferenceStats getLeafPartitionFileReferenceStats() {
        return leafFileReferenceStats;
    }

    public FileReferenceStats getNonLeafPartitionFileReferenceStats() {
        return nonLeafFileReferenceStats;
    }

    public Collection<FileReference> getFileReferences() {
        return fileReferences;
    }

    public Set<String> getFilesWithNoReferences() {
        return filesWithNoReferences;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public long getTotalRecordsInLeafPartitions() {
        return totalRecordsInLeafPartitions;
    }

    public long getTotalRecordsApprox() {
        return totalRecordsApprox;
    }

    public long getTotalRecordsInLeafPartitionsApprox() {
        return totalRecordsInLeafPartitionsApprox;
    }

    public static final class Builder {
        private long totalRecords;
        private long totalRecordsApprox;
        private long totalRecordsInLeafPartitions;
        private long totalRecordsInLeafPartitionsApprox;
        private boolean moreThanMax;
        private long leafPartitionCount;
        private long nonLeafPartitionCount;
        private long activeFilesCount;
        private FileReferenceStats leafFileReferenceStats;
        private FileReferenceStats nonLeafFileReferenceStats;
        private Collection<FileReference> fileReferences;
        private Set<String> filesWithNoReferences;

        private Builder() {
        }

        public Builder totalRecords(long totalRecords) {
            this.totalRecords = totalRecords;
            return this;
        }

        public Builder totalRecordsApprox(long totalRecordsApprox) {
            this.totalRecordsApprox = totalRecordsApprox;
            return this;
        }

        public Builder totalRecordsInLeafPartitions(long totalRecordsInLeafPartitions) {
            this.totalRecordsInLeafPartitions = totalRecordsInLeafPartitions;
            return this;
        }

        public Builder totalRecordsInLeafPartitionsApprox(long totalRecordsInLeafPartitionsApprox) {
            this.totalRecordsInLeafPartitionsApprox = totalRecordsInLeafPartitionsApprox;
            return this;
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

        public Builder activeFilesCount(long activeFilesCount) {
            this.activeFilesCount = activeFilesCount;
            return this;
        }

        public Builder leafPartitionStats(FileReferenceStats leafFileReferenceStats) {
            this.leafFileReferenceStats = leafFileReferenceStats;
            return this;
        }

        public Builder nonLeafPartitionStats(FileReferenceStats nonLeafFileReferenceStats) {
            this.nonLeafFileReferenceStats = nonLeafFileReferenceStats;
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
