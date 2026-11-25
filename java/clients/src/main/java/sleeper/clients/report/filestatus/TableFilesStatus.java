/*
 * Copyright 2022-2026 Crown Copyright
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

import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * A report on the status of files within a Sleeper table. This includes statistics on the number of partitions, how
 * many files have no references, etc. This is presented to the user by {@link FileStatusReporter} implementations.
 */
public class TableFilesStatus {

    private final Instant timeOfReport;
    private final boolean moreThanMax;
    private final int leafPartitionCount;
    private final int nonLeafPartitionCount;

    private final TableFilesStatistics statistics;

    private final AllReferencesToAllFiles files;
    private final List<TableFilesPartitionCompactions> compactions;

    private TableFilesStatus(Builder builder) {
        timeOfReport = builder.timeOfReport;
        leafPartitionCount = builder.leafPartitionCount;
        nonLeafPartitionCount = builder.nonLeafPartitionCount;
        statistics = builder.statistics;
        files = builder.files;
        compactions = builder.compactions;
        moreThanMax = builder.moreThanMax;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Instant getTimeOfReport() {
        return timeOfReport;
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

    public int getFilesWithReferencesCount() {
        return statistics.getFilesWithReferencesCount();
    }

    public int getFilesWithNoReferencesCount() {
        return statistics.getFilesWithNoReferencesCount();
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

    /**
     * Retrieves files with references. Only present if the report includes file references.
     *
     * @return the files with references
     */
    public Collection<AllReferencesToAFile> getFilesWithReferences() {
        return files.getFilesWithReferences();
    }

    /**
     * Retrieves files with no references. Only present if the report includes file references.
     *
     * @return the files with no references
     */
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
     * Retrieves compaction jobs assigned to files. Only present if the report includes compaction jobs.
     *
     * @return the compactions
     */
    public List<TableFilesPartitionCompactions> getCompactions() {
        return compactions;
    }

    public CompactionJobsSummary getCompactionsAssignedSummary() {
        return statistics.getCompactionsAssignedSummary();
    }

    public CompactionJobsSummary getCompactionsAssignedMoreThan12HoursAgoSummary() {
        return statistics.getCompactionsAssignedMoreThan12HoursAgoSummary();
    }

    public Instant getOldestCompactionAssignmentTime() {
        return statistics.getOldestCompactionAssignmentTime();
    }

    /**
     * A builder for the status of files in a Sleeper table.
     */
    public static final class Builder {
        private Instant timeOfReport;
        private int leafPartitionCount;
        private int nonLeafPartitionCount;
        private TableFilesStatistics statistics;
        private boolean moreThanMax;
        private AllReferencesToAllFiles files;
        private List<TableFilesPartitionCompactions> compactions;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        /**
         * Sets the time this report was generated.
         *
         * @param  timeOfReport the time of the report
         * @return              this builder
         */
        public Builder timeOfReport(Instant timeOfReport) {
            this.timeOfReport = timeOfReport;
            return this;
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
         * Sets whether there were more files with no references than the maximum to retrieve.
         *
         * @param  moreThanMax true if the count of files with no references was limited
         * @return             this builder
         */
        public Builder moreThanMax(boolean moreThanMax) {
            this.moreThanMax = moreThanMax;
            return this;
        }

        /**
         * Sets the files in the Sleeper table. Should only be set if the report includes file references.
         *
         * @param  files the files
         * @return       this builder
         */
        public Builder files(AllReferencesToAllFiles files) {
            this.files = files;
            return this;
        }

        /**
         * Sets the compaction jobs currently assigned to files. Should only be set if the report includes compaction
         * jobs.
         *
         * @param  compactions the compactions
         * @return             this builder
         */
        public Builder compactions(List<TableFilesPartitionCompactions> compactions) {
            this.compactions = compactions;
            return this;
        }

        public TableFilesStatus build() {
            return new TableFilesStatus(this);
        }
    }
}
