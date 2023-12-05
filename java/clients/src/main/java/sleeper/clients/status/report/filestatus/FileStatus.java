/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.core.statestore.FileInfo;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.function.Function;

/**
 * A data structure to hold information about the status of files within Sleeper
 * i.e. details on the file  partitions there are leaf and non leaf how many files need to be gc etc
 */
public class FileStatus {
    private long totalRecords;
    private long totalRecordsInLeafPartitions;

    private boolean moreThanMax;
    private long leafPartitionCount;
    private long nonLeafPartitionCount;
    private long activeFilesCount;
    private long activeFilesInLeafPartitions;
    private long activeFilesInNonLeafPartitions;

    private PartitionStats leafPartitionStats;
    private PartitionStats nonLeafPartitionStats;

    private List<FileInfo> activeFiles;
    private List<String> gcFiles;

    public long getLeafPartitionCount() {
        return leafPartitionCount;
    }

    public void setLeafPartitionCount(long leafPartitionCount) {
        this.leafPartitionCount = leafPartitionCount;
    }

    public long getNonLeafPartitionCount() {
        return nonLeafPartitionCount;
    }

    public void setNonLeafPartitionCount(long nonLeafPartitionCount) {
        this.nonLeafPartitionCount = nonLeafPartitionCount;
    }

    public long getActiveFilesCount() {
        return activeFilesCount;
    }

    public void setActiveFilesCount(long activeFilesCount) {
        this.activeFilesCount = activeFilesCount;
    }

    public long getActiveFilesInLeafPartitions() {
        return activeFilesInLeafPartitions;
    }

    public void setActiveFilesInLeafPartitions(long activeFilesInLeafPartitions) {
        this.activeFilesInLeafPartitions = activeFilesInLeafPartitions;
    }

    public long getActiveFilesInNonLeafPartitions() {
        return activeFilesInNonLeafPartitions;
    }

    public void setActiveFilesInNonLeafPartitions(long activeFilesInNonLeafPartitions) {
        this.activeFilesInNonLeafPartitions = activeFilesInNonLeafPartitions;
    }

    public boolean isMoreThanMax() {
        return moreThanMax;
    }

    public void setMoreThanMax(boolean moreThanMax) {
        this.moreThanMax = moreThanMax;
    }

    public PartitionStats getLeafPartitionStats() {
        return leafPartitionStats;
    }

    public void setLeafPartitionStats(PartitionStats leafPartitionStats) {
        this.leafPartitionStats = leafPartitionStats;
    }

    public PartitionStats getNonLeafPartitionStats() {
        return nonLeafPartitionStats;
    }

    public void setNonLeafPartitionStats(PartitionStats nonLeafPartitionStats) {
        this.nonLeafPartitionStats = nonLeafPartitionStats;
    }

    public List<FileInfo> getActiveFiles() {
        return activeFiles;
    }

    public void setActiveFiles(List<FileInfo> activeFiles) {
        this.activeFiles = activeFiles;
    }

    public List<String> getGcFiles() {
        return gcFiles;
    }

    public void setGcFiles(List<String> gcFiles) {
        this.gcFiles = gcFiles;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public long getTotalRecordsInLeafPartitions() {
        return totalRecordsInLeafPartitions;
    }

    public void setTotalRecordsInLeafPartitions(long totalRecordsInLeafPartitions) {
        this.totalRecordsInLeafPartitions = totalRecordsInLeafPartitions;
    }

    public String verboseReportString(Function<PrintStream, FileStatusReporter> getReporter) throws UnsupportedEncodingException {
        return FileStatusReporter.asString(getReporter, this, true);
    }

    public static class PartitionStats {
        private final Integer minSize;
        private final Integer maxMax;
        private final Double averageSize;
        private final Integer total;

        public PartitionStats(Integer minSize, Integer maxMax, Double averageSize, Integer total) {
            this.minSize = minSize;
            this.maxMax = maxMax;
            this.averageSize = averageSize;
            this.total = total;
        }

        public Integer getMinSize() {
            return minSize;
        }

        public Integer getMaxMax() {
            return maxMax;
        }

        public Double getAverageSize() {
            return averageSize;
        }

        public Integer getTotal() {
            return total;
        }
    }
}
