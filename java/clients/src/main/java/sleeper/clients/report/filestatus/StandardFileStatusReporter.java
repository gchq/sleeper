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
import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static sleeper.clients.util.ClientUtils.abbreviatedRowCount;

/**
 * Returns file status information to the user on the console.
 */
public class StandardFileStatusReporter implements FileStatusReporter {

    private final PrintStream out;

    public StandardFileStatusReporter() {
        this(System.out);
    }

    public StandardFileStatusReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(TableFilesStatus status, FileStatusDetailLevel detailLevel) {
        out.println("\nFiles Status Report:\n--------------------------");
        out.println("Time of report: " + status.getTimeOfReport());
        out.println("There are " + status.getLeafPartitionCount() + " leaf partitions and " + status.getNonLeafPartitionCount() + " non-leaf partitions");
        out.println("Number of files: " + (status.isMoreThanMax() ? ">" : "") + status.getFileCount());
        out.println("Number of files with references: " + status.getFilesWithReferences().size());
        out.println("Number of files with no references, which will be garbage collected: " + (status.isMoreThanMax() ? ">" : "") + status.getFilesWithNoReferences().size());
        out.println("Number of references to files: " + status.getFileReferenceCount());

        printPartitionStats(status.getLeafPartitionFileReferenceStats(), "leaf");
        printPartitionStats(status.getNonLeafPartitionFileReferenceStats(), "non-leaf");
        printRowStats(status);
        printCompactionStats(status);

        if (detailLevel.isShowFileReferences()) {
            out.println();
            out.println("Files with no references"
                    + (status.isMoreThanMax() ? " (more are present, maximum count applied)" : "") + ":"
                    + (status.getFilesWithNoReferences().isEmpty() ? " none" : ""));
            status.getFilesWithNoReferences().forEach(this::printFile);

            out.println();
            out.println("Files with references:"
                    + (status.getFilesWithReferences().isEmpty() ? " none" : ""));
            status.getFilesWithReferences().forEach(this::printFile);
        }
        if (detailLevel.isShowCompactionJobs()) {
            out.println();
            out.println("Compactions assigned to files:" + (status.getCompactions().isEmpty() ? " none" : ""));
            status.getCompactions().forEach(this::printPartitionCompactions);
        }
        if (detailLevel == FileStatusDetailLevel.STATS_ONLY) {
            out.println();
            out.println("To see the files or compactions currently in the state store, please set the detail level of the report.");
        }
    }

    private void printPartitionStats(FileReferencesStats partitions, String type) {
        if (partitions.getTotalReferences() > 0) {
            out.format("Number of file references in %s partitions: min = %d, max = %d, mean = %.3f, median = %.3f, 90th percentile = %.3f, 99th percentile = %.3f, total = %s%n",
                    type,
                    partitions.getMinReferences(),
                    partitions.getMaxReferences(),
                    partitions.getMeanReferences(),
                    partitions.getMedianReferences(),
                    partitions.get90thPercentile(),
                    partitions.get99thPercentile(),
                    partitions.getTotalReferences());
        } else {
            out.println("Number of file references in " + type + " partitions: 0");
        }
    }

    private void printRowStats(TableFilesStatus status) {
        String percentageSuffix = ": ";
        String allReferencedFilesSuffix = ": ";
        if (status.getTotalRowsApprox() > 0L) {
            allReferencedFilesSuffix = " (approx): ";
            percentageSuffix = " (approx): ";
        }
        String leafFilesSuffix = ": ";
        if (status.getTotalRowsInLeafPartitionsApprox() > 0L) {
            leafFilesSuffix = " (approx): ";
            percentageSuffix = " (approx): ";
        }
        String nonLeafFilesSuffix = ": ";
        if (status.getTotalRowsInNonLeafPartitionsApprox() > 0L) {
            nonLeafFilesSuffix = " (approx): ";
        }
        out.println("Number of rows referenced in partitions" + allReferencedFilesSuffix +
                abbreviatedRowCount(status.getTotalRows()));
        out.println("Number of rows in non-leaf partitions" + nonLeafFilesSuffix +
                abbreviatedRowCount(status.getTotalRowsInNonLeafPartitions()));
        out.println("Number of rows in leaf partitions" + leafFilesSuffix +
                abbreviatedRowCount(status.getTotalRowsInLeafPartitions()));
        out.println("Percentage of rows in leaf partitions" + percentageSuffix +
                (status.getTotalRowsInLeafPartitions() / (double) status.getTotalRows()) * 100.0);
    }

    private void printCompactionStats(TableFilesStatus status) {
        if (status.getCompactions().isEmpty()) {
            out.println("No compaction jobs assigned to files");
            return;
        }
        out.println("Number of compaction jobs assigned to files: " + compactionsSummaryStr(status, status.getCompactionsAssignedSummary()));
        printTrackerStatusCounts(status.getCompactionsAssignedSummary());
        out.println("Number of compaction jobs assigned more than 12 hours ago: " + compactionsSummaryStr(status, status.getCompactionsAssignedMoreThan12HoursAgoSummary()));
        printTrackerStatusCounts(status.getCompactionsAssignedMoreThan12HoursAgoSummary());
        out.println("Oldest file assignment time: " + status.getOldestCompactionAssignmentTime());
    }

    private void printTrackerStatusCounts(CompactionJobsSummary summary) {
        if (summary.getNumJobsAssignedTo() < 1) {
            return;
        }
        summary.getTrackerStatusCounts().ifPresent(counts -> {
            List<String> countStrs = new ArrayList<>();
            counts.getNumJobsByStatus().forEach((statusType, count) -> {
                if (count > 0) {
                    countStrs.add(statusType + ": " + count);
                }
            });
            if (counts.getNumJobsNotInTracker() > 0) {
                countStrs.add("not in tracker: " + counts.getNumJobsNotInTracker());
            }
            out.println("\tBy job tracker status: " + String.join(", ", countStrs));
        });
    }

    private String compactionsSummaryStr(TableFilesStatus status, CompactionJobsSummary summary) {
        if (summary.getNumJobsAssignedTo() == 0) {
            return "none";
        }
        return String.format("%s, in %s partitions, with %s file references (%.2f%% of references)",
                summary.getNumJobsAssignedTo(), summary.getOnNumPartitions(), summary.getNumReferencesAssigned(),
                summary.getPercentOfFileReferencesAssigned(status.getFileReferenceCount()));
    }

    private void printFile(AllReferencesToAFile file) {
        out.println(file.getFilename()
                + totalReferenceCountStr(file.getReferenceCount())
                + ", last updated at " + file.getLastStateStoreUpdateTime());
        file.getReferences().forEach(this::printFileReference);
    }

    private void printFileReference(FileReference reference) {
        out.println("\tReference in partition " + reference.getPartitionId()
                + ", " + reference.getNumberOfRows() + " rows" + (reference.isCountApproximate() ? " (approx)" : "")
                + ", last updated at " + reference.getLastStateStoreUpdateTime()
                + (reference.getJobId() != null ? ", assigned to job " + reference.getJobId() : ""));
    }

    private void printPartitionCompactions(TableFilesPartitionCompactions compactions) {
        out.println("Partition " + compactions.getPartitionId() + " has " + numJobsStr(compactions) + ", at location " + compactions.getPartitionLocation());
        for (TableFilesCompaction compaction : compactions.getJobs()) {
            out.println("\tJob " + compaction.getJobId() + " with " + compaction.getFileReferences().size() + " files assigned at " + compaction.getAssignmentTime()
                    + jobStatusStr(compaction));
        }
    }

    private String totalReferenceCountStr(int count) {
        if (count < 1) {
            return "";
        } else if (count == 1) {
            return ", 1 reference total";
        } else {
            return ", " + count + " references total";
        }
    }

    private String numJobsStr(TableFilesPartitionCompactions compactions) {
        int jobs = compactions.getJobs().size();
        if (jobs == 1) {
            return "1 job";
        } else {
            return jobs + " jobs";
        }
    }

    private String jobStatusStr(TableFilesCompaction compaction) {
        if (!compaction.isCheckedInJobTracker()) {
            return "";
        }
        return compaction.getTrackedStatus()
                .map(status -> ", tracker shows status " + status.getFurthestStatusType() + " with " + numRunsStr(status) + ", last tracker update at " + status.getLastUpdateTime())
                .orElse(", not in job tracker");
    }

    private String numRunsStr(CompactionJobStatus status) {
        int runs = status.getRunsLatestFirst().size();
        if (runs == 1) {
            return "1 run";
        } else {
            return runs + " runs";
        }
    }
}
