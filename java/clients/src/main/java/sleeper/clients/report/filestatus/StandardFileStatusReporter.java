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
import sleeper.core.statestore.FileReference;

import java.io.PrintStream;

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
    public void report(TableFilesStatus status, boolean verbose) {
        out.println("\nFiles Status Report:\n--------------------------");
        out.println("There are " + status.getLeafPartitionCount() + " leaf partitions and " + status.getNonLeafPartitionCount() + " non-leaf partitions");
        out.println("Number of files: " + (status.isMoreThanMax() ? ">" : "") + status.getFileCount());
        out.println("Number of files with references: " + status.getFilesWithReferences().size());
        out.println("Number of files with no references, which will be garbage collected: " + (status.isMoreThanMax() ? ">" : "") + status.getFilesWithNoReferences().size());
        out.println("Number of references to files: " + status.getFileReferenceCount());

        printPartitionStats(status.getLeafPartitionFileReferenceStats(), "leaf");
        printPartitionStats(status.getNonLeafPartitionFileReferenceStats(), "non-leaf");
        printRowStats(status);

        if (verbose) {
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
    }

    private void printPartitionStats(FileReferencesStats partitions, String type) {
        if (partitions.getTotalReferences() > 0) {
            out.format("Number of file references in %s partitions: min = %d, max = %d, mean = %.5f, median = %.5f, mode = %d, total = %s%n",
                    type,
                    partitions.getMinReferences(),
                    partitions.getMaxReferences(),
                    partitions.getMeanReferences(),
                    partitions.getMedianReferences(),
                    partitions.getModalReferences(),
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

    private String totalReferenceCountStr(int count) {
        if (count < 1) {
            return "";
        } else if (count == 1) {
            return ", 1 reference total";
        } else {
            return ", " + count + " references total";
        }
    }
}
