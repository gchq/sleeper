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

import java.io.PrintStream;

import static sleeper.clients.util.ClientUtils.abbreviatedRecordCount;

/**
 * The standard implementation of {@link FileStatusReporter} that returns file
 * status information to the user on the console.
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
        out.println("There are " + (status.isMoreThanMax() ? ">" : "") + status.getFilesWithNoReferences().size() + " files with no references, which are ready to be garbage collected");
        out.println("There are " + status.getActiveFilesCount() + " files with status of \"Active\"");
        out.println("\t(" + status.getReferencesInLeafPartitions() + " in leaf partitions, " + status.getReferencesInNonLeafPartitions() + " in non-leaf partitions)");

        printPartitionStats(status.getLeafPartitionFileReferenceStats(), "leaf");
        printPartitionStats(status.getNonLeafPartitionFileReferenceStats(), "non-leaf");

        if (verbose) {
            out.print("Files with no references:\n");
            out.println(status.getFilesWithNoReferences());
            out.println("Active files:");
            status.getActiveFiles().forEach(out::println);
        }
        String percentageSuffix = "= ";
        String allActiveFilesSuffix = "= ";
        if (status.getTotalRecordsApprox() > 0L) {
            allActiveFilesSuffix = "(approx) = ";
            percentageSuffix = "(approx) = ";
        }
        String leafFilesSuffix = "= ";
        if (status.getTotalRecordsInLeafPartitionsApprox() > 0L) {
            leafFilesSuffix = "(approx) = ";
            percentageSuffix = "(approx) = ";
        }
        out.println("Total number of records in all active files " + allActiveFilesSuffix +
                abbreviatedRecordCount(status.getTotalRecords()));
        out.println("Total number of records in leaf partitions " + leafFilesSuffix +
                abbreviatedRecordCount(status.getTotalRecordsInLeafPartitions()));
        out.println("Percentage of records in leaf partitions " + percentageSuffix +
                (status.getTotalRecordsInLeafPartitions() / (double) status.getTotalRecords()) * 100.0);
    }

    private void printPartitionStats(TableFilesStatus.PartitionStats partitions, String type) {
        if (partitions.getTotalReferences() > 0) {
            out.println("Number of files in " + type + " partitions:" +
                    " min = " + partitions.getMinReferences() +
                    ", max = " + partitions.getMaxReferences() +
                    ", average = " + partitions.getAverageReferences());
        } else {
            out.println("No files in " + type + " partitions");
        }
    }
}
