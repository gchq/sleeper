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
import java.util.List;

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
    public void report(FileStatus fileStatusReport, boolean verbose) {
        out.println("\nFiles Status Report:\n--------------------------");
        out.println("There are " + fileStatusReport.getLeafPartitionCount() + " leaf partitions and " + fileStatusReport.getNonLeafPartitionCount() + " non-leaf partitions");
        out.println("There are " + (fileStatusReport.isReachedMax() ? ">=" : "") + fileStatusReport.getGcFiles().size() + " files with status of \"Ready_to_be_garbage_collected\"");
        out.println("\t(" + fileStatusReport.getReadyForGCFilesInLeafPartitions() + " in leaf partitions, " + fileStatusReport.getReadyForGCInNonLeafPartitions() + " in non-leaf partitions)");
        out.println("There are " + fileStatusReport.getActiveFilesCount() + " files with status of \"Active\"");
        out.println("\t(" + fileStatusReport.getActiveFilesInLeafPartitions() + " in leaf partitions, " + fileStatusReport.getActiveFilesInNonLeafPartitions() + " in non-leaf partitions)");

        printPartitionStats(fileStatusReport.getLeafPartitionStats(), "leaf");
        printPartitionStats(fileStatusReport.getNonLeafPartitionStats(), "non-leaf");

        if (verbose) {
            printFileInfoList("Ready_to_be_garbage_collected", fileStatusReport.getGcFiles());
            printFileInfoList("Active", fileStatusReport.getActiveFiles());
        }
        String percentageSuffix = "= ";
        String allActiveFilesSuffix = "= ";
        if (fileStatusReport.getTotalRecordsApprox() > 0L) {
            allActiveFilesSuffix = "(approx) = ";
            percentageSuffix = "(approx) = ";
        }
        String leafFilesSuffix = "= ";
        if (fileStatusReport.getTotalRecordsInLeafPartitionsApprox() > 0L) {
            leafFilesSuffix = "(approx) = ";
            percentageSuffix = "(approx) = ";
        }
        out.println("Total number of records in all active files " + allActiveFilesSuffix +
                abbreviatedRecordCount(fileStatusReport.getTotalRecords()));
        out.println("Total number of records in leaf partitions " + leafFilesSuffix +
                abbreviatedRecordCount(fileStatusReport.getTotalRecordsInLeafPartitions()));
        out.println("Percentage of records in leaf partitions " + percentageSuffix +
                (fileStatusReport.getTotalRecordsInLeafPartitions() / (double) fileStatusReport.getTotalRecords()) * 100.0);
    }

    private void printPartitionStats(FileStatus.PartitionStats partitions, String type) {
        if (partitions.getTotal() > 0) {
            out.println("Number of files in " + type + " partitions:" +
                    " min = " + partitions.getMinSize() +
                    ", max = " + partitions.getMaxMax() +
                    ", average = " + partitions.getAverageSize());
        } else {
            out.println("No files in " + type + " partitions");
        }
    }

    private void printFileInfoList(String type, List<FileInfo> fileInfoList) {
        out.println(type + ":");
        fileInfoList.forEach(out::println);
    }
}
