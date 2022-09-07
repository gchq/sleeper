/*
 * Copyright 2022 Crown Copyright
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
package sleeper.status.report.filestatus;

import sleeper.statestore.FileInfo;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

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
        out.println("Total number of records in all active files = " + abbreviatedRecordCount(fileStatusReport.getTotalRecords()));
        out.println("Total number of records in leaf partitions = " + abbreviatedRecordCount(fileStatusReport.getTotalRecordsInLeafPartitions()));
        out.println("Percentage of records in leaf partitions = " + (fileStatusReport.getTotalRecordsInLeafPartitions() / (double) fileStatusReport.getTotalRecords()) * 100.0);

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

    private void printFileInfoList(String type, List<FileInfo> strings) {
        out.println(type + ":");
        strings.forEach(out::println);
    }

    private static final long K_COUNT = 1_000;
    private static final long M_COUNT = 1_000_000;
    private static final long G_COUNT = 1_000_000_000;
    private static final long T_COUNT = 1_000_000_000_000L;

    private static String abbreviatedRecordCount(long records) {
        if (records < K_COUNT) {
            return "" + records;
        } else if (records < M_COUNT) {
            return Math.round((double) records / K_COUNT) + "K (" + full(records) + ")";
        } else if (records < G_COUNT) {
            return Math.round((double) records / M_COUNT) + "M (" + full(records) + ")";
        } else if (records < T_COUNT) {
            return Math.round((double) records / G_COUNT) + "G (" + full(records) + ")";
        } else {
            return full(Math.round((double) records / T_COUNT)) + "T (" + full(records) + ")";
        }
    }

    private static String full(long records) {
        String str = "" + records;
        int length = str.length();
        int firstPartEnd = length % 3;

        List<String> parts = new ArrayList<>();
        if (firstPartEnd != 0) {
            parts.add(str.substring(0, firstPartEnd));
        }
        for (int i = firstPartEnd; i < length; i += 3) {
            parts.add(str.substring(i, i + 3));
        }
        return String.join(",", parts);
    }

}
