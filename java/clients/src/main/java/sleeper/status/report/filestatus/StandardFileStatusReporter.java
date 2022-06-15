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

import java.util.List;

/**
 * The standard implementation of {@link FileStatusReporter} that returns file
 * status information to the user on the console.
 */
public class StandardFileStatusReporter implements FileStatusReporter {

    @Override
    public void report(FileStatus fileStatusReport, boolean verbose) {
        System.out.println("\nFiles Status Report:\n--------------------------");
        System.out.println("There are " + fileStatusReport.getLeafPartitionCount() + " leaf partitions and " + fileStatusReport.getNonLeafPartitionCount() + " non-leaf partitions");
        System.out.println("There are " + (fileStatusReport.isReachedMax() ? ">=" : "") + fileStatusReport.getGcFiles().size() + " files with status of \"Ready_to_be_garbage_collected\"");
        System.out.println("\t(" + fileStatusReport.getReadyForGCFilesInLeafPartitions() + " in leaf partitions, " + fileStatusReport.getReadyForGCInNonLeafPartitions() + " in non-leaf partitions)");
        System.out.println("There are " + fileStatusReport.getActiveFilesCount() + " files with status of \"Active\"");
        System.out.println("\t(" + fileStatusReport.getActiveFilesInLeafPartitions() + " in leaf partitions, " + fileStatusReport.getActiveFilesInNonLeafPartitions() + " in non-leaf partitions)");

        printPartitionStats(fileStatusReport.getLeafPartitionStats(), "leaf");
        printPartitionStats(fileStatusReport.getNonLeafPartitionStats(), "non-leaf");

        if (verbose) {
            printFileInfoList("Ready_to_be_garbage_collected", fileStatusReport.getGcFiles());
            printFileInfoList("Active", fileStatusReport.getActiveFiles());
        }
        System.out.println("Total number of records in all active files = " + fileStatusReport.getTotalRecords());
        System.out.println("Total number of records in leaf partitions = " + fileStatusReport.getTotalRecordsInLeafPartitions());
        System.out.println("Percentage of records in leaf partitions = " + (fileStatusReport.getTotalRecordsInLeafPartitions() / (double) fileStatusReport.getTotalRecords()) * 100.0);

    }

    private void printPartitionStats(FileStatus.PartitionStats partitions, String type) {
        if (partitions.getTotal() > 0) {
            System.out.println("Number of files in " + type + " partitions:" +
                    " min = " + partitions.getMinSize() +
                    ", max = " + partitions.getMaxMax() +
                    ", average = " + partitions.getAverageSize());
        } else {
            System.out.println("No files in " + type + " partitions");
        }
    }

    private static void printFileInfoList(String type, List<FileInfo> strings) {
        System.out.println(type + ":");
        strings.forEach(System.out::println);
    }
}
