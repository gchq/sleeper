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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A CSV implementation of FileStatusReporter that returns file status information to the user on the console as a CSV.
 */
public class CVSFileStatusReporter implements FileStatusReporter {
    List<Object> outputData = new ArrayList<>();

    @Override
    public void report(FileStatus fileStatus, boolean verbose) {
        outputData.clear();

        appendToOutputDataList(fileStatus.getTotalRecords());
        appendToOutputDataList(fileStatus.getTotalRecordsInLeafPartitions());
        appendToOutputDataList(fileStatus.isReachedMax());
        appendToOutputDataList(fileStatus.getLeafPartitionCount());
        appendToOutputDataList(fileStatus.getNonLeafPartitionCount());
        appendToOutputDataList(fileStatus.getActiveFilesCount());
        appendToOutputDataList(fileStatus.getActiveFilesInLeafPartitions());
        appendToOutputDataList(fileStatus.getActiveFilesInNonLeafPartitions());

        appendToOutputDataList(fileStatus.getLeafPartitionStats());
        appendToOutputDataList(fileStatus.getNonLeafPartitionStats());

        System.out.println(this.outputData.stream().map(this::mapData).collect(Collectors.joining(",")));
    }

    private String mapData(Object object) {
        if (object == null) {
            return "null";
        } else {
            return object.toString();
        }
    }

    private void appendToOutputDataList(Object data) {
        outputData.add(data);
    }

    private void appendToOutputDataList(FileStatus.PartitionStats partitionStats) {
        appendToOutputDataList(partitionStats.getTotal());
        appendToOutputDataList(partitionStats.getMaxMax());
        appendToOutputDataList(partitionStats.getMinSize());
        appendToOutputDataList(partitionStats.getAverageSize());
    }
}
