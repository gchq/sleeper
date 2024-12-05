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

package sleeper.systemtest.dsl.ingest;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.FileReference;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SystemTestIngestToStateStore {

    private final SystemTestInstanceContext instance;
    private final IngestSourceFilesContext ingestSource;

    public SystemTestIngestToStateStore(SystemTestInstanceContext instance, IngestSourceFilesContext ingestSource) {
        this.instance = instance;
        this.ingestSource = ingestSource;
    }

    public SystemTestIngestToStateStore addFileOnPartition(
            String name, String partitionId, long numberOfRecords) throws Exception {
        String path = ingestSource.getFilePath(name);
        instance.getStateStore().addFile(FileReference.builder()
                .filename(path)
                .partitionId(partitionId)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .numberOfRecords(numberOfRecords)
                .build());
        return this;
    }

    public SystemTestIngestToStateStore addFileWithRecordEstimatesOnPartitions(
            String name, Map<String, Long> recordsByPartition) throws Exception {
        String path = ingestSource.getFilePath(name);
        boolean singlePartition = recordsByPartition.size() == 1;
        instance.getStateStore().addFiles(recordsByPartition.entrySet().stream()
                .map(entry -> FileReference.builder()
                        .filename(path)
                        .partitionId(entry.getKey())
                        .countApproximate(true)
                        .onlyContainsDataForThisPartition(singlePartition)
                        .numberOfRecords(entry.getValue())
                        .build())
                .collect(Collectors.toUnmodifiableList()));
        return this;
    }

    public SystemTestIngestToStateStore addFileOnEveryPartition(String name, long numberOfRecords) throws Exception {
        String path = ingestSource.getFilePath(name);
        PartitionTree partitionTree = new PartitionTree(instance.getStateStore().getAllPartitions());
        List<Partition> leafPartitions = partitionTree.getLeafPartitions();
        long recordsPerPartition = numberOfRecords / leafPartitions.size();
        instance.getStateStore().addFiles(leafPartitions.stream()
                .map(partition -> FileReference.builder()
                        .filename(path)
                        .partitionId(partition.getId())
                        .countApproximate(true)
                        .onlyContainsDataForThisPartition(false)
                        .numberOfRecords(recordsPerPartition)
                        .build())
                .collect(Collectors.toUnmodifiableList()));
        return this;
    }
}
