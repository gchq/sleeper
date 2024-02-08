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

package sleeper.systemtest.suite.dsl.ingest;

import sleeper.core.statestore.FileReference;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.sourcedata.IngestSourceFilesContext;

import java.util.Map;
import java.util.stream.Collectors;

public class SystemTestIngestToStateStore {

    private final SleeperInstanceContext instance;
    private final IngestSourceFilesContext ingestSource;

    public SystemTestIngestToStateStore(SleeperInstanceContext instance, IngestSourceFilesContext ingestSource) {
        this.instance = instance;
        this.ingestSource = ingestSource;
    }

    public void addFileOnPartition(
            String name, String partitionId, long numberOfRecords) throws Exception {
        String path = ingestSource.getFilePath(name);
        instance.getStateStore().addFile(FileReference.builder()
                .filename(path)
                .partitionId(partitionId)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .numberOfRecords(numberOfRecords)
                .build());
    }

    public void addFileWithRecordEstimatesOnPartitions(
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
    }
}
