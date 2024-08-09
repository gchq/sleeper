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
package sleeper.systemtest.dsl.statestore;

import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;

import java.util.List;

public class StateStoreCommitMessageFactory {

    private final String tableId;

    public StateStoreCommitMessageFactory(String tableId) {
        this.tableId = tableId;
    }

    public StateStoreCommitMessage addPartitionFile(String partitionId, String filename, long records) {
        return addFiles(List.of(FileReference.builder()
                .partitionId(partitionId)
                .filename(filename)
                .numberOfRecords(records)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build()));
    }

    public StateStoreCommitMessage addFile(FileReference file) {
        return addFiles(List.of(file));
    }

    public StateStoreCommitMessage addFiles(List<FileReference> files) {
        return StateStoreCommitMessage.tableIdAndBody(tableId,
                new IngestAddFilesCommitRequestSerDe().toJson(
                        IngestAddFilesCommitRequest.builder()
                                .tableId(tableId)
                                .fileReferences(files)
                                .build()));
    }

}
