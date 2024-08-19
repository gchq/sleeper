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

import java.util.List;

public class StateStoreCommitMessage {

    private final String tableId;
    private final String body;

    private StateStoreCommitMessage(String tableId, String body) {
        this.tableId = tableId;
        this.body = body;
    }

    public static StateStoreCommitMessage tableIdAndBody(String tableId, String body) {
        return new StateStoreCommitMessage(tableId, body);
    }

    public static Commit addFileWithJob(FileReference file) {
        return factory -> factory.addFileWithJob(file);
    }

    public static Commit addFiles(List<FileReference> files) {
        return factory -> factory.addFiles(files);
    }

    public static Commit addFile(FileReference file) {
        return factory -> factory.addFiles(List.of(file));
    }

    public static Commit addPartitionFile(String partitionId, String filename, long records) {
        return factory -> factory.addFiles(
                List.of(FileReference.builder()
                        .partitionId(partitionId)
                        .filename(filename)
                        .numberOfRecords(records)
                        .countApproximate(false)
                        .onlyContainsDataForThisPartition(true)
                        .build()));
    }

    public String getTableId() {
        return tableId;
    }

    public String getBody() {
        return body;
    }

    public interface Commit {
        StateStoreCommitMessage createMessage(StateStoreCommitMessageFactory factory);
    }

}
