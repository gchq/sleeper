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
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SystemTestStateStoreFakeCommits {

    private final SystemTestInstanceContext instance;
    private final Consumer<String> sendCommitMessage;

    public SystemTestStateStoreFakeCommits(SystemTestContext context) {
        this(context.instance(), buildCommitSender(context));
    }

    private SystemTestStateStoreFakeCommits(SystemTestInstanceContext instance, Consumer<String> sendCommitMessage) {
        this.instance = instance;
        this.sendCommitMessage = sendCommitMessage;
    }

    public SystemTestStateStoreFakeCommits forEach(LongStream stream, BiConsumer<Long, SystemTestStateStoreFakeCommits> sendCommits) {
        return this;
    }

    public SystemTestStateStoreFakeCommits addPartitionFile(String partitionId, String filename, long records) {
        addFiles(List.of(FileReference.builder()
                .partitionId(partitionId)
                .filename(filename)
                .numberOfRecords(records)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build()));
        return this;
    }

    private void addFiles(List<FileReference> files) {
        sendCommitMessage.accept(new IngestAddFilesCommitRequestSerDe().toJson(
                IngestAddFilesCommitRequest.builder()
                        .tableId(instance.getTableStatus().getTableUniqueId())
                        .fileReferences(files)
                        .build()));
    }

    private static Consumer<String> buildCommitSender(SystemTestContext context) {
        StateStoreCommitterDriver driver = context.instance().adminDrivers().stateStoreCommitter(context);
        return message -> driver.sendCommitMessages(Stream.of(message));
    }
}
