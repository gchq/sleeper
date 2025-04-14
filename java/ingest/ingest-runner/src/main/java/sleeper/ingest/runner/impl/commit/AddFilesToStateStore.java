/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.ingest.runner.impl.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFailedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

@FunctionalInterface
public interface AddFilesToStateStore {
    Logger LOGGER = LoggerFactory.getLogger(AddFilesToStateStore.class);

    void addFiles(List<FileReference> references) throws StateStoreException;

    static AddFilesToStateStore synchronousWithJob(
            TableProperties tableProperties, StateStore stateStore, IngestJobTracker tracker,
            Supplier<Instant> timeSupplier, IngestJobRunIds jobRunIds) {
        TableStatus tableStatus = tableProperties.getStatus();
        return references -> {
            try {
                List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(references);
                new AddFilesTransaction(files).synchronousCommit(stateStore);
                tracker.jobAddedFiles(IngestJobAddedFilesEvent.builder().jobRunIds(jobRunIds).files(files).writtenTime(timeSupplier.get()).build());
                LOGGER.info("Added {} files to state store with {} references in table {}, {}", files.size(), references.size(), tableStatus, jobRunIds);
            } catch (RuntimeException e) {
                tracker.jobFailed(IngestJobFailedEvent.builder().jobRunIds(jobRunIds).failure(e).failureTime(timeSupplier.get()).build());
                throw e;
            }
        };
    }

    static AddFilesToStateStore asynchronousWithJob(
            TableProperties tableProperties, StateStoreCommitRequestSender commitSender,
            Supplier<Instant> timeSupplier, IngestJobRunIds jobRunIds) {
        TableStatus tableStatus = tableProperties.getStatus();
        return references -> {
            List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(references);
            AddFilesTransaction transaction = AddFilesTransaction.builder().jobRunIds(jobRunIds).files(files).writtenTime(timeSupplier.get()).build();
            commitSender.send(StateStoreCommitRequest.create(tableStatus.getTableUniqueId(), transaction));
            LOGGER.info("Submitted asynchronous request to state store committer to add {} files with {} references in table {}, {}",
                    files.size(), references.size(), tableStatus, jobRunIds);
        };
    }

    static AddFilesToStateStore synchronousNoJob(StateStore stateStore) {
        return references -> AddFilesTransaction.fromReferences(references).synchronousCommit(stateStore);
    }

    static AddFilesToStateStore asynchronousNoJob(
            TableProperties tableProperties, StateStoreCommitRequestSender commitSender) {
        return references -> {
            List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(references);
            AddFilesTransaction transaction = new AddFilesTransaction(files);
            commitSender.send(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), transaction));
            LOGGER.info("Submitted asynchronous request to state store committer to add {} files with {} references in table {}", files.size(), references.size(), tableProperties.getStatus());
        };
    }
}
