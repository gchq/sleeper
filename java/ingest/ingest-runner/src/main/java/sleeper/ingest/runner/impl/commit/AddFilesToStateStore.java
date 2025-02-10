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
import sleeper.core.statestore.transactionlog.transaction.AddFilesTransaction;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;

import java.util.List;
import java.util.function.Consumer;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

@FunctionalInterface
public interface AddFilesToStateStore {
    Logger LOGGER = LoggerFactory.getLogger(AddFilesToStateStore.class);

    void addFiles(List<FileReference> references) throws StateStoreException;

    static AddFilesToStateStore synchronous(StateStore stateStore) {
        return stateStore::addFiles;
    }

    static AddFilesToStateStore synchronous(
            StateStore stateStore, IngestJobTracker tracker, IngestJobAddedFilesEvent.Builder statusUpdateBuilder) {
        return references -> {
            List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(references);
            stateStore.addFilesWithReferences(files);
            tracker.jobAddedFiles(statusUpdateBuilder.files(files).build());
        };
    }

    static AddFilesToStateStore bySqs(
            TableProperties tableProperties, StateStoreCommitRequestSender commitSender,
            Consumer<AddFilesTransaction.Builder> transactionConfig) {
        return references -> {
            List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(references);
            AddFilesTransaction.Builder requestBuilder = AddFilesTransaction.builder().files(files);
            transactionConfig.accept(requestBuilder);
            AddFilesTransaction transaction = requestBuilder.build();
            commitSender.send(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), transaction));
            LOGGER.info("Submitted asynchronous request to state store committer to add {} files with {} references in table {}", files.size(), references.size(), tableProperties.getStatus());
        };
    }
}
