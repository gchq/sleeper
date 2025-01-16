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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequestByTransaction;
import sleeper.core.statestore.commit.StateStoreCommitRequestUploader;
import sleeper.core.statestore.transactionlog.transactions.AddFilesTransaction;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
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
            AmazonSQS sqsClient, InstanceProperties instanceProperties, TableProperties tableProperties,
            StateStoreCommitRequestUploader uploader,
            Consumer<AddFilesTransaction.Builder> transactionConfig) {
        return references -> {
            AddFilesTransaction.Builder requestBuilder = AddFilesTransaction.builder()
                    .files(AllReferencesToAFile.newFilesWithReferences(references));
            transactionConfig.accept(requestBuilder);
            AddFilesTransaction transaction = requestBuilder.build();
            StateStoreCommitRequestByTransaction request = StateStoreCommitRequestByTransaction.create(tableProperties.get(TABLE_ID), transaction);
            LOGGER.debug("Sending asynchronous request to state store committer: {}", request);
            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                    .withMessageBody(uploader.serialiseAndUploadIfTooBig(request))
                    .withMessageGroupId(request.getTableId())
                    .withMessageDeduplicationId(UUID.randomUUID().toString()));
            LOGGER.info("Submitted asynchronous request to add files via state store committer queue");
        };
    }
}
