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
package sleeper.ingest.impl.commit;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

@FunctionalInterface
public interface AddFilesToStateStore {

    void addFiles(List<FileReference> references) throws StateStoreException;

    static AddFilesToStateStore synchronous(StateStore stateStore) {
        return stateStore::addFiles;
    }

    static AddFilesToStateStore synchronous(
            StateStore stateStore, IngestJobStatusStore statusStore,
            Consumer<IngestJobAddedFilesEvent.Builder> statusUpdateConfig) {
        return references -> {
            List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(references);
            stateStore.addFilesWithReferences(files);
            IngestJobAddedFilesEvent.Builder statusUpdateBuilder = IngestJobAddedFilesEvent.builder()
                    .files(files);
            statusUpdateConfig.accept(statusUpdateBuilder);
            statusStore.jobAddedFiles(statusUpdateBuilder.build());
        };
    }

    static AddFilesToStateStore bySqs(
            AmazonSQS sqsClient, InstanceProperties instanceProperties,
            Consumer<IngestAddFilesCommitRequest.Builder> requestConfig) {
        IngestAddFilesCommitRequestSerDe serDe = new IngestAddFilesCommitRequestSerDe();
        return references -> {
            IngestAddFilesCommitRequest.Builder requestBuilder = IngestAddFilesCommitRequest.builder()
                    .fileReferences(references);
            requestConfig.accept(requestBuilder);
            IngestAddFilesCommitRequest request = requestBuilder.build();
            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                    .withMessageBody(serDe.toJson(request))
                    .withMessageGroupId(request.getTableId())
                    .withMessageDeduplicationId(UUID.randomUUID().toString()));
        };
    }
}
