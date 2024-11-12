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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3Uploader;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.core.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.core.job.status.IngestJobStatusStore;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

@FunctionalInterface
public interface AddFilesToStateStore {
    Logger LOGGER = LoggerFactory.getLogger(AddFilesToStateStore.class);

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
            AmazonSQS sqsClient, AmazonS3 s3Client, InstanceProperties instanceProperties,
            Consumer<IngestAddFilesCommitRequest.Builder> requestConfig) {
        return bySqs(sqsClient, instanceProperties, new StateStoreCommitRequestInS3Uploader(instanceProperties, s3Client::putObject), requestConfig);
    }

    static AddFilesToStateStore bySqs(
            AmazonSQS sqsClient, InstanceProperties instanceProperties,
            StateStoreCommitRequestInS3Uploader s3Uploader,
            Consumer<IngestAddFilesCommitRequest.Builder> requestConfig) {
        IngestAddFilesCommitRequestSerDe serDe = new IngestAddFilesCommitRequestSerDe();
        return references -> {
            IngestAddFilesCommitRequest.Builder requestBuilder = IngestAddFilesCommitRequest.builder()
                    .fileReferences(references);
            requestConfig.accept(requestBuilder);
            IngestAddFilesCommitRequest request = requestBuilder.build();
            String json = serDe.toJson(request);
            LOGGER.debug("Sending asynchronous request to state store committer: {}", request);
            json = s3Uploader.uploadAndWrapIfTooBig(request.getTableId(), json);
            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                    .withMessageBody(json)
                    .withMessageGroupId(request.getTableId())
                    .withMessageDeduplicationId(UUID.randomUUID().toString()));
            LOGGER.info("Submitted asynchronous request to add files via state store committer queue");
        };
    }
}
