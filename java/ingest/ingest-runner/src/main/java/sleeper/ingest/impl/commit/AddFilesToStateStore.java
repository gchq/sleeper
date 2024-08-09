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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3SerDe;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

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
        return bySqs(sqsClient, s3Client, instanceProperties, () -> UUID.randomUUID().toString(), requestConfig);
    }

    static AddFilesToStateStore bySqs(
            AmazonSQS sqsClient, AmazonS3 s3Client, InstanceProperties instanceProperties,
            Supplier<String> s3FilenameSupplier,
            Consumer<IngestAddFilesCommitRequest.Builder> requestConfig) {
        IngestAddFilesCommitRequestSerDe serDe = new IngestAddFilesCommitRequestSerDe();
        return references -> {
            IngestAddFilesCommitRequest.Builder requestBuilder = IngestAddFilesCommitRequest.builder()
                    .fileReferences(references);
            requestConfig.accept(requestBuilder);
            IngestAddFilesCommitRequest request = requestBuilder.build();
            String json = serDe.toJson(request);
            // Store in S3 if the request will not fit in an SQS message
            if (json.length() > 262144) {
                String s3Key = StateStoreCommitRequestInS3.createFileS3Key(request.getTableId(), s3FilenameSupplier.get());
                s3Client.putObject(instanceProperties.get(DATA_BUCKET), s3Key, json);
                json = new StateStoreCommitRequestInS3SerDe().toJson(new StateStoreCommitRequestInS3(s3Key));
                LOGGER.info("Request to add files was too big for an SQS message. Will submit a reference to file in data bucket: {}", s3Key);
            }
            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                    .withMessageBody(json)
                    .withMessageGroupId(request.getTableId())
                    .withMessageDeduplicationId(UUID.randomUUID().toString()));
            LOGGER.debug("Sent request: {}", request);
            LOGGER.info("Submitted asynchronous request to add files via state store committer queue");
        };
    }
}
