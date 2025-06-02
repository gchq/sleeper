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
package sleeper.statestorev2;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.BlockingOutputStreamAsyncRequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.Upload;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.statestorev2.transactionlog.snapshots.TransactionLogSnapshotMetadata;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.FILES_SNAPSHOT_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.PARTITIONS_SNAPSHOT_BATCH_SIZE;

/**
 * Saves and loads the state of a Sleeper table in Arrow files.
 */
public class StateStoreArrowUploadFileStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreArrowFileStore.class);

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final S3Client s3Client;
    private final S3TransferManager s3TransferManager;

    public StateStoreArrowUploadFileStore(
            InstanceProperties instanceProperties, TableProperties tableProperties, S3Client s3Client, S3TransferManager s3TransferManager) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.s3Client = s3Client;
        this.s3TransferManager = s3TransferManager;
    }

    /**
     * Saves the state of partitions in a Sleeper table to an Arrow file.
     *
     * @param  objectKey   object key in the data bucket to write the file to
     * @param  partitions  the state
     * @throws IOException if the file could not be written
     */
    public void savePartitions(String objectKey, Collection<Partition> partitions) throws IOException {
        LOGGER.info("Writing {} partitions to {}", partitions.size(), objectKey);
        BlockingOutputStreamAsyncRequestBody requestBody = BlockingOutputStreamAsyncRequestBody.builder().build();
        Upload upload = startUpload(objectKey, requestBody);
        try (BufferAllocator allocator = new RootAllocator();
                WritableByteChannel channel = Channels.newChannel(requestBody.outputStream())) {
            StateStorePartitionsArrowFormat.WriteResult result = StateStorePartitionsArrowFormat.write(
                    partitions, allocator, channel, tableProperties.getInt(PARTITIONS_SNAPSHOT_BATCH_SIZE));
            LOGGER.info("Wrote {} partitions in {} Arrow record batches, to {}",
                    partitions.size(), result.numBatches(), objectKey);
        }
        upload.completionFuture().join();
    }

    /**
     * Saves the state of files in a Sleeper table to an Arrow file.
     *
     * @param  objectKey   object key in the data bucket to write the file to
     * @param  files       the state
     * @throws IOException if the file could not be written
     */
    public void saveFiles(String objectKey, StateStoreFiles files) throws IOException {
        LOGGER.info("Writing {} files to {}", files.referencedAndUnreferenced().size(), objectKey);
        BlockingOutputStreamAsyncRequestBody requestBody = BlockingOutputStreamAsyncRequestBody.builder().build();
        Upload upload = startUpload(objectKey, requestBody);
        try (BufferAllocator allocator = new RootAllocator();
                WritableByteChannel channel = Channels.newChannel(requestBody.outputStream())) {
            StateStoreFilesArrowFormat.WriteResult result = StateStoreFilesArrowFormat.write(
                    files, allocator, channel, tableProperties.getInt(FILES_SNAPSHOT_BATCH_SIZE));
            LOGGER.info("Wrote {} files with {} references in {} Arrow record batches, to {}",
                    files.referencedAndUnreferenced().size(), result.numReferences(), result.numBatches(), objectKey);
        }
        upload.completionFuture().join();
    }

    /**
     * Deletes the snapshot file within the bucket.
     *
     * @param metadata metadata for the snapshot
     */
    public void deleteSnapshotFile(TransactionLogSnapshotMetadata metadata) {
        s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(instanceProperties.get(DATA_BUCKET))
                .key(metadata.getObjectKey())
                .build());
    }

    /**
     * Checks if a file contains no Sleeper files or partitions. This checks if the file is empty.
     *
     * @param  objectKey   object key in the data bucket of the file to read
     * @return             true if the file is empty
     * @throws IOException if the file could not be read
     */
    public boolean isEmpty(String objectKey) throws IOException {
        return s3Client.getObject(get -> get
                .bucket(instanceProperties.get(DATA_BUCKET))
                .key(objectKey),
                (response, inputStream) -> {
                    try (BufferAllocator allocator = new RootAllocator();
                            ReadableByteChannel channel = Channels.newChannel(inputStream)) {
                        return ArrowFormatUtils.isEmpty(allocator, channel);
                    }
                });
    }

    private Upload startUpload(String objectKey, BlockingOutputStreamAsyncRequestBody requestBody) {
        return s3TransferManager.upload(request -> request
                .putObjectRequest(put -> put
                        .bucket(instanceProperties.get(DATA_BUCKET))
                        .key(objectKey))
                .requestBody(requestBody));
    }
}
