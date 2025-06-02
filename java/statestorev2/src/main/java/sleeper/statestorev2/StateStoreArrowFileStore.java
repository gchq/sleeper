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
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.statestorev2.transactionlog.snapshots.TransactionLogSnapshotMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

/**
 * Saves and loads the state of a Sleeper table in Arrow files.
 */
public class StateStoreArrowFileStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreArrowFileStore.class);

    private final InstanceProperties instanceProperties;
    private final S3Client s3Client;

    public StateStoreArrowFileStore(
            InstanceProperties instanceProperties, S3Client s3Client) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
    }

    /**
     * Loads the state of a snapshot in a Sleeper table from an Arrow file.
     *
     * @param  metadata             metadata pointing to the file to read
     * @return                      the snapshot
     * @throws UncheckedIOException if the file could not be read
     */
    public TransactionLogSnapshot loadSnapshot(TransactionLogSnapshotMetadata metadata) {
        try {
            switch (metadata.getType()) {
                case FILES:
                    return new TransactionLogSnapshot(
                            loadFiles(metadata.getObjectKey()),
                            metadata.getTransactionNumber());
                case PARTITIONS:
                    return new TransactionLogSnapshot(
                            StateStorePartitions.from(loadPartitions(metadata.getObjectKey())),
                            metadata.getTransactionNumber());
                default:
                    throw new IllegalArgumentException("Unrecognised snapshot type: " + metadata.getType());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed loading state for snapshot: " + metadata, e);
        }
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

    /**
     * Loads the state of partitions in a Sleeper table from an Arrow file.
     *
     * @param  objectKey   object key in the data bucket of the file to read
     * @return             the partitions
     * @throws IOException if the file could not be read
     */
    private List<Partition> loadPartitions(String objectKey) throws IOException {
        LOGGER.debug("Loading partitions from {}", objectKey);
        return s3Client.getObject(get -> get
                .bucket(instanceProperties.get(DATA_BUCKET))
                .key(objectKey),
                (response, inputStream) -> {
                    try (BufferAllocator allocator = new RootAllocator();
                            ReadableByteChannel channel = Channels.newChannel(inputStream)) {
                        StateStorePartitionsArrowFormat.ReadResult result = StateStorePartitionsArrowFormat.read(allocator, channel);
                        LOGGER.debug("Loaded {} partitions in {} Arrow record batches, from {}",
                                result.partitions().size(), result.numBatches(), objectKey);
                        return result.partitions();
                    }
                });
    }

    /**
     * Loads the state of files in a Sleeper table from an Arrow file.
     *
     * @param  objectKey   object key in the data bucket of the file to read
     * @return             the files
     * @throws IOException if the file could not be read
     */
    private StateStoreFiles loadFiles(String objectKey) throws IOException {
        LOGGER.debug("Loading files from {}", objectKey);
        return s3Client.getObject(get -> get
                .bucket(instanceProperties.get(DATA_BUCKET))
                .key(objectKey),
                (response, inputStream) -> {
                    try (BufferAllocator allocator = new RootAllocator();
                            ReadableByteChannel channel = Channels.newChannel(inputStream)) {
                        StateStoreFilesArrowFormat.ReadResult result = StateStoreFilesArrowFormat.read(allocator, channel);
                        LOGGER.debug("Loaded {} files with {} references in {} Arrow record batches, from {}",
                                result.files().referencedAndUnreferenced().size(), result.numReferences(), result.numBatches(), objectKey);
                        return result.files();
                    }
                });
    }
}
