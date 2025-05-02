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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.statestorev2.transactionlog.snapshots.TransactionLogSnapshotMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;

import static sleeper.core.properties.table.TableProperty.FILES_SNAPSHOT_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.PARTITIONS_SNAPSHOT_BATCH_SIZE;

/**
 * Saves and loads the state of a Sleeper table in Arrow files.
 */
public class StateStoreArrowFileStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreArrowFileStore.class);

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Configuration configuration;
    private final S3TransferManager s3TransferManager;
    private final S3Client s3Client;

    public StateStoreArrowFileStore(InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration, S3Client s3) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.configuration = configuration;
        this.s3TransferManager = null;
        this.s3Client = s3;
    }

    /**
     * Saves the state of partitions in a Sleeper table to an Arrow file.
     *
     * @param  path        path to write the file to
     * @param  partitions  the state
     * @throws IOException if the file could not be written
     */
    public void savePartitions(String path, Collection<Partition> partitions) throws IOException {
        LOGGER.info("Writing {} partitions to {}", partitions.size(), path);
        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                WritableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).create(hadoopPath))) {
            StateStorePartitionsArrowFormat.WriteResult result = StateStorePartitionsArrowFormat.write(
                    partitions, allocator, channel, tableProperties.getInt(PARTITIONS_SNAPSHOT_BATCH_SIZE));
            LOGGER.info("Wrote {} partitions in {} Arrow record batches, to {}",
                    partitions.size(), result.numBatches(), path);
        }
    }

    /**
     * Saves the state of files in a Sleeper table to an Arrow file.
     *
     * @param  path        path to write the file to
     * @param  files       the state
     * @throws IOException if the file could not be written
     */
    public void saveFiles(String path, StateStoreFiles files) throws IOException {
        LOGGER.info("Writing {} files to {}", files.referencedAndUnreferenced().size(), path);
        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                WritableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).create(hadoopPath))) {
            StateStoreFilesArrowFormat.WriteResult result = StateStoreFilesArrowFormat.write(
                    files, allocator, channel, tableProperties.getInt(FILES_SNAPSHOT_BATCH_SIZE));
            LOGGER.info("Wrote {} files with {} references in {} Arrow record batches, to {}",
                    files.referencedAndUnreferenced().size(), result.numReferences(), result.numBatches(), path);
        }
    }

    /**
     * Loads the state of partitions in a Sleeper table from an Arrow file.
     *
     * @param  path        path to the file to read
     * @return             the partitions
     * @throws IOException if the file could not be read
     */
    public List<Partition> loadPartitions(String path) throws IOException {
        LOGGER.debug("Loading partitions from {}", path);

        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                ReadableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).open(hadoopPath))) {
            StateStorePartitionsArrowFormat.ReadResult result = StateStorePartitionsArrowFormat.read(allocator, channel);
            LOGGER.debug("Loaded {} partitions in {} Arrow record batches, from {}",
                    result.partitions().size(), result.numBatches(), path);
            return result.partitions();
        }
    }

    /**
     * Loads the state of files in a Sleeper table from an Arrow file.
     *
     * @param  path        path to the file to read
     * @return             the files
     * @throws IOException if the file could not be read
     */
    public StateStoreFiles loadFiles(String path) throws IOException {
        LOGGER.debug("Loading files from {}", path);
        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                ReadableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).open(hadoopPath))) {
            StateStoreFilesArrowFormat.ReadResult result = StateStoreFilesArrowFormat.read(allocator, channel);
            LOGGER.debug("Loaded {} files with {} references in {} Arrow record batches, from {}",
                    result.files().referencedAndUnreferenced().size(), result.numReferences(), result.numBatches(), path);
            return result.files();
        }
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
                            loadFiles(metadata.getPath()),
                            metadata.getTransactionNumber());
                case PARTITIONS:
                    return new TransactionLogSnapshot(
                            StateStorePartitions.from(loadPartitions(metadata.getPath())),
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
     * @param  path        path to the file to read
     * @return             true if the file is empty
     * @throws IOException if the file could not be read
     */
    public boolean isEmpty(String path) throws IOException {
        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                ReadableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).open(hadoopPath))) {
            return ArrowFormatUtils.isEmpty(allocator, channel);
        }
    }
}
