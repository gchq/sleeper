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
package sleeper.statestore;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AllReferencesToAFile;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;

/**
 * Saves and loads the state of a Sleeper table in Arrow files.
 */
public class StateStoreArrowFileStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreArrowFileStore.class);

    private final Configuration configuration;

    public StateStoreArrowFileStore(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Saves the state of partitions in a Sleeper table to an Arrow file.
     *
     * @param  path          path to write the file to
     * @param  partitions    the state
     * @param  sleeperSchema the Sleeper table schema
     * @throws IOException   if the file could not be written
     */
    public void savePartitions(String path, Collection<Partition> partitions, Schema sleeperSchema) throws IOException {
        LOGGER.debug("Writing {} partitions to {}", partitions.size(), path);
        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                WritableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).create(hadoopPath))) {
            StateStorePartitionsArrowFormat.write(partitions, allocator, channel);
        }
        LOGGER.debug("Wrote {} partitions to {}", partitions.size(), path);
    }

    /**
     * Saves the state of files in a Sleeper table to an Arrow file.
     *
     * @param  path        path to write the file to
     * @param  files       the state
     * @throws IOException if the file could not be written
     */
    public void saveFiles(String path, Collection<AllReferencesToAFile> files) throws IOException {
        LOGGER.debug("Writing {} files to {}", files.size(), path);
        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                WritableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).create(hadoopPath))) {
            StateStoreFilesArrowFormat.write(files, allocator, channel);
        }
        LOGGER.debug("Wrote {} files to {}", files.size(), path);
    }

    /**
     * Loads the state of partitions in a Sleeper table from an Arrow file.
     *
     * @param  path          path to the file to read
     * @param  sleeperSchema the Sleeper table schema
     * @return               the partitions
     * @throws IOException   if the file could not be read
     */
    public List<Partition> loadPartitions(String path, Schema sleeperSchema) throws IOException {
        LOGGER.debug("Loading partitions from {}", path);
        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                ReadableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).open(hadoopPath))) {
            List<Partition> partitions = StateStorePartitionsArrowFormat.read(allocator, channel);
            LOGGER.debug("Loaded {} partitions from {}", partitions.size(), path);
            return partitions;
        }
    }

    /**
     * Loads the state of files in a Sleeper table from an Arrow file.
     *
     * @param  path        path to the file to read
     * @return             the files
     * @throws IOException if the file could not be read
     */
    public List<AllReferencesToAFile> loadFiles(String path) throws IOException {
        LOGGER.debug("Loading files from {}", path);
        Path hadoopPath = new Path(path);
        try (BufferAllocator allocator = new RootAllocator();
                ReadableByteChannel channel = Channels.newChannel(hadoopPath.getFileSystem(configuration).open(hadoopPath))) {
            List<AllReferencesToAFile> files = StateStoreFilesArrowFormat.read(allocator, channel);
            LOGGER.debug("Loaded {} files from {}", files.size(), path);
            return files;
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
