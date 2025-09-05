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
package sleeper.trino;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

/**
 * A Java copy of the Trino configuration information supplied in an etc/catalog/sleeper.properties file.
 */
public class SleeperConfig {
    private String configBucket = "";
    private int maxSplitBatchSize = 1024;
    private boolean enableTrinoPartitioning = true;
    private long maxArrowRootAllocatorBytes = 2 * 1024 * 1024 * 1024L;
    private long maxBytesToWriteLocallyPerWriter = 2 * 1024 * 1024 * 1024L;
    private String localWorkingDirectory = "/tmp";

    /**
     * The config bucket is the name of an S3 bucket which contains the Sleeper configuration. It should not have a
     * leading s3:// or a trailing slash.
     *
     * @return The name of the config bucket
     */
    @NotNull
    public String getConfigBucket() {
        return configBucket;
    }

    @Config("sleeper.config-bucket")
    public SleeperConfig setConfigBucket(String configBucket) {
        this.configBucket = configBucket;
        return this;
    }

    /**
     * The maximum number of splits to be returned in a single batch. Used by {@link SleeperSplitManager}. Experience
     * suggests that the framework will keep on requesting batches from the split manager and so constraining the size
     * of the batches makes little difference, unless the flow of splits is highly constrained by system parameters such
     * as:
     *
     * <pre>
     *      query.min-schedule-split-batch-size=1
     *      query.schedule-split-batch-size=1
     *      node-scheduler.max-splits-per-node=1
     *      node-scheduler.max-pending-splits-per-task=1
     *      node-scheduler.max-unacknowledged-splits-per-task=1
     *      task.max-worker-threads=1
     * </pre>
     *
     * Not all of these values may be necessary.
     *
     * @return The maximum number of splits to include in a single batch.
     */
    @NotNull
    public int getMaxSplitBatchSize() {
        return maxSplitBatchSize;
    }

    @Config("sleeper.max-split-batch-size")
    public SleeperConfig setMaxSplitBatchSize(int maxSplitBatchSize) {
        this.maxSplitBatchSize = maxSplitBatchSize;
        return this;
    }

    /**
     * Indicate whether Trino should implement write-partitioning when data is written to Sleeper. Write-portitioning
     * sends all of the rows for a particular Sleeper partition to just one Trino worker and so only one worker node
     * will write data to any one Sleeper partition. This ensures that the number of files which are written to Sleeper
     * is kept to a minimum.
     *
     * @return A flag indicating whether Trino partitioning should be enabled or not.
     */
    @NotNull
    public boolean isEnableTrinoPartitioning() {
        return enableTrinoPartitioning;
    }

    @Config("sleeper.enable-trino-partitioning")
    public SleeperConfig setEnableTrinoPartitioning(boolean enableTrinoPartitioning) {
        this.enableTrinoPartitioning = enableTrinoPartitioning;
        return this;
    }

    /**
     * The maximum number of bytes that can be allocated by the Arrow root allocator. The root allocator is used when it
     * is storing batches of rows in-memory before writing to local files or to S3, as well as for working space during
     * the sort and write operations.
     *
     * @return The maximum number of bytes.
     */
    @NotNull
    public long getMaxArrowRootAllocatorBytes() {
        return maxArrowRootAllocatorBytes;
    }

    @Config("sleeper.max-arrow-root-allocator-bytes")
    public SleeperConfig setMaxArrowRootAllocatorBytes(long maxArrowRootAllocatorBytes) {
        this.maxArrowRootAllocatorBytes = maxArrowRootAllocatorBytes;
        return this;
    }

    /**
     * The maximum number of bytes to write to local disk during an insert operation. Note that this is a guide value
     * only, and may be exceeded by (1) large in-memory batches that are written to disk in one go, and (2) the Parquet
     * file as it is prepared to be uploaded.
     *
     * @return The maximum number of bytes.
     */
    @NotNull
    public long getMaxBytesToWriteLocallyPerWriter() {
        return maxBytesToWriteLocallyPerWriter;
    }

    @Config("sleeper.max-bytes-to-write-locally-per-writer")
    public void setMaxBytesToWriteLocallyPerWriter(long maxBytesToWriteLocallyPerWriter) {
        this.maxBytesToWriteLocallyPerWriter = maxBytesToWriteLocallyPerWriter;
    }

    /**
     * The local directory to use to store temporary files during the ingest of data into S3, and other working files
     * such as Sleeper iterators. This directory should ideally have rapid access.
     *
     * @return The full path to the local directory.
     */
    @NotNull
    public String getLocalWorkingDirectory() {
        return localWorkingDirectory;
    }

    @Config("sleeper.local-working-directory")
    public void setLocalWorkingDirectory(String localWorkingDirectory) {
        this.localWorkingDirectory = localWorkingDirectory;
    }
}
