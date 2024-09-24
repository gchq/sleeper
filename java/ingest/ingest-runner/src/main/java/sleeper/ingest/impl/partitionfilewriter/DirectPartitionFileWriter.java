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
package sleeper.ingest.impl.partitionfilewriter;

import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.TableFilePaths;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Writes a Parquet partition file synchronously. The Parquet file and its associated quantile sketches file are written
 * directly to the final file store using a {@link ParquetWriter}.
 */
public class DirectPartitionFileWriter implements PartitionFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectPartitionFileWriter.class);

    private final Schema sleeperSchema;
    private final Partition partition;
    private final Configuration hadoopConfiguration;
    private final String partitionParquetFileName;
    private final String quantileSketchesFileName;
    private final ParquetWriter<Record> parquetWriter;
    private final Map<String, ItemsSketch> keyFieldToSketchMap;
    private long recordsWrittenToCurrentPartition;

    /**
     * Create an instance. The final file store is specified as the prefix to the filePathPrefix argument.
     * <p>
     * Warning: this constructor allows a bespoke Hadoop configuration to be specified, but it will not always be used
     * due to a cache in the underlying {@link org.apache.hadoop.fs.FileSystem} object. This
     * {@link org.apache.hadoop.fs.FileSystem} object maintains a cache of file systems and the first time that it
     * creates a {@link org.apache.hadoop.fs.s3a.S3AFileSystem} object, the provided Hadoop configuration will be used.
     * Thereafter, the Hadoop configuration will be ignored until {@link org.apache.hadoop.fs.FileSystem#closeAll()} is
     * called. This is strange behaviour and can cause errors which are difficult to diagnose.
     *
     * @param  partition            the {@link Partition} that is to be written by this writer
     * @param  parquetConfiguration Hadoop, schema and Parquet configuration for writing files. The Hadoop
     *                              configuration is used to find the classes required to support the file system
     *                              specified in the filePathPrefix.
     * @param  filePaths            the file path generator for S3 objects to write
     * @throws IOException          if there was a failure writing the file
     */
    public DirectPartitionFileWriter(
            Partition partition,
            ParquetConfiguration parquetConfiguration,
            TableFilePaths filePaths,
            String fileName) throws IOException {
        this.sleeperSchema = parquetConfiguration.getTableProperties().getSchema();
        this.partition = requireNonNull(partition);
        this.hadoopConfiguration = parquetConfiguration.getHadoopConfiguration();
        this.partitionParquetFileName = filePaths.constructPartitionParquetFilePath(partition, fileName);
        this.quantileSketchesFileName = filePaths.constructQuantileSketchesFilePath(partition, fileName);
        this.parquetWriter = parquetConfiguration.createParquetWriter(this.partitionParquetFileName);
        LOGGER.info("Created Parquet writer for partition {} to file {}", partition.getId(), partitionParquetFileName);
        this.keyFieldToSketchMap = PartitionFileWriterUtils.createQuantileSketchMap(sleeperSchema);
        this.recordsWrittenToCurrentPartition = 0L;
    }

    /**
     * Append a record to the partition file.
     *
     * @param  record      the record to append
     * @throws IOException if there was a failure writing to the file
     */
    @Override
    public void append(Record record) throws IOException {
        parquetWriter.write(record);
        PartitionFileWriterUtils.updateQuantileSketchMap(
                sleeperSchema,
                keyFieldToSketchMap,
                record);
        recordsWrittenToCurrentPartition++;
        if (recordsWrittenToCurrentPartition % 1000000 == 0) {
            LOGGER.info("Written {} rows to partition {}", recordsWrittenToCurrentPartition, partition.getId());
        }
    }

    /**
     * Close the partition file. In this implementation, the file is closed synchronously and a completed future is
     * returned.
     *
     * @return             a completed future containing the details of the file that was written
     * @throws IOException if there was a failure closing the partition writer or writing the sketches file
     */
    @Override
    public CompletableFuture<FileReference> close() throws IOException {
        parquetWriter.close();
        LOGGER.info("Closed writer for partition {} after writing {} rows", partition.getId(), recordsWrittenToCurrentPartition);
        // Write sketches to an Hadoop file system, which could be s3a:// or file://
        new SketchesSerDeToS3(sleeperSchema).saveToHadoopFS(
                new Path(quantileSketchesFileName),
                new Sketches(keyFieldToSketchMap),
                hadoopConfiguration);
        LOGGER.info("Wrote sketches for partition {} to file {}", partition.getId(), quantileSketchesFileName);
        FileReference fileReference = PartitionFileWriterUtils.createFileReference(
                partitionParquetFileName,
                partition.getId(),
                recordsWrittenToCurrentPartition);
        return CompletableFuture.completedFuture(fileReference);
    }

    /**
     * Abort the writing process and delete the incomplete file.
     */
    @Override
    public void abort() {
        try {
            parquetWriter.close();
        } catch (Exception e) {
            LOGGER.error("Error aborting ParquetWriter", e);
        }
        try {
            Path path = new Path(partitionParquetFileName);
            path.getFileSystem(hadoopConfiguration).delete(path, false);
        } catch (Exception e) {
            LOGGER.error("Error deleting " + partitionParquetFileName, e);
        }
    }
}
