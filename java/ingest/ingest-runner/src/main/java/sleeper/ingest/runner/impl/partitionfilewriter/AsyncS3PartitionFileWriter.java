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
package sleeper.ingest.runner.impl.partitionfilewriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;

import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.table.TableFilePaths;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static sleeper.ingest.runner.impl.partitionfilewriter.PartitionFileWriterUtils.createFileReference;

/**
 * Writes partition files to S3 in an asynchronous manner. Here's a summary of this process:
 * <ul>
 * <li>Data is provided to this class, in sort order, through the {@link #append} method.</li>
 * <li>As the records arrive, local Parquet files are created for each partition. As the records are in sorted order,
 * there will be first be records for one partition, then records for another partition, and so on. (See note
 * below)</li>
 * <li>As each Parquet partition file is completed, an asynchronous upload to S3 is initiated, which will delete the
 * local copy of the Parquet partition file once the upload has completed</li>
 * <li>This whole process repeats until {@link #close()} is called, at which point the remaining partition file is
 * uploaded</li>
 * <li>The {@link #close()} method returns a future which will complete once all of the Parquet partition files have
 * been uploaded</li>
 * </ul>
 * <p>
 * Note that the sort-order and the partition-order may not be the same when the Sleeper Schema has more than one row
 * key. This is the responsibility of the calling classes to handle and this class can assume that all of the records
 * that it receives belong to the same partition.
 */
public class AsyncS3PartitionFileWriter implements PartitionFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncS3PartitionFileWriter.class);

    private final S3TransferManager s3TransferManager;
    private final Schema sleeperSchema;
    private final Partition partition;
    private final String s3BucketName;
    private final Configuration hadoopConfiguration;
    private final String partitionParquetLocalFileName;
    private final String partitionParquetS3Key;
    private final String quantileSketchesLocalFileName;
    private final String quantileSketchesS3Key;
    private final ParquetWriter<Record> parquetWriter;
    private final Sketches sketches;
    private long recordsWrittenToCurrentPartition;

    /**
     * Creates an instance. Warning: this constructor allows a bespoke Hadoop configuration to be specified, but it will
     * not always be used due to a cache in the underlying {@link org.apache.hadoop.fs.FileSystem} object. This
     * {@link org.apache.hadoop.fs.FileSystem} object maintains a cache of file systems and the first time that it
     * creates a {@link org.apache.hadoop.fs.s3a.S3AFileSystem} object, the provided Hadoop configuration will be used.
     * Thereafter, the Hadoop configuration will be ignored until {@link org.apache.hadoop.fs.FileSystem#closeAll()} is
     * called. This is strange behaviour and can cause errors which are difficult to diagnose.
     *
     * @param  partition             the partition to write to
     * @param  parquetConfiguration  Hadoop, schema and Parquet configuration for writing the local Parquet partition
     *                               file
     * @param  s3TransferManager     the manager to use to perform the asynchronous upload
     * @param  localWorkingDirectory the local directory to use to create temporary files
     * @param  s3BucketName          the S3 bucket name and prefix to write to
     * @param  filePaths             the file path generator for S3 objects to write
     * @throws IOException           if there was a failure writing the file
     */
    public AsyncS3PartitionFileWriter(
            Partition partition,
            ParquetConfiguration parquetConfiguration,
            String s3BucketName,
            TableFilePaths filePaths,
            S3TransferManager s3TransferManager,
            String localWorkingDirectory,
            String fileName) throws IOException {
        this.s3TransferManager = requireNonNull(s3TransferManager);
        this.sleeperSchema = parquetConfiguration.getTableProperties().getSchema();
        this.partition = requireNonNull(partition);
        this.s3BucketName = requireNonNull(s3BucketName);
        this.hadoopConfiguration = parquetConfiguration.getHadoopConfiguration();
        this.partitionParquetLocalFileName = String.format("%s/partition_%s_%s.parquet", localWorkingDirectory, partition.getId(), fileName);
        this.quantileSketchesLocalFileName = String.format("%s/partition_%s_%s.sketches", localWorkingDirectory, partition.getId(), fileName);
        this.partitionParquetS3Key = filePaths.constructPartitionParquetFilePath(partition, fileName);
        this.quantileSketchesS3Key = filePaths.constructQuantileSketchesFilePath(partition, fileName);
        this.parquetWriter = parquetConfiguration.createParquetWriter(partitionParquetLocalFileName);
        LOGGER.info("Created Parquet writer for partition {}", partition.getId());
        this.sketches = Sketches.from(sleeperSchema);
        this.recordsWrittenToCurrentPartition = 0L;
    }

    /**
     * Upload the named file, asynchronously, to S3. Deletes the local copy of that file. The future completes once the
     * file has been deleted, and it contains the response which was returned from the file upload.
     *
     * @param  s3TransferManager   the manager to use to perform the asynchronous upload
     * @param  localFileName       the file to upload
     * @param  s3BucketName        the S3 bucket to put the file into
     * @param  s3Key               the S3 key of the uploaded file
     * @param  hadoopConfiguration the Hadoop configuration to use when deleting the local file
     * @return                     the {@link CompletableFuture} which was returned by the upload
     */
    private static CompletableFuture<CompletedFileUpload> asyncUploadLocalFileToS3ThenDeleteLocalCopy(
            S3TransferManager s3TransferManager,
            String localFileName,
            String s3BucketName,
            String s3Key,
            Configuration hadoopConfiguration) {
        File localFile = new File(localFileName);
        FileUpload fileUpload = s3TransferManager.uploadFile(request -> request
                .putObjectRequest(put -> put
                        .bucket(s3BucketName).key(s3Key))
                .source(localFile));
        LOGGER.debug("Started asynchronous upload of local file {} to s3://{}/{}", localFileName, s3BucketName, s3Key);
        return fileUpload.completionFuture().whenComplete((msg, ex) -> {
            LOGGER.debug("Completed asynchronous upload of local file {} to s3://{}/{}", localFileName, s3BucketName, s3Key);
            Path path = new Path(localFileName);
            boolean successfulDelete = false;
            try {
                successfulDelete = path.getFileSystem(hadoopConfiguration).delete(path, false);
            } catch (IOException e) {
                LOGGER.error("Error whilst deleting {}: {}", path, e);
            }
            if (!successfulDelete) {
                LOGGER.warn("Failed to delete local file {}", localFileName);
            }
        });
    }

    /**
     * Append a record to the partition. This writes the record to a local Parquet file and does not upload it to S3.
     *
     * @param  record      the record to append
     * @throws IOException if there was a failure writing to the file
     */
    @Override
    public void append(Record record) throws IOException {
        parquetWriter.write(record);
        sketches.update(record);
        recordsWrittenToCurrentPartition++;
        if (recordsWrittenToCurrentPartition % 1000000 == 0) {
            LOGGER.info("Written {} rows to partition {}", recordsWrittenToCurrentPartition, partition.getId());
        }
    }

    /**
     * Close this partition writer. The local Parquet file is closed and then an asynchronous upload to S3 is initiated,
     * for both the Parquet file and for the associated quantiles sketch file. The local copies are deleted and then the
     * {@link CompletableFuture} completes. The details of new partition file are returned in the completed future.
     *
     * @return             details about the new partition file
     * @throws IOException if there was a failure closing the partition writer or writing the sketches file
     */
    @Override
    public CompletableFuture<FileReference> close() throws IOException {
        parquetWriter.close();
        LOGGER.debug("Closed writer for local partition {} after writing {} rows: file {}",
                partition.getId(),
                recordsWrittenToCurrentPartition,
                partitionParquetLocalFileName);
        // Write sketches to a local file
        new SketchesSerDeToS3(sleeperSchema).saveToHadoopFS(
                new Path(quantileSketchesLocalFileName),
                sketches, hadoopConfiguration);
        LOGGER.debug("Wrote sketches to local file {}", quantileSketchesLocalFileName);
        FileReference fileReference = createFileReference(
                String.format("s3a://%s/%s", s3BucketName, partitionParquetS3Key),
                partition.getId(),
                recordsWrittenToCurrentPartition);
        // Start the asynchronous upload of the files to S3
        CompletableFuture<?> partitionFileUploadFuture = asyncUploadLocalFileToS3ThenDeleteLocalCopy(
                s3TransferManager,
                partitionParquetLocalFileName,
                s3BucketName,
                partitionParquetS3Key,
                hadoopConfiguration);
        CompletableFuture<?> quantileFileUploadFuture = asyncUploadLocalFileToS3ThenDeleteLocalCopy(
                s3TransferManager,
                quantileSketchesLocalFileName,
                s3BucketName,
                quantileSketchesS3Key,
                hadoopConfiguration);
        return CompletableFuture.allOf(partitionFileUploadFuture, quantileFileUploadFuture)
                .thenApply(dummy -> fileReference);
    }

    /**
     * Make a best-effort attempt to delete local files and free up resources.
     */
    @Override
    public void abort() {
        try {
            parquetWriter.close();
        } catch (Exception e) {
            LOGGER.error("Error aborting ParquetWriter", e);
        }
        if (!((new File(partitionParquetLocalFileName)).delete())) {
            LOGGER.error("Failed to delete " + partitionParquetLocalFileName);
        }
        if (!((new File(quantileSketchesLocalFileName)).delete())) {
            LOGGER.error("Failed to delete " + quantileSketchesLocalFileName);
        }
    }
}
