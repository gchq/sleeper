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
package sleeper.compaction.job.execution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.TimedOutWaitingForFileAssignmentsException;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.io.parquet.utils.RangeQueryUtils;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static sleeper.sketches.s3.SketchesSerDeToS3.sketchesPathForDataFile;

/**
 * Executes a compaction job. Compacts N input files into a single output file.
 */
public class CompactSortedFiles implements CompactionTask.CompactionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactSortedFiles.class);
    public static final int JOB_ASSIGNMENT_WAIT_ATTEMPTS = 10;
    public static final WaitRange JOB_ASSIGNMENT_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(2, 60);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final ObjectFactory objectFactory;
    private final StateStoreProvider stateStoreProvider;

    public CompactSortedFiles(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, ObjectFactory objectFactory) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.objectFactory = objectFactory;
        this.stateStoreProvider = stateStoreProvider;
    }

    public RecordsProcessed compact(CompactionJob compactionJob) throws IOException, IteratorException, StateStoreException, InterruptedException {
        TableProperties tableProperties = tablePropertiesProvider.getById(compactionJob.getTableId());
        Schema schema = tableProperties.getSchema();
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        Partition partition = stateStore.getAllPartitions().stream()
                .filter(p -> Objects.equals(compactionJob.getPartitionId(), p.getId()))
                .findFirst().orElseThrow(() -> new NoSuchElementException("Partition not found for compaction job"));
        Configuration conf = getConfiguration();

        // Create a reader for each file
        List<CloseableIterator<Record>> inputIterators = createInputIterators(compactionJob, partition, schema, conf);

        CloseableIterator<Record> mergingIterator = getMergingIterator(objectFactory, schema, compactionJob, inputIterators);
        // Merge these iterator into one sorted iterator

        // Create writer
        LOGGER.debug("Creating writer for file {}", compactionJob.getOutputFile());
        Path outputPath = new Path(compactionJob.getOutputFile());
        // Setting file writer mode to OVERWRITE so if the same job runs again after failing to
        // update the state store, it will overwrite the existing output file written by the previous run
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(
                outputPath, tableProperties, conf, ParquetFileWriter.Mode.OVERWRITE);

        LOGGER.info("Compaction job {}: Created writer for file {}", compactionJob.getId(), compactionJob.getOutputFile());
        Sketches sketches = Sketches.from(schema);

        long recordsWritten = 0L;
        while (mergingIterator.hasNext()) {
            Record record = mergingIterator.next();
            sketches.update(schema, record);
            // Write out
            writer.write(record);
            recordsWritten++;
            if (0 == recordsWritten % 1_000_000) {
                LOGGER.info("Compaction job {}: Written {} records", compactionJob.getId(), recordsWritten);
            }
        }
        writer.close();
        LOGGER.debug("Compaction job {}: Closed writer", compactionJob.getId());

        // Remove the extension (if present), then add one
        Path sketchesPath = sketchesPathForDataFile(compactionJob.getOutputFile());
        new SketchesSerDeToS3(schema).saveToHadoopFS(sketchesPath, sketches, conf);
        LOGGER.info("Compaction job {}: Wrote sketches file to {}", compactionJob.getId(), sketchesPath);

        for (CloseableIterator<Record> iterator : inputIterators) {
            iterator.close();
        }
        LOGGER.debug("Compaction job {}: Closed readers", compactionJob.getId());

        long totalNumberOfRecordsRead = 0L;
        for (CloseableIterator<Record> iterator : inputIterators) {
            totalNumberOfRecordsRead += ((ParquetReaderIterator) iterator).getNumberOfRecordsRead();
        }

        LOGGER.info("Compaction job {}: Read {} records and wrote {} records", compactionJob.getId(), totalNumberOfRecordsRead, recordsWritten);

        updateStateStoreSuccess(compactionJob, recordsWritten, stateStore);
        LOGGER.info("Compaction job {}: compaction committed to state store at {}", compactionJob.getId(), LocalDateTime.now());

        return new RecordsProcessed(totalNumberOfRecordsRead, recordsWritten);
    }

    private List<CloseableIterator<Record>> createInputIterators(CompactionJob compactionJob, Partition partition, Schema schema, Configuration conf) throws IOException {
        List<CloseableIterator<Record>> inputIterators = new ArrayList<>();
        FilterCompat.Filter partitionFilter = FilterCompat.get(RangeQueryUtils.getFilterPredicate(partition));
        for (String file : compactionJob.getInputFiles()) {
            ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(file), schema)
                    .withConf(conf)
                    .withFilter(partitionFilter)
                    .build();
            ParquetReaderIterator recordIterator = new ParquetReaderIterator(reader);
            inputIterators.add(recordIterator);
            LOGGER.debug("Compaction job {}: Created reader for file {}", compactionJob.getId(), file);
            LOGGER.debug("Compaction job {}: File is being filtered on ranges {}", compactionJob.getId(),
                    partition.getRegion().getRanges().toString());
        }
        return inputIterators;
    }

    public static CloseableIterator<Record> getMergingIterator(
            ObjectFactory objectFactory, Schema schema, CompactionJob compactionJob,
            List<CloseableIterator<Record>> inputIterators) throws IteratorException {
        CloseableIterator<Record> mergingIterator = new MergingIterator(schema, inputIterators);

        // Apply an iterator if one is provided
        if (null != compactionJob.getIteratorClassName()) {
            SortedRecordIterator iterator;
            try {
                iterator = objectFactory.getObject(compactionJob.getIteratorClassName(), SortedRecordIterator.class);
            } catch (ObjectFactoryException e) {
                throw new IteratorException("ObjectFactoryException creating iterator of class " + compactionJob.getIteratorClassName(), e);
            }
            LOGGER.debug("Created iterator of class {}", compactionJob.getIteratorClassName());
            iterator.init(compactionJob.getIteratorConfig(), schema);
            LOGGER.debug("Initialised iterator with config {}", compactionJob.getIteratorConfig());
            mergingIterator = iterator.apply(mergingIterator);
        }
        return mergingIterator;
    }

    private Configuration getConfiguration() {
        return HadoopConfigurationProvider.getConfigurationForECS(instanceProperties);
    }

    public static void updateStateStoreSuccess(
            CompactionJob job,
            long recordsWritten,
            StateStore stateStore) throws StateStoreException, InterruptedException {
        updateStateStoreSuccess(job, recordsWritten, stateStore,
                JOB_ASSIGNMENT_WAIT_ATTEMPTS,
                new ExponentialBackoffWithJitter(JOB_ASSIGNMENT_WAIT_RANGE));
    }

    public static void updateStateStoreSuccess(
            CompactionJob job,
            long recordsWritten,
            StateStore stateStore,
            int jobAssignmentWaitAttempts,
            ExponentialBackoffWithJitter jobAssignmentWaitBackoff) throws StateStoreException, InterruptedException {
        FileReference fileReference = FileReference.builder()
                .filename(job.getOutputFile())
                .partitionId(job.getPartitionId())
                .numberOfRecords(recordsWritten)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();

        // Compaction jobs are sent for execution before updating the state store to assign the input files to the job.
        // Sometimes the compaction can finish before the job assignment is finished. We wait for the job assignment
        // rather than immediately failing the job run.
        FileReferenceNotAssignedToJobException failure = null;
        for (int attempts = 0; attempts < jobAssignmentWaitAttempts; attempts++) {
            jobAssignmentWaitBackoff.waitBeforeAttempt(attempts);
            try {
                stateStore.atomicallyReplaceFileReferencesWithNewOne(job.getId(), job.getPartitionId(), job.getInputFiles(), fileReference);
                LOGGER.debug("Updated file references in state store");
                return;
            } catch (FileReferenceNotAssignedToJobException e) {
                LOGGER.warn("Job not yet assigned to input files on attempt {} of {}: {}",
                        attempts + 1, jobAssignmentWaitAttempts, e.getMessage());
                failure = e;
            }
        }
        throw new TimedOutWaitingForFileAssignmentsException(failure);
    }
}
