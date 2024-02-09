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
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.io.parquet.utils.RangeQueryUtils;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;
import static sleeper.sketches.s3.SketchesSerDeToS3.sketchesPathForDataFile;

/**
 * Executes a compaction {@link CompactionJob}, i.e. compacts N input files into a single
 * output file.
 */
public class CompactSortedFiles {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Schema schema;
    private final ObjectFactory objectFactory;
    private final CompactionJob compactionJob;
    private final Partition partition;
    private final StateStore stateStore;
    private final CompactionJobStatusStore jobStatusStore;
    private final String taskId;

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactSortedFiles.class);

    public CompactSortedFiles(InstanceProperties instanceProperties,
                              TableProperties tableProperties,
                              ObjectFactory objectFactory,
                              CompactionJob compactionJob,
                              StateStore stateStore,
                              CompactionJobStatusStore jobStatusStore,
                              String taskId) throws StateStoreException {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.schema = this.tableProperties.getSchema();
        this.objectFactory = objectFactory;
        this.compactionJob = compactionJob;
        this.partition = stateStore.getAllPartitions().stream()
                .filter(partition -> Objects.equals(compactionJob.getPartitionId(), partition.getId()))
                .findFirst().orElseThrow(() -> new NoSuchElementException("Partition not found for compaction job"));
        this.stateStore = stateStore;
        this.jobStatusStore = jobStatusStore;
        this.taskId = taskId;
    }

    public RecordsProcessedSummary run() throws IOException, IteratorException, StateStoreException {
        Instant startTime = Instant.now();
        String id = compactionJob.getId();
        LOGGER.info("Compaction job {}: compaction called at {}", id, startTime);
        jobStatusStore.jobStarted(compactionJob, startTime, taskId);

        RecordsProcessed recordsProcessed = compact();

        Instant finishTime = Instant.now();
        // Print summary
        LOGGER.info("Compaction job {}: finished at {}", id, finishTime);

        RecordsProcessedSummary summary = new RecordsProcessedSummary(recordsProcessed, startTime, finishTime);
        METRICS_LOGGER.info("Compaction job {}: compaction run time = {}", id, summary.getDurationInSeconds());
        METRICS_LOGGER.info("Compaction job {}: compaction read {} records at {} per second", id, summary.getRecordsRead(), String.format("%.1f", summary.getRecordsReadPerSecond()));
        METRICS_LOGGER.info("Compaction job {}: compaction wrote {} records at {} per second", id, summary.getRecordsWritten(), String.format("%.1f", summary.getRecordsWrittenPerSecond()));
        jobStatusStore.jobFinished(compactionJob, summary, taskId);
        return summary;
    }

    private RecordsProcessed compact() throws IOException, IteratorException, StateStoreException {
        Configuration conf = getConfiguration();

        // Create a reader for each file
        List<CloseableIterator<Record>> inputIterators = createInputIterators(conf);

        // Merge these iterator into one sorted iterator
        CloseableIterator<Record> mergingIterator = getMergingIterator(inputIterators);

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

        updateStateStoreSuccess(compactionJob.getInputFiles(),
                compactionJob.getOutputFile(),
                compactionJob.getId(),
                compactionJob.getPartitionId(),
                recordsWritten,
                stateStore);
        LOGGER.info("Compaction job {}: compaction committed to state store at {}", compactionJob.getId(), LocalDateTime.now());

        return new RecordsProcessed(totalNumberOfRecordsRead, recordsWritten);
    }

    private List<CloseableIterator<Record>> createInputIterators(Configuration conf) throws IOException {
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

    private CloseableIterator<Record> getMergingIterator(List<CloseableIterator<Record>> inputIterators) throws IteratorException {
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

    private static void updateStateStoreSuccess(List<String> inputFiles,
                                                String outputFile,
                                                String jobId,
                                                String partitionId,
                                                long recordsWritten,
                                                StateStore stateStore) throws StateStoreException {
        FileReference fileReference = FileReference.builder()
                .filename(outputFile)
                .partitionId(partitionId)
                .numberOfRecords(recordsWritten)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
        try {
            stateStore.atomicallyReplaceFileReferencesWithNewOne(jobId, partitionId, inputFiles, fileReference);
            LOGGER.debug("Called atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile method on StateStore");
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating StateStore (moving input files to ready for GC and creating new active file): {}", e.getMessage());
            throw e;
        }
    }
}
