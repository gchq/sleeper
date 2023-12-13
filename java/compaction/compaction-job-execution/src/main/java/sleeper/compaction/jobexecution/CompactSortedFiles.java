/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.compaction.jobexecution;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionOutputFileNameFactory;
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
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.SplitFileInfo;
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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;
import static sleeper.sketches.s3.SketchesSerDeToS3.sketchesPathForDataFile;

/**
 * Executes a compaction {@link CompactionJob}, i.e. compacts N input files into a single
 * output file (in the case of a compaction job within a partition) or into
 * two output files (in the case of a splitting compaction job which reads
 * files in one partition and outputs files to the split partition).
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
    private final CompactionOutputFileNameFactory fileNameFactory;

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
        this.fileNameFactory = CompactionOutputFileNameFactory.forTable(instanceProperties, tableProperties);
    }

    public RecordsProcessedSummary compact() throws IOException, IteratorException, StateStoreException {
        if (!compactionJob.isSplittingJob()) {
            return compact(this::compactNoSplitting);
        } else {
            return compact(this::compactSplittingByCopy);
        }
    }

    private interface RunCompaction {
        RecordsProcessed run() throws IOException, IteratorException, StateStoreException;
    }

    private RecordsProcessedSummary compact(RunCompaction runJob) throws IOException, IteratorException, StateStoreException {
        Instant startTime = Instant.now();
        String id = compactionJob.getId();
        LOGGER.info("Compaction job {}: compaction called at {}", id, startTime);
        jobStatusStore.jobStarted(compactionJob, startTime, taskId);

        RecordsProcessed recordsProcessed = runJob.run();

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

    private RecordsProcessed compactNoSplitting() throws IOException, IteratorException, StateStoreException {
        Configuration conf = getConfiguration();

        // Create a reader for each file
        List<CloseableIterator<Record>> inputIterators = createInputIterators(conf);

        // Merge these iterator into one sorted iterator
        CloseableIterator<Record> mergingIterator = getMergingIterator(inputIterators);

        // Create writer
        LOGGER.debug("Creating writer for file {}", compactionJob.getOutputFile());
        Path outputPath = new Path(compactionJob.getOutputFile());
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(outputPath, tableProperties, conf);

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
                compactionJob.getPartitionId(),
                recordsWritten,
                stateStore);
        LOGGER.info("Compaction job {}: compaction committed to state store at {}", compactionJob.getId(), LocalDateTime.now());

        return new RecordsProcessed(totalNumberOfRecordsRead, recordsWritten);
    }

    private RecordsProcessed compactSplittingByCopy() throws IOException, StateStoreException {
        Configuration conf = getConfiguration();
        Map<String, FileInfo> activeFileByName = stateStore.getActiveFiles().stream()
                .collect(Collectors.toMap(FileInfo::getFilename, Function.identity()));
        long recordsProcessed = 0;
        List<FileInfo> inputFileInfos = compactionJob.getInputFiles().stream()
                .map(activeFileByName::get)
                .collect(Collectors.toUnmodifiableList());
        List<FileInfo> outputFileInfos = new ArrayList<>();
        for (int i = 0; i < inputFileInfos.size(); i++) {
            FileInfo inputFileInfo = inputFileInfos.get(i);
            String inputFilename = inputFileInfo.getFilename();
            for (String childPartitionId : compactionJob.getChildPartitions()) {
                String outputFilename = filenameInPartition(childPartitionId, i);
                copyFile(inputFilename, outputFilename, conf);
                copyFile(getSketchesFilename(inputFilename), getSketchesFilename(outputFilename), conf);
                recordsProcessed += inputFileInfo.getNumberOfRecords();
                outputFileInfos.add(SplitFileInfo.copyToChildPartition(inputFileInfo, childPartitionId, outputFilename));
            }
        }
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(inputFileInfos, outputFileInfos);
        return new RecordsProcessed(recordsProcessed, recordsProcessed);
    }

    private static String getSketchesFilename(String filename) {
        return FilenameUtils.removeExtension(filename) + ".sketches";
    }

    private String filenameInPartition(String partitionId, int fileIndex) {
        return fileNameFactory.jobPartitionFile(compactionJob.getId(), partitionId, fileIndex);
    }

    private static void copyFile(String inputFilename, String outputFilename, Configuration conf) throws IOException {
        Path inputFile = new Path(inputFilename);
        Path outputFile = new Path(outputFilename);
        FileUtil.copy(inputFile.getFileSystem(conf), inputFile,
                outputFile.getFileSystem(conf), outputFile,
                false, conf);
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
                                                String partitionId,
                                                long recordsWritten,
                                                StateStore stateStore) throws StateStoreException {
        List<FileInfo> filesToBeMarkedReadyForGC = new ArrayList<>();
        for (String file : inputFiles) {
            FileInfo fileInfo = FileInfo.wholeFile()
                    .filename(file)
                    .partitionId(partitionId)
                    .numberOfRecords(recordsWritten)
                    .build();
            filesToBeMarkedReadyForGC.add(fileInfo);
        }
        FileInfo fileInfo = FileInfo.wholeFile()
                .filename(outputFile)
                .partitionId(partitionId)
                .numberOfRecords(recordsWritten)
                .build();
        try {
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToBeMarkedReadyForGC, fileInfo);
            LOGGER.debug("Called atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile method on DynamoDBStateStore");
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating DynamoDB (moving input files to ready for GC and creating new active file): {}", e.getMessage());
            throw e;
        }
    }
}
