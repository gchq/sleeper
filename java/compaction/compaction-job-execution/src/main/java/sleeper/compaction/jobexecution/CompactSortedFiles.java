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

import com.facebook.collections.ByteArray;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.TableUtils;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.record.Record;
import sleeper.core.record.SingleKeyComparator;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;

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
                              String taskId) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.schema = this.tableProperties.getSchema();
        this.objectFactory = objectFactory;
        this.compactionJob = compactionJob;
        this.stateStore = stateStore;
        this.jobStatusStore = jobStatusStore;
        this.taskId = taskId;
    }

    public RecordsProcessedSummary compact() throws IOException, IteratorException, StateStoreException {
        if (!compactionJob.isSplittingJob()) {
            return compact(this::compactNoSplitting);
        } else {
            return compact(this::compactSplitting);
        }
    }

    public RecordsProcessedSummary compactSplittingByCopy() throws IteratorException, IOException, StateStoreException {
        return compact(this::runCompactSplittingByCopy);
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

    private RecordsProcessed compactNoSplitting() throws IOException, IteratorException {
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
        Map<String, ItemsSketch> keyFieldToSketch = getSketches();

        long recordsWritten = 0L;
        while (mergingIterator.hasNext()) {
            Record record = mergingIterator.next();
            updateQuantilesSketch(record, keyFieldToSketch);
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
        String sketchesFilename = compactionJob.getOutputFile();
        sketchesFilename = FilenameUtils.removeExtension(sketchesFilename);
        sketchesFilename = sketchesFilename + ".sketches";
        Path sketchesPath = new Path(sketchesFilename);
        new SketchesSerDeToS3(schema).saveToHadoopFS(sketchesPath, new Sketches(keyFieldToSketch), conf);
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

    private RecordsProcessed compactSplitting() throws IOException, IteratorException {
        Configuration conf = getConfiguration();

        // Create a reader for each file
        List<CloseableIterator<Record>> inputIterators = createInputIterators(conf);

        // Merge these iterator into one sorted iterator
        CloseableIterator<Record> mergingIterator = getMergingIterator(inputIterators);

        // Create writers
        Path leftPath = new Path(compactionJob.getOutputFiles().getLeft());
        ParquetWriter<Record> leftWriter = ParquetRecordWriterFactory.createParquetRecordWriter(leftPath, tableProperties, conf);
        LOGGER.debug("Compaction job {}: Created writer for file {}", compactionJob.getId(), compactionJob.getOutputFiles().getLeft());

        Path rightPath = new Path(compactionJob.getOutputFiles().getRight());
        ParquetWriter<Record> rightWriter = ParquetRecordWriterFactory.createParquetRecordWriter(rightPath, tableProperties, conf);
        LOGGER.debug("Compaction job {}: Created writer for file {}", compactionJob.getId(), compactionJob.getOutputFiles().getRight());

        Map<String, ItemsSketch> leftKeyFieldToSketch = getSketches();
        Map<String, ItemsSketch> rightKeyFieldToSketch = getSketches();

        long recordsWrittenToLeftFile = 0L;
        long recordsWrittenToRightFile = 0L;
        int dimension = compactionJob.getDimension();
        // Compare using the key of dimension compactionJob.getDimension(), i.e. of that position in the list
        SingleKeyComparator keyComparator = new SingleKeyComparator(schema.getRowKeyTypes().get(dimension));
        String comparisonKeyFieldName = schema.getRowKeyFieldNames().get(dimension);
        LOGGER.debug("Splitting on dimension {} (field name {})", dimension, comparisonKeyFieldName);

        Object splitPoint = compactionJob.getSplitPoint();
        LOGGER.info("Split point is " + splitPoint);

        // TODO This is unnecessarily complicated as the records for the left file will all be written in one go,
        // followed by the records to the right file.
        while (mergingIterator.hasNext()) {
            Record record = mergingIterator.next();
            if (keyComparator.compare(record.get(comparisonKeyFieldName), splitPoint) < 0) {
                leftWriter.write(record);
                recordsWrittenToLeftFile++;
                updateQuantilesSketch(record, leftKeyFieldToSketch);
            } else {
                rightWriter.write(record);
                recordsWrittenToRightFile++;
                updateQuantilesSketch(record, rightKeyFieldToSketch);
            }

            if ((recordsWrittenToLeftFile > 0 && 0 == recordsWrittenToLeftFile % 1_000_000)
                    || (recordsWrittenToRightFile > 0 && 0 == recordsWrittenToRightFile % 1_000_000)) {
                LOGGER.info("Compaction job {}: Written {} records to left file and {} records to right file",
                        compactionJob.getId(), recordsWrittenToLeftFile, recordsWrittenToRightFile);
            }

        }
        leftWriter.close();
        rightWriter.close();
        LOGGER.debug("Compaction job {}: Closed writers", compactionJob.getId());

        // Remove the extension (if present), then add one
        String leftSketchesFilename = getSketchesFilename(compactionJob.getOutputFiles().getLeft());
        Path leftSketchesPath = new Path(leftSketchesFilename);
        new SketchesSerDeToS3(schema).saveToHadoopFS(leftSketchesPath, new Sketches(leftKeyFieldToSketch), conf);

        String rightSketchesFilename = getSketchesFilename(compactionJob.getOutputFiles().getRight());
        Path rightSketchesPath = new Path(rightSketchesFilename);
        new SketchesSerDeToS3(schema).saveToHadoopFS(rightSketchesPath, new Sketches(rightKeyFieldToSketch), conf);

        LOGGER.info("Wrote sketches to {} and {}", leftSketchesPath, rightSketchesPath);

        for (CloseableIterator<Record> iterator : inputIterators) {
            iterator.close();
        }
        LOGGER.debug("Compaction job {}: Closed readers", compactionJob.getId());

        long totalNumberOfRecordsRead = 0L;
        for (CloseableIterator<Record> iterator : inputIterators) {
            totalNumberOfRecordsRead += ((ParquetReaderIterator) iterator).getNumberOfRecordsRead();
        }

        LOGGER.info("Compaction job {}: Read {} records and wrote ({}, {}) records",
                compactionJob.getId(), totalNumberOfRecordsRead, recordsWrittenToLeftFile, recordsWrittenToRightFile);

        updateStateStoreSuccess(compactionJob.getInputFiles(),
                compactionJob.getOutputFiles(),
                compactionJob.getPartitionId(),
                compactionJob.getChildPartitions(),
                new ImmutablePair<>(recordsWrittenToLeftFile, recordsWrittenToRightFile),
                stateStore);
        LOGGER.info("Splitting compaction job {}: compaction committed to state store at {}", compactionJob.getId(), LocalDateTime.now());
        return new RecordsProcessed(totalNumberOfRecordsRead, recordsWrittenToLeftFile + recordsWrittenToRightFile);
    }

    private RecordsProcessed runCompactSplittingByCopy() throws IOException, StateStoreException {
        String outputFilePrefix = TableUtils.buildDataFilePathPrefix(instanceProperties, tableProperties);
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
                String outputFilename = TableUtils.constructPartitionParquetFilePath(
                        outputFilePrefix, childPartitionId, compactionJob.getId() + "-" + i);
                copyFile(inputFilename, outputFilename, conf);
                copyFile(getSketchesFilename(inputFilename), getSketchesFilename(outputFilename), conf);
                recordsProcessed += inputFileInfo.getNumberOfRecords();
                outputFileInfos.add(inputFileInfo.toBuilder()
                        .partitionId(childPartitionId)
                        .filename(outputFilename)
                        .numberOfRecords(inputFileInfo.getNumberOfRecords() / 2)
                        .build());
            }
        }
        stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(inputFileInfos, outputFileInfos);
        return new RecordsProcessed(recordsProcessed, recordsProcessed);
    }

    private static String getSketchesFilename(String filename) {
        return FilenameUtils.removeExtension(filename) + ".sketches";
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
        for (String file : compactionJob.getInputFiles()) {
            ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(file), schema).withConf(conf).build();
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
                                                StateStore stateStore) {
        List<FileInfo> filesToBeMarkedReadyForGC = new ArrayList<>();
        for (String file : inputFiles) {
            FileInfo fileInfo = FileInfo.builder()
                    .filename(file)
                    .partitionId(partitionId)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .build();
            filesToBeMarkedReadyForGC.add(fileInfo);
        }
        FileInfo fileInfo = FileInfo.builder()
                .filename(outputFile)
                .partitionId(partitionId)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(recordsWritten)
                .build();
        try {
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToBeMarkedReadyForGC, fileInfo);
            LOGGER.debug("Called atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile method on DynamoDBStateStore");
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating DynamoDB (moving input files to ready for GC and creating new active file): {}", e.getMessage());
        }
    }

    private static void updateStateStoreSuccess(List<String> inputFiles,
                                                Pair<String, String> outputFiles,
                                                String partition,
                                                List<String> childPartitions,
                                                Pair<Long, Long> recordsWritten,
                                                StateStore stateStore) {
        List<FileInfo> filesToBeMarkedReadyForGC = new ArrayList<>();
        for (String file : inputFiles) {
            FileInfo fileInfo = FileInfo.builder()
                    .filename(file)
                    .partitionId(partition)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .build();
            filesToBeMarkedReadyForGC.add(fileInfo);
        }
        FileInfo leftFileInfo = FileInfo.builder()
                .filename(outputFiles.getLeft())
                .partitionId(childPartitions.get(0))
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(recordsWritten.getLeft())
                .build();
        FileInfo rightFileInfo = FileInfo.builder()
                .filename(outputFiles.getRight())
                .partitionId(childPartitions.get(1))
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(recordsWritten.getRight())
                .build();
        try {
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToBeMarkedReadyForGC, leftFileInfo, rightFileInfo);
            LOGGER.debug("Called atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile method on DynamoDBStateStore");
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating DynamoDB while moving input files to ready for GC and creating new active file", e);
        }
    }

    // TODO These methods are copies of the same ones in IngestRecordsFromIterator - move to sketches module
    private Map<String, ItemsSketch> getSketches() {
        Map<String, ItemsSketch> keyFieldToSketch = new HashMap<>();
        for (Field rowKeyField : schema.getRowKeyFields()) {
            ItemsSketch<?> sketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
            keyFieldToSketch.put(rowKeyField.getName(), sketch);
        }
        return keyFieldToSketch;
    }

    private void updateQuantilesSketch(Record record, Map<String, ItemsSketch> keyFieldToSketch) {
        for (Field rowKeyField : schema.getRowKeyFields()) {
            if (rowKeyField.getType() instanceof ByteArrayType) {
                byte[] value = (byte[]) record.get(rowKeyField.getName());
                keyFieldToSketch.get(rowKeyField.getName()).update(ByteArray.wrap(value));
            } else {
                Object value = record.get(rowKeyField.getName());
                keyFieldToSketch.get(rowKeyField.getName()).update(value);
            }
        }
    }
}
