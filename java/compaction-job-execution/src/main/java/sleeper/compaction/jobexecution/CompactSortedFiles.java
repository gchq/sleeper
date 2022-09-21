/*
 * Copyright 2022 Crown Copyright
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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.key.Key;
import sleeper.core.record.Record;
import sleeper.core.record.SingleKeyComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;

/**
 * Executes a compaction {@link CompactionJob}, i.e. compacts N input files into a single
 * output file (in the case of a compaction job within a partition) or into
 * two output files (in the case of a splitting compaction job which reads
 * files in one partition and outputs files to the split partition).
 */
public class CompactSortedFiles {
    private final InstanceProperties instanceProperties;
    private final ObjectFactory objectFactory;
    private final Schema schema;
    private final String rowKeyName0;
    private final MessageType messageType;
    private final CompactionJob compactionJob;
    private final StateStore stateStore;
    private final CompactionJobStatusStore jobStatusStore;
    private final int rowGroupSize;
    private final int pageSize;
    private final String compressionCodec;

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactSortedFiles.class);

    public CompactSortedFiles(InstanceProperties instanceProperties,
                              ObjectFactory objectFactory,
                              Schema schema,
                              MessageType messageType,
                              CompactionJob compactionJob,
                              StateStore stateStore,
                              CompactionJobStatusStore jobStatusStore,
                              int rowGroupSize,
                              int pageSize,
                              String compressionCodec) {
        this.instanceProperties = instanceProperties;
        this.objectFactory = objectFactory;
        this.schema = schema;
        this.rowKeyName0 = schema.getRowKeyFieldNames().get(0);
        this.messageType = messageType;
        this.compactionJob = compactionJob;
        this.stateStore = stateStore;
        this.jobStatusStore = jobStatusStore;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.compressionCodec = compressionCodec;
    }

    public CompactionJobSummary compact() throws IOException, IteratorException {
        LocalDateTime startLDT = LocalDateTime.now();
        String id = compactionJob.getId();
        LOGGER.info("Compaction job {}: compaction called at {}", id, startLDT);
        jobStatusStore.jobStarted(compactionJob);

        CompactionJobSummary summary;
        if (!compactionJob.isSplittingJob()) {
            summary = compactNoSplitting();
        } else {
            summary = compactSplitting();
        }

        LocalDateTime finishLDT = LocalDateTime.now();
        // Print summary
        LOGGER.info("Compaction job {}: finished at {} with status {}", id, finishLDT,
                summary instanceof FailedCompactionJobSummary ? "failed" : "successful");

        double durationInSeconds = Duration.between(startLDT, finishLDT).toMillis() / 1000.0;
        double recordsReadPerSecond = summary.getLinesRead() / durationInSeconds;
        double recordsWrittenPerSecond = summary.getLinesWritten() / durationInSeconds;
        METRICS_LOGGER.info("Compaction job {}: compaction run time = {}", id, durationInSeconds);
        METRICS_LOGGER.info("Compaction job {}: compaction read {} records at {} per second", id, summary.getLinesRead(), String.format("%.1f", recordsReadPerSecond));
        METRICS_LOGGER.info("Compaction job {}: compaction wrote {} records at {} per second", id, summary.getLinesWritten(), String.format("%.1f", recordsWrittenPerSecond));
        return summary;
    }

    private CompactionJobSummary compactNoSplitting() throws IOException, IteratorException {
        Configuration conf = getConfiguration();

        // Create a reader for each file
        List<CloseableIterator<Record>> inputIterators = createInputIterators(conf);

        // Merge these iterator into one sorted iterator
        CloseableIterator<Record> mergingIterator = getMergingIterator(inputIterators);

        // Create writer
        LOGGER.debug("Creating writer for file {}", compactionJob.getOutputFile());
        ParquetRecordWriter.Builder builder = new ParquetRecordWriter.Builder(new Path(compactionJob.getOutputFile()),
                messageType, schema)
                .withCompressionCodec(CompressionCodecName.fromConf(compressionCodec))
                .withRowGroupSize(rowGroupSize)
                .withPageSize(pageSize)
                .withConf(conf);
        ParquetWriter<Record> writer = builder.build();
        LOGGER.info("Compaction job {}: Created writer for file {}", compactionJob.getId(), compactionJob.getOutputFile());
        Map<String, ItemsSketch> keyFieldToSketch = getSketches();

        long linesWritten = 0L;
        // Record min and max of the first dimension of the row key (the min is from the first record, the max is from
        // the last).
        Object minKey = null;
        Object maxKey = null;

        while (mergingIterator.hasNext()) {
            Record record = mergingIterator.next();
            if (null == minKey) {
                minKey = record.get(rowKeyName0);
            }
            maxKey = record.get(rowKeyName0);
            updateQuantilesSketch(record, keyFieldToSketch);
            // Write out
            writer.write(record);
            linesWritten++;
            if (0 == linesWritten % 1_000_000) {
                LOGGER.info("Compaction job {}: Written {} lines", compactionJob.getId(), linesWritten);
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

        long finishTime = System.currentTimeMillis();
        long totalNumberOfLinesRead = 0L;
        for (CloseableIterator<Record> iterator : inputIterators) {
            totalNumberOfLinesRead += ((ParquetReaderIterator) iterator).getNumberOfRecordsRead();
        }

        LOGGER.info("Compaction job {}: Read {} lines and wrote {} lines", compactionJob.getId(), totalNumberOfLinesRead, linesWritten);

        updateStateStoreSuccess(compactionJob.getInputFiles(),
                compactionJob.getOutputFile(),
                compactionJob.getPartitionId(),
                linesWritten,
                minKey,
                maxKey,
                finishTime,
                stateStore,
                schema.getRowKeyTypes());
        LOGGER.info("Compaction job {}: compaction finished at {}", compactionJob.getId(), LocalDateTime.now());

        CompactionJobSummary summary = new CompactionJobSummary(totalNumberOfLinesRead, linesWritten);
        return summary;
    }

    private CompactionJobSummary compactSplitting() throws IOException, IteratorException {
        Configuration conf = getConfiguration();

        // Create a reader for each file
        List<CloseableIterator<Record>> inputIterators = createInputIterators(conf);

        // Merge these iterator into one sorted iterator
        CloseableIterator<Record> mergingIterator = getMergingIterator(inputIterators);

        // Create writers
        ParquetRecordWriter leftWriter = new ParquetRecordWriter(new Path(compactionJob.getOutputFiles().getLeft()), messageType, schema,
                CompressionCodecName.fromConf(compressionCodec), rowGroupSize, pageSize); //4 * 1024 * 1024, DEFAULT_PAGE_SIZE);
        LOGGER.debug("Compaction job {}: Created writer for file {}", compactionJob.getId(), compactionJob.getOutputFiles().getLeft());
        ParquetRecordWriter rightWriter = new ParquetRecordWriter(new Path(compactionJob.getOutputFiles().getRight()), messageType, schema,
                CompressionCodecName.fromConf(compressionCodec), rowGroupSize, pageSize); //4 * 1024 * 1024, DEFAULT_PAGE_SIZE);
        LOGGER.debug("Compaction job {}: Created writer for file {}", compactionJob.getId(), compactionJob.getOutputFiles().getRight());

        Map<String, ItemsSketch> leftKeyFieldToSketch = getSketches();
        Map<String, ItemsSketch> rightKeyFieldToSketch = getSketches();

        long linesWrittenToLeftFile = 0L;
        long linesWrittenToRightFile = 0L;
        // Record min and max of the first dimension of the row key (the min is from the first record, the max is from
        // the last) from both files.
        Object minKeyLeftFile = null;
        Object minKeyRightFile = null;
        Object maxKeyLeftFile = null;
        Object maxKeyRightFile = null;
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
                linesWrittenToLeftFile++;
                if (null == minKeyLeftFile) {
                    minKeyLeftFile = record.get(rowKeyName0);
                }
                maxKeyLeftFile = record.get(rowKeyName0);
                updateQuantilesSketch(record, leftKeyFieldToSketch);
            } else {
                rightWriter.write(record);
                linesWrittenToRightFile++;
                if (null == minKeyRightFile) {
                    minKeyRightFile = record.get(rowKeyName0);
                }
                maxKeyRightFile = record.get(rowKeyName0);
                updateQuantilesSketch(record, rightKeyFieldToSketch);
            }

            if ((linesWrittenToLeftFile > 0 && 0 == linesWrittenToLeftFile % 1_000_000)
                    || (linesWrittenToRightFile > 0 && 0 == linesWrittenToRightFile % 1_000_000)) {
                LOGGER.info("Compaction job {}: Written {} lines to left file and {} lines to right file",
                        compactionJob.getId(), linesWrittenToLeftFile, linesWrittenToRightFile);
            }

        }
        leftWriter.close();
        rightWriter.close();
        LOGGER.debug("Compaction job {}: Closed writers", compactionJob.getId());

        // Remove the extension (if present), then add one
        String leftSketchesFilename = compactionJob.getOutputFiles().getLeft();
        leftSketchesFilename = FilenameUtils.removeExtension(leftSketchesFilename);
        leftSketchesFilename = leftSketchesFilename + ".sketches";
        Path leftSketchesPath = new Path(leftSketchesFilename);
        new SketchesSerDeToS3(schema).saveToHadoopFS(leftSketchesPath, new Sketches(leftKeyFieldToSketch), conf);

        String rightSketchesFilename = compactionJob.getOutputFiles().getRight();
        rightSketchesFilename = FilenameUtils.removeExtension(rightSketchesFilename);
        rightSketchesFilename = rightSketchesFilename + ".sketches";
        Path rightSketchesPath = new Path(rightSketchesFilename);
        new SketchesSerDeToS3(schema).saveToHadoopFS(rightSketchesPath, new Sketches(rightKeyFieldToSketch), conf);

        LOGGER.info("Wrote sketches to {} and {}", leftSketchesPath, rightSketchesPath);

        for (CloseableIterator<Record> iterator : inputIterators) {
            iterator.close();
        }
        LOGGER.debug("Compaction job {}: Closed readers", compactionJob.getId());

        long finishTime = System.currentTimeMillis();
        long totalNumberOfLinesRead = 0L;
        for (CloseableIterator<Record> iterator : inputIterators) {
            totalNumberOfLinesRead += ((ParquetReaderIterator) iterator).getNumberOfRecordsRead();
        }

        LOGGER.info("Compaction job {}: Read {} lines and wrote ({}, {}) lines",
                compactionJob.getId(), totalNumberOfLinesRead, linesWrittenToLeftFile, linesWrittenToRightFile);

        updateStateStoreSuccess(compactionJob.getInputFiles(),
                compactionJob.getOutputFiles(),
                compactionJob.getPartitionId(),
                compactionJob.getChildPartitions(),
                new ImmutablePair<>(linesWrittenToLeftFile, linesWrittenToRightFile),
                new ImmutablePair<>(minKeyLeftFile, minKeyRightFile),
                new ImmutablePair<>(maxKeyLeftFile, maxKeyRightFile),
                finishTime,
                stateStore,
                schema.getRowKeyTypes());
        LOGGER.info("Compaction job {}: compaction finished at {}", compactionJob.getId(), LocalDateTime.now());
        CompactionJobSummary summary = new CompactionJobSummary(totalNumberOfLinesRead, linesWrittenToLeftFile + linesWrittenToRightFile);
        return summary;
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
            SortedRecordIterator iterator = null;
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

    private static boolean updateStateStoreSuccess(List<String> inputFiles,
                                                   String outputFile,
                                                   String partitionId,
                                                   long linesWritten,
                                                   Object minRowKey0,
                                                   Object maxRowKey0,
                                                   long finishTime,
                                                   StateStore stateStore,
                                                   List<PrimitiveType> rowKeyTypes) {
        List<FileInfo> filesToBeMarkedReadyForGC = new ArrayList<>();
        for (String file : inputFiles) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(rowKeyTypes)
                    .filename(file)
                    .partitionId(partitionId)
                    .lastStateStoreUpdateTime(finishTime)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .build();
            filesToBeMarkedReadyForGC.add(fileInfo);
        }
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .filename(outputFile)
                .partitionId(partitionId)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(linesWritten)
                .minRowKey(linesWritten > 0 ? Key.create(minRowKey0) : null)
                .maxRowKey(linesWritten > 0 ? Key.create(maxRowKey0) : null)
                .lastStateStoreUpdateTime(finishTime)
                .build();
        try {
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToBeMarkedReadyForGC, fileInfo);
            LOGGER.debug("Called atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile method on DynamoDBStateStore");
            return true;
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating DynamoDB (moving input files to ready for GC and creating new active file): {}", e.getMessage());
            return false;
        }
    }

    private static boolean updateStateStoreSuccess(List<String> inputFiles,
                                                   Pair<String, String> outputFiles,
                                                   String partition,
                                                   List<String> childPartitions,
                                                   Pair<Long, Long> linesWritten,
                                                   Pair<Object, Object> minKeys,
                                                   Pair<Object, Object> maxKeys,
                                                   long finishTime,
                                                   StateStore stateStore,
                                                   List<PrimitiveType> rowKeyTypes) {
        List<FileInfo> filesToBeMarkedReadyForGC = new ArrayList<>();
        for (String file : inputFiles) {
            FileInfo fileInfo = FileInfo.builder()
                    .rowKeyTypes(rowKeyTypes)
                    .filename(file)
                    .partitionId(partition)
                    .lastStateStoreUpdateTime(finishTime)
                    .fileStatus(FileInfo.FileStatus.ACTIVE)
                    .build();
            filesToBeMarkedReadyForGC.add(fileInfo);
        }
        FileInfo leftFileInfo = FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .filename(outputFiles.getLeft())
                .partitionId(childPartitions.get(0))
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(linesWritten.getLeft())
                .minRowKey(linesWritten.getLeft() > 0 ? Key.create(minKeys.getLeft()) : null)
                .maxRowKey(linesWritten.getLeft() > 0 ? Key.create(maxKeys.getLeft()) : null)
                .lastStateStoreUpdateTime(finishTime)
                .build();
        FileInfo rightFileInfo = FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .filename(outputFiles.getRight())
                .partitionId(childPartitions.get(1))
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(linesWritten.getRight())
                .minRowKey(linesWritten.getRight() > 0 ? Key.create(minKeys.getRight()) : null)
                .maxRowKey(linesWritten.getRight() > 0 ? Key.create(maxKeys.getRight()) : null)
                .lastStateStoreUpdateTime(finishTime)
                .build();
        try {
            stateStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(filesToBeMarkedReadyForGC, leftFileInfo, rightFileInfo);
            LOGGER.debug("Called atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile method on DynamoDBStateStore");
            return true;
        } catch (StateStoreException e) {
            LOGGER.error("Exception updating DynamoDB while moving input files to ready for GC and creating new active file", e);
            return false;
        }
    }

    public static class CompactionJobSummary {
        private final long linesRead;
        private final long linesWritten;

        public CompactionJobSummary(long linesRead, long linesWritten) {
            this.linesRead = linesRead;
            this.linesWritten = linesWritten;
        }

        public long getLinesRead() {
            return linesRead;
        }

        public long getLinesWritten() {
            return linesWritten;
        }
    }

    public static class FailedCompactionJobSummary extends CompactionJobSummary {

        public FailedCompactionJobSummary(long linesRead, long linesWritten) {
            super(linesRead, linesWritten);
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
