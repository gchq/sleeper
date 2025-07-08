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
package sleeper.compaction.job.execution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.SortedRecordIterator;
import sleeper.core.partition.Partition;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.SleeperRow;
import sleeper.core.schema.Schema;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.parquet.utils.RangeQueryUtils;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.SketchesStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes a compaction job. Compacts N input files into a single output file.
 */
public class JavaCompactionRunner implements CompactionRunner {
    private final ObjectFactory objectFactory;
    private final Configuration configuration;
    private final SketchesStore sketchesStore;

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaCompactionRunner.class);

    public JavaCompactionRunner(ObjectFactory objectFactory, Configuration configuration, SketchesStore sketchesStore) {
        this.objectFactory = objectFactory;
        this.configuration = configuration;
        this.sketchesStore = sketchesStore;
    }

    @Override
    public RecordsProcessed compact(CompactionJob compactionJob, TableProperties tableProperties, Partition partition) throws IOException, IteratorCreationException {
        Schema schema = tableProperties.getSchema();

        // Create a reader for each file
        List<CloseableIterator<SleeperRow>> inputIterators = createInputIterators(compactionJob, partition, schema);

        CloseableIterator<SleeperRow> mergingIterator = getMergingIterator(objectFactory, schema, compactionJob, inputIterators);
        // Merge these iterator into one sorted iterator

        // Create writer
        LOGGER.debug("Creating writer for file {}", compactionJob.getOutputFile());
        Path outputPath = new Path(compactionJob.getOutputFile());
        // Setting file writer mode to OVERWRITE so if the same job runs again after failing to
        // update the state store, it will overwrite the existing output file written
        // by the previous run
        ParquetWriter<SleeperRow> writer = ParquetRecordWriterFactory.createParquetRecordWriter(
                outputPath, tableProperties, configuration, ParquetFileWriter.Mode.OVERWRITE);

        LOGGER.info("Compaction job {}: Created writer for file {}", compactionJob.getId(), compactionJob.getOutputFile());
        Sketches sketches = Sketches.from(schema);

        long recordsWritten = 0L;
        while (mergingIterator.hasNext()) {
            SleeperRow record = mergingIterator.next();
            sketches.update(record);
            // Write out
            writer.write(record);
            recordsWritten++;
            if (0 == recordsWritten % 1_000_000) {
                LOGGER.info("Compaction job {}: Written {} records", compactionJob.getId(), recordsWritten);
            }
        }
        writer.close();
        LOGGER.debug("Compaction job {}: Closed writer", compactionJob.getId());

        sketchesStore.saveFileSketches(compactionJob.getOutputFile(), schema, sketches);
        LOGGER.info("Compaction job {}: Wrote sketches file for {}", compactionJob.getId(), compactionJob.getOutputFile());

        for (CloseableIterator<SleeperRow> iterator : inputIterators) {
            iterator.close();
        }
        LOGGER.debug("Compaction job {}: Closed readers", compactionJob.getId());

        long totalNumberOfRecordsRead = 0L;
        for (CloseableIterator<SleeperRow> iterator : inputIterators) {
            totalNumberOfRecordsRead += ((ParquetReaderIterator) iterator).getNumberOfRecordsRead();
        }

        LOGGER.info("Compaction job {}: Read {} records and wrote {} records", compactionJob.getId(), totalNumberOfRecordsRead, recordsWritten);
        return new RecordsProcessed(totalNumberOfRecordsRead, recordsWritten);
    }

    private List<CloseableIterator<SleeperRow>> createInputIterators(CompactionJob compactionJob, Partition partition, Schema schema) throws IOException {
        List<CloseableIterator<SleeperRow>> inputIterators = new ArrayList<>();

        FilterCompat.Filter partitionFilter = FilterCompat.get(RangeQueryUtils.getFilterPredicate(partition));
        for (String file : compactionJob.getInputFiles()) {
            ParquetReader<SleeperRow> reader = new ParquetRecordReader.Builder(new Path(file), schema)
                    .withConf(configuration)
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

    public static CloseableIterator<SleeperRow> getMergingIterator(
            ObjectFactory objectFactory, Schema schema, CompactionJob compactionJob,
            List<CloseableIterator<SleeperRow>> inputIterators) throws IteratorCreationException {
        CloseableIterator<SleeperRow> mergingIterator = new MergingIterator(schema, inputIterators);

        // Apply an iterator if one is provided
        if (null != compactionJob.getIteratorClassName()) {
            SortedRecordIterator iterator;
            try {
                iterator = objectFactory.getObject(compactionJob.getIteratorClassName(), SortedRecordIterator.class);
            } catch (ObjectFactoryException e) {
                throw new IteratorCreationException("ObjectFactoryException creating iterator of class " + compactionJob.getIteratorClassName(), e);
            }

            LOGGER.debug("Created iterator of class {}", compactionJob.getIteratorClassName());
            iterator.init(compactionJob.getIteratorConfig(), schema);
            LOGGER.debug("Initialised iterator with config {}", compactionJob.getIteratorConfig());
            mergingIterator = iterator.apply(mergingIterator);
        }
        return mergingIterator;
    }

    @Override
    public String implementationLanguage() {
        return "Java";
    }

    @Override
    public boolean isHardwareAccelerated() {
        return false;
    }
}
