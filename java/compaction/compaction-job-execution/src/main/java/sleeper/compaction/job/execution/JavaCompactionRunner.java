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
import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.partition.Partition;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.IteratorFactory;
import sleeper.core.util.ObjectFactory;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReader;
import sleeper.parquet.row.ParquetRowWriterFactory;
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
    public RowsProcessed compact(CompactionJob compactionJob, TableProperties tableProperties, Partition partition) throws IOException, IteratorCreationException {
        Schema schema = tableProperties.getSchema();

        // Create a reader for each file
        List<CloseableIterator<Row>> inputIterators = createInputIterators(compactionJob, partition, schema);

        CloseableIterator<Row> mergingIterator = getMergingIterator(objectFactory, schema, compactionJob, inputIterators);
        // Merge these iterator into one sorted iterator

        // Create writer
        LOGGER.debug("Creating writer for file {}", compactionJob.getOutputFile());
        Path outputPath = new Path(compactionJob.getOutputFile());
        // Setting file writer mode to OVERWRITE so if the same job runs again after failing to
        // update the state store, it will overwrite the existing output file written
        // by the previous run
        ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(
                outputPath, tableProperties, configuration, ParquetFileWriter.Mode.OVERWRITE);

        LOGGER.info("Compaction job {}: Created writer for file {}", compactionJob.getId(), compactionJob.getOutputFile());
        Sketches sketches = Sketches.from(schema);

        long rowsWritten = 0L;
        while (mergingIterator.hasNext()) {
            Row row = mergingIterator.next();
            sketches.update(row);
            // Write out
            writer.write(row);
            rowsWritten++;
            if (0 == rowsWritten % 1_000_000) {
                LOGGER.info("Compaction job {}: Written {} rows", compactionJob.getId(), rowsWritten);
            }
        }
        writer.close();
        LOGGER.debug("Compaction job {}: Closed writer", compactionJob.getId());

        sketchesStore.saveFileSketches(compactionJob.getOutputFile(), schema, sketches);
        LOGGER.info("Compaction job {}: Wrote sketches file for {}", compactionJob.getId(), compactionJob.getOutputFile());

        for (CloseableIterator<Row> iterator : inputIterators) {
            iterator.close();
        }
        LOGGER.debug("Compaction job {}: Closed readers", compactionJob.getId());

        long totalNumberOfRowsRead = 0L;
        for (CloseableIterator<Row> iterator : inputIterators) {
            totalNumberOfRowsRead += ((ParquetReaderIterator) iterator).getNumberOfRowsRead();
        }

        LOGGER.info("Compaction job {}: Read {} rows and wrote {} rows", compactionJob.getId(), totalNumberOfRowsRead, rowsWritten);
        return new RowsProcessed(totalNumberOfRowsRead, rowsWritten);
    }

    private List<CloseableIterator<Row>> createInputIterators(CompactionJob compactionJob, Partition partition, Schema schema) throws IOException {
        List<CloseableIterator<Row>> inputIterators = new ArrayList<>();

        FilterCompat.Filter partitionFilter = FilterCompat.get(RangeQueryUtils.getFilterPredicate(partition));
        for (String file : compactionJob.getInputFiles()) {
            ParquetReader<Row> reader = new ParquetRowReader.Builder(new Path(file), schema)
                    .withConf(configuration)
                    .withFilter(partitionFilter)
                    .build();
            ParquetReaderIterator rowIterator = new ParquetReaderIterator(reader);
            inputIterators.add(rowIterator);
            LOGGER.debug("Compaction job {}: Created reader for file {}", compactionJob.getId(), file);
            LOGGER.debug("Compaction job {}: File is being filtered on ranges {}", compactionJob.getId(),
                    partition.getRegion().getRanges().toString());
        }
        return inputIterators;
    }

    public static CloseableIterator<Row> getMergingIterator(
            ObjectFactory objectFactory, Schema schema, CompactionJob compactionJob,
            List<CloseableIterator<Row>> inputIterators) throws IteratorCreationException {
        CloseableIterator<Row> mergingIterator = new MergingIterator(schema, inputIterators);

        // Apply an iterator if one is provided
        if (null != compactionJob.getIteratorClassName()) {
            SortedRowIterator iterator;
            IteratorFactory iterFactory = new IteratorFactory(objectFactory);
            iterator = iterFactory.getIterator(compactionJob.getIteratorClassName(), compactionJob.getIteratorConfig(), schema);
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
