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
package sleeper.query.runner.recordretrieval;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Schema;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.utils.RangeQueryUtils;
import sleeper.query.model.LeafPartitionQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Pulls back records for a single leaf partition according to a provided predicate.
 */
public class LeafPartitionRecordRetrieverImpl implements LeafPartitionRecordRetriever {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeafPartitionRecordRetrieverImpl.class);

    private final Configuration filesConfig;
    private final ExecutorService executorService;

    public LeafPartitionRecordRetrieverImpl(ExecutorService executorService, Configuration conf) {
        this.executorService = executorService;
        this.filesConfig = conf;
    }

    public CloseableIterator<Record> getRecords(List<String> files, Schema dataReadSchema, FilterPredicate filterPredicate) throws RecordRetrievalException {
        if (files.isEmpty()) {
            return new WrappedIterator<>(Collections.emptyIterator());
        }

        ArrayList<RetrieveTask> tasks = new ArrayList<>();
        Map<Integer, CloseableIterator<Record>> indexToReader = new HashMap<>();
        for (String file : files) {
            try {
                tasks.add(new RetrieveTask(createParquetReader(dataReadSchema, file, filterPredicate)));
            } catch (IOException e) {
                throw new RecordRetrievalException("Failed to create a parquet reader", e);
            }
            LOGGER.debug("Created reader for file {}", file);
        }

        List<Future<Pair<Record, CloseableIterator<Record>>>> futures;
        try {
            futures = executorService.invokeAll(tasks);
        } catch (InterruptedException e) {
            throw new RecordRetrievalException("Interrupted while invoking retrieve tasks", e);
        }

        // First record from each iterator is returned separately - this forces
        // the initialisation of the reader and the retrieval of the first
        // batch to be done in parallel.
        Map<Integer, Record> currentValues = new HashMap<>();
        int count = 0;
        for (Future<Pair<Record, CloseableIterator<Record>>> future : futures) {
            Pair<Record, CloseableIterator<Record>> pair;
            try {
                pair = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RecordRetrievalException("Failed to retrieve records due to an exception", e);
            }
            if (null != pair) {
                indexToReader.put(count, pair.getRight());
                currentValues.put(count, pair.getLeft());
                count++;
            }
        }

        // Sort current values and create iterator
        RecordComparator recordComparator = new RecordComparator(dataReadSchema);
        Iterator<Record> currentValuesSorted = currentValues.values()
                .stream()
                .sorted(recordComparator)
                .iterator();

        // Create list of iterators - one for each of the ParquetReaderIterators
        // and one for the values that have already been read.
        List<CloseableIterator<Record>> iterators = new ArrayList<>();
        iterators.add(new WrappedIterator<>(currentValuesSorted));
        iterators.addAll(indexToReader.values());

        return new MergingIterator(dataReadSchema, iterators);
    }

    @Override
    public CloseableIterator<Record> getRecords(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RecordRetrievalException {
        List<String> files = leafPartitionQuery.getFiles();
        if (files.isEmpty()) {
            return new WrappedIterator<>(Collections.emptyIterator());
        }

        FilterPredicate filterPredicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey(
                leafPartitionQuery.getRegions(), leafPartitionQuery.getPartitionRegion());
        return getRecords(files, dataReadSchema, filterPredicate);
    }

    private ParquetReader<Record> createParquetReader(Schema readSchema, String fileName, FilterPredicate filterPredicate) throws IOException {
        // NB Do not create a ParquetReaderIterator here as that forces the
        // opening of the file which needs to be done in parallel.
        return new ParquetRecordReader.Builder(new Path(fileName), readSchema)
                .withConf(filesConfig)
                .withFilter(FilterCompat.get(filterPredicate))
                .build();
    }
}
