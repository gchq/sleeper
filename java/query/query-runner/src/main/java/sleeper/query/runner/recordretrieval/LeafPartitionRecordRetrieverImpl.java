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
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.row.RowComparator;
import sleeper.core.schema.Schema;
import sleeper.parquet.record.ParquetRowReader;
import sleeper.parquet.utils.RangeQueryUtils;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.core.rowretrieval.RowRetrievalException;

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

import static sleeper.core.properties.table.TableProperty.PARQUET_QUERY_COLUMN_INDEX_ENABLED;

/**
 * Pulls back records for a single leaf partition according to a provided predicate.
 */
public class LeafPartitionRecordRetrieverImpl implements LeafPartitionRowRetriever {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeafPartitionRecordRetrieverImpl.class);

    private final Configuration filesConfig;
    private final ExecutorService executorService;
    private final TableProperties tableProperties;

    public LeafPartitionRecordRetrieverImpl(ExecutorService executorService, Configuration conf, TableProperties tableProperties) {
        this.executorService = executorService;
        this.filesConfig = conf;
        this.tableProperties = tableProperties;
    }

    public static LeafPartitionRowRetrieverProvider createProvider(ExecutorService executorService, Configuration conf) {
        return tableProperties -> new LeafPartitionRecordRetrieverImpl(executorService, conf, tableProperties);
    }

    public CloseableIterator<Row> getRecords(List<String> files, Schema dataReadSchema, FilterPredicate filterPredicate) throws RowRetrievalException {
        if (files.isEmpty()) {
            return new WrappedIterator<>(Collections.emptyIterator());
        }

        ArrayList<RetrieveTask> tasks = new ArrayList<>();
        Map<Integer, CloseableIterator<Row>> indexToReader = new HashMap<>();
        for (String file : files) {
            try {
                tasks.add(new RetrieveTask(createParquetReader(dataReadSchema, file, filterPredicate)));
            } catch (IOException e) {
                throw new RowRetrievalException("Failed to create a parquet reader", e);
            }
            LOGGER.debug("Created reader for file {}", file);
        }

        List<Future<Pair<Row, CloseableIterator<Row>>>> futures;
        try {
            futures = executorService.invokeAll(tasks);
        } catch (InterruptedException e) {
            throw new RowRetrievalException("Interrupted while invoking retrieve tasks", e);
        }

        // First record from each iterator is returned separately - this forces
        // the initialisation of the reader and the retrieval of the first
        // batch to be done in parallel.
        Map<Integer, Row> currentValues = new HashMap<>();
        int count = 0;
        for (Future<Pair<Row, CloseableIterator<Row>>> future : futures) {
            Pair<Row, CloseableIterator<Row>> pair;
            try {
                pair = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RowRetrievalException("Failed to retrieve records due to an exception", e);
            }
            if (null != pair) {
                indexToReader.put(count, pair.getRight());
                currentValues.put(count, pair.getLeft());
                count++;
            }
        }

        // Sort current values and create iterator
        RowComparator rowComparator = new RowComparator(dataReadSchema);
        Iterator<Row> currentValuesSorted = currentValues.values()
                .stream()
                .sorted(rowComparator)
                .iterator();

        // Create list of iterators - one for each of the ParquetReaderIterators
        // and one for the values that have already been read.
        List<CloseableIterator<Row>> iterators = new ArrayList<>();
        iterators.add(new WrappedIterator<>(currentValuesSorted));
        iterators.addAll(indexToReader.values());

        return new MergingIterator(dataReadSchema, iterators);
    }

    @Override
    public CloseableIterator<Row> getRows(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RowRetrievalException {
        List<String> files = leafPartitionQuery.getFiles();
        if (files.isEmpty()) {
            return new WrappedIterator<>(Collections.emptyIterator());
        }

        FilterPredicate filterPredicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey(
                leafPartitionQuery.getRegions(), leafPartitionQuery.getPartitionRegion());
        return getRecords(files, dataReadSchema, filterPredicate);
    }

    private ParquetReader<Row> createParquetReader(Schema readSchema, String fileName, FilterPredicate filterPredicate) throws IOException {
        // NB Do not create a ParquetReaderIterator here as that forces the
        // opening of the file which needs to be done in parallel.
        return new ParquetRowReader.Builder(new Path(fileName), readSchema)
                .withConf(filesConfig)
                .withFilter(FilterCompat.get(filterPredicate))
                .useColumnIndexFilter(tableProperties.getBoolean(PARQUET_QUERY_COLUMN_INDEX_ENABLED))
                .build();
    }
}
