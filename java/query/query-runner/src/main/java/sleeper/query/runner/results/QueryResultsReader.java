/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.runner.results;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.iterator.closeable.ConcatenatingIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.util.PollWithRetries;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.utils.TableHadoopConfigurationProvider;
import sleeper.query.core.model.Query;
import sleeper.query.core.tracker.TrackedQuery;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

/**
 * Reads the results of a query that were written to the S3 results bucket.
 */
@FunctionalInterface
public interface QueryResultsReader {

    /**
     * Lists the results files that have been published for a query so far. Each subquery publishes its results as a
     * separate file, so this may be called while the query is still running to find the results of subqueries that
     * have already finished. Returns an empty list if no results have been published yet.
     *
     * @param  tableName the name of the table the query ran against
     * @param  queryId   the query ID
     * @return           the results files published so far
     */
    List<ResultsFile> listResultsFiles(String tableName, String queryId);

    /**
     * Reads the results of a query that has finished. The returned iterator must be closed after use.
     *
     * @param  tableName                     the name of the table the query ran against
     * @param  queryId                       the query ID
     * @return                               an iterator over the rows in the results
     * @throws QueryResultsNotFoundException if no results were published for the query
     */
    default CloseableIterator<Row> getResults(String tableName, String queryId) {
        List<ResultsFile> files = listResultsFiles(tableName, queryId);
        if (files.isEmpty()) {
            throw new QueryResultsNotFoundException(queryId, null);
        }
        return new ConcatenatingIterator(files.stream()
                .map(ResultsFile::contents)
                .toList());
    }

    /**
     * Reads the results of a query that has finished. The returned iterator must be closed after use.
     *
     * @param  query                         the query
     * @return                               an iterator over the rows in the results
     * @throws QueryResultsNotFoundException if no results were published for the query
     */
    default CloseableIterator<Row> getResults(Query query) {
        return getResults(query.getTableName(), query.getQueryId());
    }

    /**
     * Streams the results of a query that has been submitted, as they are published. Each subquery publishes its
     * results as a separate file, and these are read as they are found. The returned iterator must be closed after
     * use.
     *
     * @param  tableName      the name of the table the query ran against
     * @param  queryId        the query ID
     * @param  statusSupplier retrieves the entry for the query in the query tracker, null if it is not tracked yet
     * @param  poll           the polling interval and timeout for each wait for more results
     * @return                an iterator over the rows in the results, as they are published
     */
    default CloseableIterator<Row> streamResults(
            String tableName, String queryId, Supplier<TrackedQuery> statusSupplier, PollWithRetries poll) {
        return new StreamingQueryResultsIterator(tableName, queryId, this, statusSupplier, poll);
    }

    /**
     * Creates a copy of this reader that reads results files with the given number of threads. With more than one
     * thread, the results files are read in parallel and the rows are interleaved across files in no particular order.
     * With one thread, the files are read one at a time in order.
     *
     * @param  threads                  the number of threads to read results files in parallel
     * @return                          the reader
     * @throws IllegalArgumentException if the number of threads is less than 1
     */
    default QueryResultsReader withFileReadThreads(int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("Number of threads to read query results files must be at least 1, found " + threads);
        }
        return new QueryResultsReaderWithThreads(this, threads);
    }

    /**
     * Creates a reader for results written to Parquet files in the query results bucket.
     *
     * @param  instanceProperties      the instance properties
     * @param  tablePropertiesProvider the table properties provider
     * @param  hadoopProvider          the Hadoop configuration provider
     * @return                         the reader
     */
    static QueryResultsReader fromS3(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            TableHadoopConfigurationProvider hadoopProvider) {
        return (tableName, queryId) -> {
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            Schema schema = tableProperties.getSchema();
            Configuration conf = hadoopProvider.getConfiguration(tableProperties);
            Path resultsPath = new Path(instanceProperties.get(FILE_SYSTEM)
                    + instanceProperties.get(QUERY_RESULTS_BUCKET) + "/query-" + queryId);
            try {
                FileSystem fileSystem = resultsPath.getFileSystem(conf);
                return Stream.of(fileSystem.listStatus(resultsPath))
                        .map(FileStatus::getPath)
                        .sorted(Comparator.comparing(Path::getName))
                        .map(file -> new ResultsFile(file.getName(), () -> openResultsFile(file, schema, conf)))
                        .toList();
            } catch (FileNotFoundException e) {
                return List.of();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed listing results of query " + queryId, e);
            }
        };
    }

    private static CloseableIterator<Row> openResultsFile(Path file, Schema schema, Configuration conf) {
        try {
            ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(file, schema)
                    .withConf(conf).build();
            return new ParquetReaderIterator(reader);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed opening results file " + file, e);
        }
    }

    /**
     * A results file published for a query.
     *
     * @param name     the name of the file
     * @param contents opens the file to read its rows, the iterator must be closed after use
     */
    record ResultsFile(String name, Supplier<CloseableIterator<Row>> contents) {
    }
}
