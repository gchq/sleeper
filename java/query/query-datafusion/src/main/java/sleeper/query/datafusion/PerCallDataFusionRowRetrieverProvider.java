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
package sleeper.query.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.core.rowretrieval.RowRetrievalException;

import java.io.IOException;

/**
 * Provides a DataFusion row retriever that is safe to use concurrently from multiple threads, e.g. when queries are
 * run in parallel by {@link sleeper.query.core.rowretrieval.QueryExecutor}.
 * <p>
 * {@link DataFusionLeafPartitionRowRetriever} is not thread safe, since it wraps a native FFI context and an Arrow
 * buffer allocator that must not be shared across threads. Callers such as
 * {@link sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor} only resolve a retriever from this provider once
 * and then reuse it for every subsequent call, potentially from many different threads, so isolating threads at the
 * point where the retriever is resolved from the provider would not help. Instead, the single retriever handed out by
 * {@link #getRowRetriever(TableProperties)} creates a brand new context, allocator and delegate retriever on every
 * call to retrieve rows, so there is nothing shared between threads, or even between separate calls on the same
 * thread. The context and allocator are closed when the row iterator returned from that call is closed.
 */
public class PerCallDataFusionRowRetrieverProvider implements LeafPartitionRowRetrieverProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(PerCallDataFusionRowRetrieverProvider.class);

    private final DataFusionAwsConfig awsConfig;
    private final LeafPartitionRowRetriever retriever = new PerCallRowRetriever();

    public PerCallDataFusionRowRetrieverProvider(DataFusionAwsConfig awsConfig) {
        this.awsConfig = awsConfig;
    }

    @Override
    public LeafPartitionRowRetriever getRowRetriever(TableProperties tableProperties) {
        return retriever;
    }

    /**
     * A row retriever that creates a brand new delegate, buffer allocator and FFI context on every call to getRows.
     */
    private final class PerCallRowRetriever implements LeafPartitionRowRetriever {
        @Override
        public CloseableIterator<Row> getRows(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema, TableProperties tableProperties) throws RowRetrievalException {
            BufferAllocator allocator = new RootAllocator();
            FFIContext<DataFusionQueryFunctions> context = FFIContext.getFFIContextSafely(DataFusionQueryFunctions.class);
            try {
                DataFusionLeafPartitionRowRetriever delegate = new DataFusionLeafPartitionRowRetriever(awsConfig, allocator, context);
                CloseableIterator<Row> rows = delegate.getRows(leafPartitionQuery, dataReadSchema, tableProperties);
                return new ClosingRowIterator(rows, allocator, context);
            } catch (RowRetrievalException | RuntimeException e) {
                closeQuietly(context, "FFI context");
                closeQuietly(allocator, "buffer allocator");
                throw e;
            }
        }

        @Override
        public boolean supportsFiltersAndAggregations() {
            return true;
        }

        @Override
        public boolean supportsSqlFiltering() {
            return true;
        }
    }

    private static void closeQuietly(AutoCloseable closeable, String description) {
        try {
            closeable.close();
        } catch (Exception e) {
            LOGGER.warn("Failed to close {}", description, e);
        }
    }

    /**
     * A CloseableIterator of Rows that closes the provided BufferAllocator and FFIContext.
     */
    private static final class ClosingRowIterator implements CloseableIterator<Row> {
        private final CloseableIterator<Row> rows;
        private final BufferAllocator allocator;
        private final FFIContext<DataFusionQueryFunctions> context;

        private ClosingRowIterator(CloseableIterator<Row> rows, BufferAllocator allocator, FFIContext<DataFusionQueryFunctions> context) {
            this.rows = rows;
            this.allocator = allocator;
            this.context = context;
        }

        @Override
        public boolean hasNext() {
            return rows.hasNext();
        }

        @Override
        public Row next() {
            return rows.next();
        }

        @Override
        public void close() throws IOException {
            try {
                rows.close();
            } finally {
                closeQuietly(context, "FFI context");
                closeQuietly(allocator, "buffer allocator");
            }
        }
    }
}
