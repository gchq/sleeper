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
package sleeper.datasource;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.util.ObjectFactory;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionQueryFunctions;

import java.io.IOException;
import java.util.List;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Doesn't need to be serialisable.
 *
 * NB This initial version returns InternalRow objects. This will be improved in future so that it returns
 * ColumnarBatch. These will come from the DataFusion-based query.
 * SleeperScan will need to use columnarSupportMode to indicate that it supports column mode.
 */
public class SleeperPartitionReader implements PartitionReader<InternalRow> {
    private TableProperties tableProperties;
    private Schema schema;
    private int numFields;
    private LeafPartitionQueryExecutor leafPartitionQueryExecutor;
    private CloseableIterator<Row> rows;

    public SleeperPartitionReader(InstanceProperties instanceProperties, TableProperties tableProperties, InputPartition partition) {
        this.tableProperties = tableProperties;
        this.schema = this.tableProperties.getSchema();
        this.numFields = this.schema.getAllFieldNames().size();

        SleeperInputPartition sleeperInputPartition = (SleeperInputPartition) partition;
        BufferAllocator allocator = new RootAllocator();
        FFIContext<DataFusionQueryFunctions> ffiContext = new FFIContext<>(DataFusionQueryFunctions.getInstance());
        LeafPartitionRowRetriever rowRetriever = new DataFusionLeafPartitionRowRetriever.Provider(DataFusionAwsConfig.getDefault(), allocator, ffiContext).getRowRetriever(tableProperties);

        this.leafPartitionQueryExecutor = new LeafPartitionQueryExecutor(ObjectFactory.noUserJars(), this.tableProperties, rowRetriever);

        LeafPartitionQuery leafPartitionQuery = LeafPartitionQuery.builder()
                .files(sleeperInputPartition.getFiles())
                .leafPartitionId(sleeperInputPartition.getLeafPartitionId())
                .partitionRegion(sleeperInputPartition.getPartitionRegion())
                .tableId(this.tableProperties.get(TABLE_ID))
                .queryId(sleeperInputPartition.getQueryId())
                .subQueryId(sleeperInputPartition.getSubQueryId())
                .regions(List.of(sleeperInputPartition.getRegion()))
                .processingConfig(QueryProcessingConfig.none())
                .build();
        try {
            this.rows = leafPartitionQueryExecutor.getRows(leafPartitionQuery);
        } catch (QueryException e) {
            throw new RuntimeException("Exception calling getRows on leafPartitionQueryExecutor", e);
        }
    }

    @Override
    public void close() throws IOException {
        rows.close();
    }

    @Override
    public boolean next() throws IOException {
        return rows.hasNext();
    }

    @Override
    public InternalRow get() {
        Row row = rows.next();
        Object[] values = new Object[numFields];
        int i = 0;
        for (String fieldName : schema.getAllFieldNames()) {
            Object object = row.get(fieldName);
            // TODO Does this work with lists and maps?
            if (object instanceof String) {
                values[i] = UTF8String.fromString((String) object);
            } else {
                values[i] = object;
            }
            i++;
        }
        return new GenericInternalRow(values);
    }
}