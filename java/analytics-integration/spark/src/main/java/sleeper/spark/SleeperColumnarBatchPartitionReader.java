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
package sleeper.spark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.rowretrieval.RowRetrievalException;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionQueryFunctions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Reads data from a Sleeper table using the DataFusionLeafPartitionRowRetriever. This returns Arrow
 * record batches which are converted into Spark {@link ColumnarBatch}s.
 */
public class SleeperColumnarBatchPartitionReader implements PartitionReader<ColumnarBatch> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperColumnarBatchPartitionReader.class);

    private final TableProperties tableProperties;
    private final Schema schema;
    private final ArrowReader arrowReader;
    private final BufferAllocator allocator;
    private final FFIContext<DataFusionQueryFunctions> ffiContext;
    private long numBatches;

    public SleeperColumnarBatchPartitionReader(InstanceProperties instanceProperties, TableProperties tableProperties, InputPartition partition) {
        this.tableProperties = tableProperties;
        this.schema = this.tableProperties.getSchema();

        SleeperInputPartition sleeperInputPartition = (SleeperInputPartition) partition;
        this.allocator = new RootAllocator();
        this.ffiContext = FFIContext.getFFIContext(DataFusionQueryFunctions.class);

        LeafPartitionQuery leafPartitionQuery = LeafPartitionQuery.builder()
                .files(sleeperInputPartition.getFiles())
                .leafPartitionId(sleeperInputPartition.getLeafPartitionId())
                .partitionRegion(sleeperInputPartition.getPartitionRegion())
                .tableId(this.tableProperties.get(TABLE_ID))
                .queryId(sleeperInputPartition.getQueryId())
                .subQueryId(sleeperInputPartition.getSubQueryId())
                .regions(sleeperInputPartition.getRegions())
                .processingConfig(QueryProcessingConfig.none())
                .build();
        numBatches = 0L;
        try {
            this.arrowReader = new DataFusionLeafPartitionRowRetriever(DataFusionAwsConfig.getDefault(), allocator, ffiContext)
                    .getArrowReader(leafPartitionQuery, schema, tableProperties);
            LOGGER.info("Created ArrowReader");
        } catch (RowRetrievalException e) {
            throw new RuntimeException("Exception calling getArrowReader on leafPartitionQueryExecutor", e);
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing arrowReader (processed {}" + numBatches + " columnar batches)");
        arrowReader.close();
        ffiContext.close();
        allocator.close();
    }

    @Override
    public boolean next() throws IOException {
        return arrowReader.loadNextBatch();
    }

    @Override
    public ColumnarBatch get() {
        try {
            VectorSchemaRoot batch = arrowReader.getVectorSchemaRoot();
            List<ColumnVector> columnVectors = new ArrayList<>();
            batch.getFieldVectors().forEach(arrowVector -> {
                ColumnVector sparkColVector = new ArrowColumnVector(arrowVector);
                columnVectors.add(sparkColVector);
            });
            ColumnarBatch sparkColumnarBatch = new ColumnarBatch(
                    columnVectors.toArray(new ColumnVector[0]),
                    batch.getRowCount());
            numBatches++;
            return sparkColumnarBatch;
        } catch (IOException e) {
            throw new RuntimeException("Exception calling get() to retrieve ColumnarBatch", e);
        }
    }
}
