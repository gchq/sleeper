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
package sleeper.bulkimport.runner.dataframe;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.runner.BulkImportJobDriver;
import sleeper.bulkimport.runner.BulkImportSparkContext;
import sleeper.bulkimport.runner.common.SparkFileReferenceRow;
import sleeper.bulkimport.runner.common.StructTypeFactory;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Runs a bulk import job using Spark's Dataframe API. Sorts and writes out the data split by Sleeper partition.
 */
public class BulkImportJobDataframeDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobDataframeDriver.class);
    private static final String PARTITION_FIELD_NAME = "__partition";

    private BulkImportJobDataframeDriver() {
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobDriver.start(args, BulkImportJobDataframeDriver::createFileReferences);
    }

    public static Dataset<Row> createFileReferences(BulkImportSparkContext input) {
        Schema schema = input.getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        StructType convertedSchema = new StructTypeFactory().getStructType(schema);
        StructType schemaWithPartitionField = createEnhancedSchema(convertedSchema);

        int numLeafPartitions = (int) input.getPartitionsBroadcast().value()
                .stream().filter(Partition::isLeafPartition).count();
        LOGGER.info("There are {} leaf partitions", numLeafPartitions);

        Dataset<Row> dataWithPartition = input.getRows().mapPartitions(
                new AddPartitionFunction(schemaAsString, input.getPartitionsBroadcast()),
                ExpressionEncoder.apply(schemaWithPartitionField));

        Column[] sortColumns = Lists.newArrayList(
                Lists.newArrayList(PARTITION_FIELD_NAME),
                schema.getRowKeyFieldNames(), schema.getSortKeyFieldNames())
                .stream()
                .flatMap(List::stream)
                .map(Column::new)
                .toArray(Column[]::new);
        LOGGER.info("Sorting by columns {}", Arrays.stream(sortColumns)
                .map(Column::toString).collect(Collectors.joining(",")));

        Dataset<Row> sortedRows = dataWithPartition.sort(sortColumns);

        return sortedRows.mapPartitions(
                new WriteParquetFiles(
                        input.getInstanceProperties().saveAsString(),
                        input.getTableProperties().saveAsString(),
                        input.getHadoopConf()),
                ExpressionEncoder.apply(SparkFileReferenceRow.createFileReferenceSchema()));
    }

    private static StructType createEnhancedSchema(StructType convertedSchema) {
        StructType structTypeWithPartition = new StructType(convertedSchema.fields());
        return structTypeWithPartition
                .add(new StructField(PARTITION_FIELD_NAME, DataTypes.StringType, false, null));
    }
}
