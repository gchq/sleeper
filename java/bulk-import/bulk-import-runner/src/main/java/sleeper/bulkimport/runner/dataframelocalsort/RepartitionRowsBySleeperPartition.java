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
package sleeper.bulkimport.runner.dataframelocalsort;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.runner.BulkImportSparkContext;
import sleeper.bulkimport.runner.common.StructTypeFactory;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

/**
 * Repartitions data to establish a one to one equivalence between Spark partition and Sleeper partition. This can
 * ensure that all the data for each Sleeper partition will be gathered together on a single node of the Spark cluster.
 * Note that this requires enough partitions in the Sleeper table for the data to be spread across the Spark cluster.
 */
public class RepartitionRowsBySleeperPartition {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepartitionRowsBySleeperPartition.class);
    private static final String PARTITION_FIELD_NAME = "__partition";

    private RepartitionRowsBySleeperPartition() {
    }

    /**
     * Repartitions the input data for a bulk import job for a one to one equivalence between Spark partition and
     * Sleeper partition.
     *
     * @param  input the context of the bulk import job
     * @return       the repartitioned data set
     */
    public static Dataset<Row> repartition(BulkImportSparkContext input) {
        Schema schema = input.getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        StructType convertedSchema = new StructTypeFactory().getStructType(schema);
        StructType schemaWithPartitionField = createEnhancedSchema(convertedSchema);

        int numLeafPartitions = (int) input.getPartitionsBroadcast().value()
                .stream().filter(Partition::isLeafPartition).count();
        LOGGER.info("There are {} leaf partitions", numLeafPartitions);

        Dataset<Row> dataWithPartition = input.getRows().mapPartitions(
                new AddPartitionAsIntFunction(schemaAsString, input.getPartitionsBroadcast()),
                ExpressionEncoder.apply(schemaWithPartitionField));
        LOGGER.info("After adding partition id as int, there are {} partitions", dataWithPartition.rdd().getNumPartitions());

        return new com.joom.spark.package$implicits$ExplicitRepartitionWrapper(dataWithPartition)
                .explicitRepartition(numLeafPartitions, new Column(PARTITION_FIELD_NAME));
    }

    private static StructType createEnhancedSchema(StructType convertedSchema) {
        StructType structTypeWithPartition = new StructType(convertedSchema.fields());
        return structTypeWithPartition
                .add(new StructField(PARTITION_FIELD_NAME, DataTypes.IntegerType, false, null));
    }

}
