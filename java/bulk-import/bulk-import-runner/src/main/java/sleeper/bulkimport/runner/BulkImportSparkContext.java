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
package sleeper.bulkimport.runner;

import com.joom.spark.ExplicitRepartitionStrategy$;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import sleeper.bulkimport.runner.common.JdkImmutableListRegistrator;
import sleeper.bulkimport.runner.common.StructTypeFactory;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

/**
 * A combination of a Spark context, and the input data for a bulk import job. References the input files as a Spark
 * data set, and the Sleeper partition tree as a broadcast across the Spark cluster. This will be operated on by further
 * code to perform the import.
 * <p>
 * Note that the Spark context should be stopped at the end of the bulk import job. This class implements AutoCloseable
 * to make that easier.
 */
public class BulkImportSparkContext implements BulkImportContext<BulkImportSparkContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportSparkContext.class);

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final SparkContext sparkContext;
    private final JavaSparkContext javaSparkContext;
    private final Dataset<Row> rows;
    private final Broadcast<List<Partition>> partitionsBroadcast;

    private BulkImportSparkContext(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            SparkContext sparkContext, JavaSparkContext javaSparkContext,
            Dataset<Row> rows, Broadcast<List<Partition>> partitionsBroadcast) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.sparkContext = sparkContext;
        this.javaSparkContext = javaSparkContext;
        this.rows = rows;
        this.partitionsBroadcast = partitionsBroadcast;
    }

    public static BulkImportJobDriver.ContextCreator<BulkImportSparkContext> creator(InstanceProperties instanceProperties) {
        return (tableProperties, partitions, job) -> create(instanceProperties, tableProperties, partitions, job.getFiles());
    }

    /**
     * Creates a context for a bulk import job. Creates a Spark context and feeds the input data to Spark.
     *
     * @param  instanceProperties the instance properties
     * @param  tableProperties    the table properties
     * @param  partitions         all partitions in the Sleeper table partition tree
     * @param  filenames          the paths to the input Parquet files, excluding the file system
     * @return                    the context for the bulk import
     */
    public static BulkImportSparkContext create(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            List<Partition> partitions, List<String> filenames) {
        LOGGER.info("Initialising Spark");
        SparkSession session = new SparkSession.Builder().config(createSparkConf()).getOrCreate();
        Seq<SparkStrategy> strategies = JavaConverters.iterableAsScalaIterable(List.<SparkStrategy>of(ExplicitRepartitionStrategy$.MODULE$)).toSeq();
        session.experimental().extraStrategies_$eq(strategies);

        SparkContext sparkContext = session.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        LOGGER.info("Retrieved Spark context");

        Broadcast<List<Partition>> partitionsBroadcast = javaSparkContext.broadcast(partitions);
        Dataset<Row> rows = session.read()
                .schema(new StructTypeFactory().getStructType(tableProperties.getSchema()))
                .option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "true")
                .parquet(addFileSystem(instanceProperties, filenames).toArray(new String[0]));
        LOGGER.info("Prepared partitions broadcast and rows dataset");

        return new BulkImportSparkContext(
                instanceProperties, tableProperties,
                sparkContext, javaSparkContext,
                rows, partitionsBroadcast);
    }

    /**
     * Creates a Spark configuration suitable for a bulk import job. Configures serialisation for Sleeper partitions.
     *
     * @return the Spark configuration
     */
    public static SparkConf createSparkConf() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.set("spark.kryo.registrator", JdkImmutableListRegistrator.class.getName());
        sparkConf.registerKryoClasses(new Class[]{Partition.class});
        return sparkConf;
    }

    private static List<String> addFileSystem(InstanceProperties instanceProperties, List<String> filenames) {
        List<String> pathsWithFs = new ArrayList<>();
        String fs = instanceProperties.get(FILE_SYSTEM);
        LOGGER.info("Using file system {}", fs);
        filenames.forEach(file -> pathsWithFs.add(fs + file));
        LOGGER.info("Paths to be read are {}", pathsWithFs);
        return pathsWithFs;
    }

    @Override
    public BulkImportSparkContext withPartitions(List<Partition> partitions) {
        return new BulkImportSparkContext(
                instanceProperties, tableProperties,
                sparkContext, javaSparkContext,
                rows, javaSparkContext.broadcast(partitions));
    }

    @Override
    public void close() {
        stopSparkContext();
    }

    /**
     * Stops the Spark context. This is the same as calling {@link #close()}.
     */
    public void stopSparkContext() {
        sparkContext.stop();
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }

    public Schema getSchema() {
        return tableProperties.getSchema();
    }

    public Dataset<Row> getRows() {
        return rows;
    }

    public Broadcast<List<Partition>> getPartitionsBroadcast() {
        return partitionsBroadcast;
    }

    public Configuration getHadoopConf() {
        return sparkContext.hadoopConfiguration();
    }
}
