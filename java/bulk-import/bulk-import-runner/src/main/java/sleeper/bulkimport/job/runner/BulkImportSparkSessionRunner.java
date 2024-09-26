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

package sleeper.bulkimport.job.runner;

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
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

public class BulkImportSparkSessionRunner implements BulkImportJobDriver.SessionRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportSparkSessionRunner.class);

    private final BulkImportJobRunner jobRunner;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;

    public BulkImportSparkSessionRunner(
            BulkImportJobRunner jobRunner, InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider) {
        this.jobRunner = jobRunner;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
    }

    @Override
    public BulkImportJobOutput run(BulkImportJob job) throws IOException {
        // Initialise Spark
        LOGGER.info("Initialising Spark");
        SparkSession session = new SparkSession.Builder().config(createSparkConf()).getOrCreate();
        scala.collection.immutable.List<SparkStrategy> strategies = JavaConverters
                .collectionAsScalaIterable(Collections.singletonList((org.apache.spark.sql.execution.SparkStrategy) ExplicitRepartitionStrategy$.MODULE$)).toList();
        session.experimental().extraStrategies_$eq(strategies);
        SparkContext sparkContext = session.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        LOGGER.info("Spark initialised");

        // Load table information
        LOGGER.info("Loading table properties and schema for table {}", job.getTableName());
        TableProperties tableProperties = tablePropertiesProvider.getByName(job.getTableName());
        Schema schema = tableProperties.getSchema();
        StructType convertedSchema = new StructTypeFactory().getStructType(schema);

        // Load statestore and partitions
        LOGGER.info("Loading statestore and partitions");
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        List<Partition> allPartitions;
        try {
            allPartitions = stateStore.getAllPartitions();
        } catch (StateStoreException e) {
            LOGGER.error("Could not load partitions", e);
            throw new RuntimeException("Failed to load statestore. Are permissions correct for this service account?", e);
        }

        Configuration conf = sparkContext.hadoopConfiguration();
        Broadcast<List<Partition>> broadcastedPartitions = javaSparkContext.broadcast(allPartitions);
        LOGGER.info("Starting data processing");

        // Create paths to be read
        List<String> pathsWithFs = new ArrayList<>();
        String fs = instanceProperties.get(FILE_SYSTEM);
        LOGGER.info("Using file system {}", fs);
        job.getFiles().forEach(file -> pathsWithFs.add(fs + file));
        LOGGER.info("Paths to be read are {}", pathsWithFs);

        // Run bulk import
        Dataset<Row> dataWithPartition = session.read()
                .schema(convertedSchema)
                .option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "true")
                .parquet(pathsWithFs.toArray(new String[0]));

        LOGGER.info("Running bulk import job with id {}", job.getId());
        List<FileReference> fileReferences = jobRunner.createFileReferences(
                BulkImportJobInput.builder().rows(dataWithPartition)
                        .instanceProperties(instanceProperties).tableProperties(tableProperties)
                        .broadcastedPartitions(broadcastedPartitions).conf(conf).build())
                .collectAsList().stream()
                .map(SparkFileReferenceRow::createFileReference)
                .collect(Collectors.toList());

        return new BulkImportJobOutput(fileReferences, sparkContext::stop);
    }

    public static SparkConf createSparkConf() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.set("spark.kryo.registrator", JdkImmutableListRegistrator.class.getName());
        sparkConf.registerKryoClasses(new Class[]{Partition.class});
        return sparkConf;
    }
}
