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

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

public class BulkImportContext implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportContext.class);

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final SparkContext sparkContext;
    private final Dataset<Row> rows;
    private final Broadcast<List<Partition>> broadcastedPartitions;

    private BulkImportContext(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableProperties = builder.tableProperties;
        sparkContext = builder.sparkContext;
        rows = builder.rows;
        broadcastedPartitions = builder.broadcastedPartitions;
    }

    public static BulkImportContext create(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            List<Partition> partitions, List<String> filenames) {
        LOGGER.info("Initialising Spark");
        SparkSession session = new SparkSession.Builder().config(createSparkConf()).getOrCreate();
        Seq<SparkStrategy> strategies = JavaConverters.iterableAsScalaIterable(List.<SparkStrategy>of(ExplicitRepartitionStrategy$.MODULE$)).toSeq();
        session.experimental().extraStrategies_$eq(strategies);

        SparkContext sparkContext = session.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        LOGGER.info("Retrieved Spark context");

        Broadcast<List<Partition>> broadcastedPartitions = javaSparkContext.broadcast(partitions);
        Dataset<Row> rows = session.read()
                .schema(new StructTypeFactory().getStructType(tableProperties.getSchema()))
                .option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "true")
                .parquet(addFileSystem(instanceProperties, filenames).toArray(new String[0]));
        LOGGER.info("Prepared partitions broadcast and rows dataset");

        return builder()
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties)
                .sparkContext(sparkContext)
                .broadcastedPartitions(broadcastedPartitions)
                .rows(rows)
                .build();
    }

    public static SparkConf createSparkConf() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.set("spark.kryo.registrator", JdkImmutableListRegistrator.class.getName());
        sparkConf.registerKryoClasses(new Class[]{Partition.class});
        return sparkConf;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static List<String> addFileSystem(InstanceProperties instanceProperties, List<String> filenames) {
        List<String> pathsWithFs = new ArrayList<>();
        String fs = instanceProperties.get(FILE_SYSTEM);
        LOGGER.info("Using file system {}", fs);
        filenames.forEach(file -> pathsWithFs.add(fs + file));
        LOGGER.info("Paths to be read are {}", pathsWithFs);
        return pathsWithFs;
    }

    public InstanceProperties instanceProperties() {
        return instanceProperties;
    }

    public TableProperties tableProperties() {
        return tableProperties;
    }

    public Schema schema() {
        return tableProperties.getSchema();
    }

    public Dataset<Row> rows() {
        return rows;
    }

    public Broadcast<List<Partition>> broadcastedPartitions() {
        return broadcastedPartitions;
    }

    public Configuration conf() {
        return sparkContext.hadoopConfiguration();
    }

    public void stopSparkContext() {
        sparkContext.stop();
    }

    @Override
    public void close() {
        stopSparkContext();
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties;
        private SparkContext sparkContext;
        private Dataset<Row> rows;
        private Broadcast<List<Partition>> broadcastedPartitions;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder sparkContext(SparkContext sparkContext) {
            this.sparkContext = sparkContext;
            return this;
        }

        public Builder rows(Dataset<Row> rows) {
            this.rows = rows;
            return this;
        }

        public Builder broadcastedPartitions(Broadcast<List<Partition>> broadcastedPartitions) {
            this.broadcastedPartitions = broadcastedPartitions;
            return this;
        }

        public BulkImportContext build() {
            return new BulkImportContext(this);
        }
    }
}
