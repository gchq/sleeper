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
package sleeper.bulkimport.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;

import java.util.List;

public class BulkImportJobInput {

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final Dataset<Row> rows;
    private final Broadcast<List<Partition>> broadcastedPartitions;
    private final Configuration conf;

    private BulkImportJobInput(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableProperties = builder.tableProperties;
        rows = builder.rows;
        broadcastedPartitions = builder.broadcastedPartitions;
        conf = builder.conf;
    }

    public static Builder builder() {
        return new Builder();
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
        return conf;
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties;
        private Dataset<Row> rows;
        private Broadcast<List<Partition>> broadcastedPartitions;
        private Configuration conf;

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

        public Builder rows(Dataset<Row> rows) {
            this.rows = rows;
            return this;
        }

        public Builder broadcastedPartitions(Broadcast<List<Partition>> broadcastedPartitions) {
            this.broadcastedPartitions = broadcastedPartitions;
            return this;
        }

        public Builder conf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public BulkImportJobInput build() {
            return new BulkImportJobInput(this);
        }
    }
}
