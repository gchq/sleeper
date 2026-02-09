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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.util.SerializableConfiguration;

import sleeper.bulkimport.runner.common.HadoopSketchesStore;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.Iterator;

import static sleeper.core.properties.PropertiesUtils.loadProperties;

/**
 * Writes sorted rows to a Parquet file. When it comes across a {@link sleeper.core.row.Row} belonging to a
 * different leaf partition (denoted by the "partitionId" column), the Parquet file is flushed to the file system along
 * with its accompanying sketches file.
 */
public class WriteParquetFiles implements MapPartitionsFunction<Row, Row> {
    private static final long serialVersionUID = 1873341639622053831L;

    private final String instancePropertiesStr;
    private final String tablePropertiesStr;
    private final SerializableConfiguration serializableConf;

    public WriteParquetFiles(String instancePropertiesStr, String tablePropertiesStr, Configuration conf) {
        this.instancePropertiesStr = instancePropertiesStr;
        this.tablePropertiesStr = tablePropertiesStr;
        this.serializableConf = new SerializableConfiguration(conf);
    }

    @Override
    public Iterator<Row> call(Iterator<Row> rowIter) {
        InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(loadProperties(instancePropertiesStr));
        TableProperties tableProperties = new TableProperties(instanceProperties, loadProperties(tablePropertiesStr));

        return new FileWritingIterator(rowIter, instanceProperties, tableProperties, serializableConf.value(), new HadoopSketchesStore(serializableConf.value()));
    }
}
