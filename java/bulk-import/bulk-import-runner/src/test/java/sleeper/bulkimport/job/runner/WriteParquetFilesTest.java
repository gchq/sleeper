/*
 * Copyright 2022 Crown Copyright
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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TABLE_PROPERTIES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class WriteParquetFilesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    public InstanceProperties createInstanceProperties(String fs) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(FILE_SYSTEM, "");
        instanceProperties.set(JARS_BUCKET, "");
        instanceProperties.set(ACCOUNT, "");
        instanceProperties.set(REGION, "");
        instanceProperties.set(VERSION, "");
        instanceProperties.set(VPC_ID, "");
        instanceProperties.set(SUBNET, "");
        instanceProperties.set(TABLE_PROPERTIES, "");
        instanceProperties.set(FILE_SYSTEM, fs);
        return instanceProperties;
    }

    @Test
    public void shouldWriteParquetFiles() throws IOException {
        // Given
        String dir = folder.newFolder().getAbsolutePath();
        String dataBucket = "dataBucket";
        Schema schema = getSchema();
        InstanceProperties instanceProperties = createInstanceProperties(dir);
        String instancePropertiesString = instanceProperties.saveAsString();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, UUID.randomUUID().toString());
        tableProperties.set(DATA_BUCKET, dataBucket);
        tableProperties.setSchema(schema);
        String tablePropertiesString = tableProperties.saveAsString();
        WriteParquetFiles writeParquetFiles = new WriteParquetFiles(instancePropertiesString, tablePropertiesString, new Configuration());
        Row row1 = RowFactory.create(1, 2L, "3", "root");
        Row row2 = RowFactory.create(4, 5L, "6", "root");
        Iterator<Row> rows = Arrays.asList(row1, row2).iterator();

        // When
        Iterator<Row> fileInfoIterator = writeParquetFiles.call(rows);

        // Then

        Row fileInfo = fileInfoIterator.next();

        String filename = fileInfo.getString(1);
        assertThat(fileInfo.getLong(2)).isEqualTo(2L);
        assertThat(fileInfoIterator.hasNext()).isFalse();

        ParquetReader<Record> reader = new ParquetRecordReader(new org.apache.hadoop.fs.Path(filename), schema);
        Record record1 = reader.read();
        Record expectedRecord1 = new Record();
        expectedRecord1.put("key", 1);
        expectedRecord1.put("sort", 2L);
        expectedRecord1.put("value", "3");
        assertThat(record1).isEqualTo(expectedRecord1);
        Record record2 = reader.read();
        Record expectedRecord2 = new Record();
        expectedRecord2.put("key", 4);
        expectedRecord2.put("sort", 5L);
        expectedRecord2.put("value", "6");
        assertThat(record2).isEqualTo(expectedRecord2);
        assertThat(reader.read()).isNull();
        reader.close();
    }

    @Test
    public void ShouldWriteToMultipleParquetFilesWhenDataContainsMoreThanOnePartition() throws IOException {
        // Given
        String dir = folder.newFolder().getAbsolutePath();
        String dataBucket = "dataBucket";
        Schema schema = getSchema();
        InstanceProperties instanceProperties = createInstanceProperties(dir);
        String instancePropertiesString = instanceProperties.saveAsString();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, UUID.randomUUID().toString());
        tableProperties.set(DATA_BUCKET, dataBucket);
        tableProperties.setSchema(schema);
        String tablePropertiesString = tableProperties.saveAsString();
        WriteParquetFiles writeParquetFiles = new WriteParquetFiles(instancePropertiesString, tablePropertiesString, new Configuration());
        Row row1 = RowFactory.create(1, 2L, "3", "a");
        Row row2 = RowFactory.create(4, 5L, "6", "b");
        Iterator<Row> rows = Arrays.asList(row1, row2).iterator();

        // When
        Iterator<Row> fileInfoIterator = writeParquetFiles.call(rows);

        // Then

        // For 
        Row fileInfo = fileInfoIterator.next();

        assertThat(fileInfo.getString(0)).isEqualTo("a");
        String filename = fileInfo.getString(1);
        assertThat(fileInfo.getLong(2)).isEqualTo(1L);

        ParquetReader<Record> reader = new ParquetRecordReader(new org.apache.hadoop.fs.Path(filename), schema);
        Record record1 = reader.read();
        Record expectedRecord1 = new Record();
        expectedRecord1.put("key", 1);
        expectedRecord1.put("sort", 2L);
        expectedRecord1.put("value", "3");
        assertThat(record1).isEqualTo(expectedRecord1);
        assertThat(reader.read()).isNull();
        reader.close();

        // For B
        fileInfo = fileInfoIterator.next();

        assertThat(fileInfo.getString(0)).isEqualTo("b");
        filename = fileInfo.getString(1);
        assertThat(fileInfo.getLong(2)).isEqualTo(1L);

        reader = new ParquetRecordReader(new org.apache.hadoop.fs.Path(filename), schema);
        Record record2 = reader.read();
        Record expectedRecord2 = new Record();
        expectedRecord2.put("key", 4);
        expectedRecord2.put("sort", 5L);
        expectedRecord2.put("value", "6");
        assertThat(record2).isEqualTo(expectedRecord2);
        assertThat(reader.read()).isNull();
        reader.close();
        assertThat(fileInfoIterator.hasNext()).isFalse();

    }

    private Schema getSchema() {
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new IntType()));
        schema.setSortKeyFields(new Field("sort", new LongType()));
        schema.setValueFields(
                new Field("value", new StringType()));
        return schema;
    }
}
