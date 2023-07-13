/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.bulkimport.job.runner.dataframe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.configuration.properties.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.CommonProperty.ID;
import static sleeper.configuration.properties.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.CommonProperty.REGION;
import static sleeper.configuration.properties.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

class WriteParquetFilesIT {

    @TempDir
    public java.nio.file.Path folder;

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
        instanceProperties.set(SUBNETS, "");
        instanceProperties.set(FILE_SYSTEM, fs);
        return instanceProperties;
    }

    @Test
    void shouldWriteParquetFiles() throws IOException {
        // Given
        String dir = createTempDirectory(folder, null).toString();
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
        Record expectedRecord1 = new Record();
        expectedRecord1.put("key", 1);
        expectedRecord1.put("sort", 2L);
        expectedRecord1.put("value", "3");
        Record expectedRecord2 = new Record();
        expectedRecord2.put("key", 4);
        expectedRecord2.put("sort", 5L);
        expectedRecord2.put("value", "6");
        assertThat(fileInfoIterator).toIterable()
                .extracting(
                        fileInfo -> fileInfo.getLong(2),
                        fileInfo -> readRecords(fileInfo.getString(1), schema))
                .containsExactly(
                        tuple(2L, Arrays.asList(expectedRecord1, expectedRecord2)));
    }

    @Test
    void shouldWriteToMultipleParquetFilesWhenDataContainsMoreThanOnePartition() throws IOException {
        // Given
        String dir = createTempDirectory(folder, null).toString();
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
        Record expectedRecord1 = new Record();
        expectedRecord1.put("key", 1);
        expectedRecord1.put("sort", 2L);
        expectedRecord1.put("value", "3");
        Record expectedRecord2 = new Record();
        expectedRecord2.put("key", 4);
        expectedRecord2.put("sort", 5L);
        expectedRecord2.put("value", "6");
        assertThat(fileInfoIterator).toIterable()
                .extracting(
                        fileInfo -> fileInfo.getString(0),
                        fileInfo -> fileInfo.getLong(2),
                        fileInfo -> readRecords(fileInfo.getString(1), schema))
                .containsExactly(
                        tuple("a", 1L, Collections.singletonList(expectedRecord1)),
                        tuple("b", 1L, Collections.singletonList(expectedRecord2)));
    }

    private Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }

    private List<Record> readRecords(String filename, Schema schema) {
        try (ParquetReader<Record> reader = new ParquetRecordReader(new Path(filename), schema)) {
            List<Record> records = new ArrayList<>();
            for (Record record = reader.read(); record != null; record = reader.read()) {
                records.add(new Record(record));
            }
            return records;
        } catch (IOException e) {
            throw new RuntimeException("Failed reading records", e);
        }
    }
}
