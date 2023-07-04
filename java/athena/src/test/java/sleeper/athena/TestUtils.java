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
package sleeper.athena;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.IngestFactory;
import sleeper.statestore.InitialiseStateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TestUtils {

    private TestUtils() {
    }

    public static InstanceProperties createInstance(AmazonS3 s3Client) {
        String configBucket = s3Client.createBucket(UUID.randomUUID().toString()).getName();
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(VERSION, "1");
        instanceProperties.set(CONFIG_BUCKET, configBucket);
        instanceProperties.set(FILE_SYSTEM, "file://"); // Overwrite S3 because we're going to use the standard fs.
        instanceProperties.set(JARS_BUCKET, "unused");
        instanceProperties.set(ACCOUNT, "unused");
        instanceProperties.set(REGION, "unused");
        instanceProperties.set(VPC_ID, "unused");
        instanceProperties.set(SUBNETS, "unused");
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 1000L);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 1024L);
        instanceProperties.setNumber(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, 10);

        try {
            instanceProperties.saveToS3(s3Client);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        s3Client.shutdown();

        return instanceProperties;
    }

    public static TableProperties createTable(InstanceProperties instance, Schema schema, String dataDir, AmazonDynamoDB dynamoDB, AmazonS3 s3Client, Object... splitPoints) throws IOException {
        TableProperties tableProperties = new TableProperties(instance);
        tableProperties.setSchema(schema);
        String tableName = UUID.randomUUID().toString();
        tableProperties.set(TABLE_NAME, tableName);

        // Create a place for data to go
        tableProperties.set(DATA_BUCKET, dataDir);

        // Create a state store
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, tableName + "-af");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, tableName + "-rfgcf");

        try {
            DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(tableName, schema, dynamoDB).create();
            InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, stateStore, Lists.newArrayList(splitPoints)).run();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        } finally {
            dynamoDB.shutdown();
        }

        tableProperties.saveToS3(s3Client);
        s3Client.shutdown();
        return tableProperties;
    }

    public static void ingestData(AmazonDynamoDB dynamoClient, AmazonS3 s3Client, String dataDir, InstanceProperties instanceProperties,
                                  TableProperties table) {
        try {
            IngestFactory factory = IngestFactory.builder()
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(dataDir)
                    .stateStoreProvider(new StateStoreProvider(dynamoClient, instanceProperties))
                    .hadoopConfiguration(new Configuration())
                    .instanceProperties(instanceProperties)
                    .build();
            factory.ingestFromRecordIterator(table, generateTimeSeriesData().iterator());
        } catch (IOException | StateStoreException | IteratorException e) {
            throw new RuntimeException("Failed to Ingest data", e);
        } finally {
            dynamoClient.shutdown();
            s3Client.shutdown();
        }
    }

    private static List<Record> generateTimeSeriesData() {
        LocalDate startDate = LocalDate.of(2017, 1, 1);
        LocalDate endDate = LocalDate.of(2021, 1, 1);
        List<Record> records = new ArrayList<>();
        for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
            Record record = new Record();
            record.put("year", date.getYear());
            record.put("month", date.getMonthValue());
            record.put("day", date.getDayOfMonth());
            record.put("timestamp", Date.from(Timestamp.valueOf(date.atStartOfDay()).toInstant()).getTime());
            record.put("count", (long) date.getYear() * (long) date.getMonthValue() * (long) date.getDayOfMonth());
            HashMap<String, String> map = new HashMap<>();
            map.put(date.getMonth().name(), date.getMonth().name());
            record.put("map", map);
            record.put("list", Lists.newArrayList(date.getEra().toString()));
            record.put("str", date.toString());
            records.add(record);
        }

        return records;
    }

    public static FederatedIdentity createIdentity() {
        return new FederatedIdentity("arn", "account", new HashMap<>(), new ArrayList<>());
    }
}
