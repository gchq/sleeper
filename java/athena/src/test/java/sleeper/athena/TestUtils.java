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
package sleeper.athena;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.instance.ArrayListIngestProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.validation.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;

public class TestUtils {

    private TestUtils() {
    }

    public static InstanceProperties createInstance(AmazonS3 s3Client, AmazonDynamoDB dynamoDB, String dataDir) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DATA_BUCKET, dataDir);
        instanceProperties.set(FILE_SYSTEM, "file://"); // Overwrite S3 because we're going to use the standard fs.
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 1000L);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 1024L);
        instanceProperties.setNumber(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, 10);

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        instanceProperties.saveToS3(s3Client);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();

        return instanceProperties;
    }

    public static TableProperties createTable(
            InstanceProperties instance, Schema schema, AmazonDynamoDB dynamoDB, AmazonS3 s3Client,
            Configuration configuration, Object... splitPoints) {
        TableProperties tableProperties = createTestTableProperties(instance, schema);
        tableProperties.setEnum(TableProperty.INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF);
        S3TableProperties.getStore(instance, s3Client, dynamoDB).save(tableProperties);

        try {
            StateStore stateStore = new StateStoreFactory(instance, s3Client, dynamoDB, configuration).getStateStore(tableProperties);
            stateStore.initialise(new PartitionsFromSplitPoints(schema, List.of(splitPoints)).construct());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }

        return tableProperties;
    }

    public static void ingestData(
            AmazonS3 s3Client, AmazonDynamoDB dynamoClient, String dataDir,
            InstanceProperties instanceProperties, TableProperties table) {
        try {
            IngestFactory factory = IngestFactory.builder()
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(dataDir)
                    .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, new Configuration()))
                    .hadoopConfiguration(new Configuration())
                    .instanceProperties(instanceProperties)
                    .build();
            factory.ingestFromRecordIterator(table, generateTimeSeriesData().iterator());
        } catch (IOException | StateStoreException | IteratorCreationException e) {
            throw new RuntimeException("Failed to Ingest data", e);
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
