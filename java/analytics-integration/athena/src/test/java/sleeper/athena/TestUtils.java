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
package sleeper.athena;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_ROWS_TO_WRITE_LOCALLY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class TestUtils {

    private TestUtils() {
    }

    public static InstanceProperties createInstance(S3Client s3Client, DynamoDbClient dynamoDB, String dataDir) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DATA_BUCKET, dataDir);
        instanceProperties.set(FILE_SYSTEM, "file://"); // Overwrite S3 because we're going to use the standard fs.
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.setNumber(MAX_ROWS_TO_WRITE_LOCALLY, 1000L);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 1024L);
        instanceProperties.setNumber(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, 10);

        s3Client.createBucket(request -> request.bucket(instanceProperties.get(CONFIG_BUCKET)));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();

        return instanceProperties;
    }

    public static TableProperties createTable(
            InstanceProperties instance, Schema schema, S3Client s3Client, DynamoDbClient dynamoClient, Object... splitPoints) {
        TableProperties tableProperties = createTestTableProperties(instance, schema);
        tableProperties.setEnum(TableProperty.INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF);
        S3TableProperties.createStore(instance, s3Client, dynamoClient).save(tableProperties);

        StateStore stateStore = new StateStoreFactory(instance, s3Client, dynamoClient).getStateStore(tableProperties);
        update(stateStore).initialise(new PartitionsFromSplitPoints(schema, List.of(splitPoints)).construct());

        return tableProperties;
    }

    public static void ingestData(
            S3Client s3Client, DynamoDbClient dynamoClient, String dataDir,
            InstanceProperties instanceProperties, TableProperties table) {
        try {
            IngestFactory factory = IngestFactory.builder()
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(dataDir)
                    .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                    .hadoopConfiguration(new Configuration())
                    .instanceProperties(instanceProperties)
                    .build();
            factory.ingestFromRowIterator(table, generateTimeSeriesData().iterator());
        } catch (IOException | IteratorCreationException e) {
            throw new RuntimeException("Failed to Ingest data", e);
        }
    }

    private static List<Row> generateTimeSeriesData() {
        LocalDate startDate = LocalDate.of(2017, 1, 1);
        LocalDate endDate = LocalDate.of(2021, 1, 1);
        List<Row> rows = new ArrayList<>();
        for (LocalDate date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
            Row row = new Row();
            row.put("year", date.getYear());
            row.put("month", date.getMonthValue());
            row.put("day", date.getDayOfMonth());
            row.put("timestamp", Date.from(Timestamp.valueOf(date.atStartOfDay()).toInstant()).getTime());
            row.put("count", (long) date.getYear() * (long) date.getMonthValue() * (long) date.getDayOfMonth());
            HashMap<String, String> map = new HashMap<>();
            map.put(date.getMonth().name(), date.getMonth().name());
            row.put("map", map);
            row.put("list", Lists.newArrayList(date.getEra().toString()));
            row.put("str", date.toString());
            rows.add(row);
        }

        return rows;
    }

    public static FederatedIdentity createIdentity() {
        return new FederatedIdentity("arn", "account", new HashMap<>(), new ArrayList<>());
    }

    public static Constraints createConstraints(Map<String, ValueSet> predicate) {
        return new Constraints(predicate, new ArrayList<>(), new ArrayList<>(), Constraints.DEFAULT_NO_LIMIT, new HashMap<>());
    }

}
