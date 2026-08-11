/*
 * Copyright 2022-2026 Crown Copyright
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
import java.util.ArrayList;
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
    static final List<String> KEY1_VALUES = List.of("D", "F", "G", "U");

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
            factory.ingestFromRowIterator(table, generateData().iterator());
        } catch (IOException | IteratorCreationException e) {
            throw new RuntimeException("Failed to Ingest data", e);
        }
    }

    // Generates a regular grid of rows over a string row key and two integer row keys. The derived fields are
    // recomputable from the integer keys so the assertions can calculate the expected values.
    private static List<Row> generateData() {
        List<Row> rows = new ArrayList<>();
        for (String key1 : KEY1_VALUES) {
            for (int key2 = 1; key2 <= 12; key2++) {
                for (int key3 = 1; key3 <= 28; key3++) {
                    Row row = new Row();
                    row.put("key1", key1);
                    row.put("key2", key2);
                    row.put("key3", key3);
                    row.put("timestamp", (long) key2 * 100 + key3);
                    row.put("count", (long) key2 * key3);
                    HashMap<String, String> map = new HashMap<>();
                    map.put("mapKey", "mapValue");
                    row.put("map", map);
                    row.put("list", Lists.newArrayList("listValue"));
                    row.put("str", key1 + "-" + key2 + "-" + key3);
                    rows.add(row);
                }
            }
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
