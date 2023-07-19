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

package sleeper.systemtest.suite.dsl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.ingest.IngestFactory;
import sleeper.query.QueryException;
import sleeper.query.executor.QueryExecutor;
import sleeper.query.model.Query;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.suite.fixtures.SystemTestInstance;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class SleeperSystemTest {

    private static final SleeperSystemTest INSTANCE = new SleeperSystemTest();

    private final SystemTestParameters parameters = SystemTestParameters.loadFromSystemProperties();
    private final CloudFormationClient cloudFormationClient = CloudFormationClient.create();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    private final SleeperInstanceContext instance = new SleeperInstanceContext(parameters, cloudFormationClient, s3Client);

    public static SleeperSystemTest getInstance() {
        return INSTANCE;
    }

    public void connectToInstance(SystemTestInstance testInstance) {
        instance.connectTo(testInstance.getIdentifier(), testInstance.getInstanceConfiguration());
    }

    public InstanceProperties instanceProperties() {
        return instance.getCurrentInstance().getInstanceProperties();
    }

    public TableProperties tableProperties() {
        return instance.getCurrentInstance().getTableProperties();
    }

    public void ingestData(Path tempDir, Stream<Record> recordStream) throws Exception {
        InstanceProperties instanceProperties = instanceProperties();
        IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(tempDir.toString())
                .stateStoreProvider(new StateStoreProvider(dynamoDB, instanceProperties))
                .instanceProperties(instanceProperties)
                .build().ingestFromRecordIterator(tableProperties(), recordStream.iterator());
    }

    public List<Record> allRecordsInTable() throws StateStoreException, QueryException {
        InstanceProperties instanceProperties = instanceProperties();
        TableProperties tableProperties = tableProperties();
        StateStore stateStore = new StateStoreProvider(dynamoDB, instanceProperties).getStateStore(tableProperties);
        QueryExecutor queryExecutor = new QueryExecutor(ObjectFactory.noUserJars(), tableProperties(),
                stateStore, new Configuration(), Executors.newSingleThreadExecutor());
        PartitionTree tree = new PartitionTree(tableProperties.getSchema(), stateStore.getAllPartitions());
        List<Record> records = new ArrayList<>();
        try (CloseableIterator<Record> recordIterator = queryExecutor.execute(
                new Query.Builder(tableProperties.get(TableProperty.TABLE_NAME), UUID.randomUUID().toString(),
                        List.of(tree.getRootPartition().getRegion())).build())) {
            recordIterator.forEachRemaining(records::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return records;
    }
}
