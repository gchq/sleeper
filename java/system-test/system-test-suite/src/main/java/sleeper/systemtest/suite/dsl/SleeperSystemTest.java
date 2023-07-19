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
import sleeper.query.QueryException;
import sleeper.query.executor.QueryExecutor;
import sleeper.query.model.Query;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.drivers.ingest.IngestContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.suite.fixtures.SystemTestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SleeperSystemTest {

    private static final SleeperSystemTest INSTANCE = new SleeperSystemTest();

    private final SystemTestParameters parameters = SystemTestParameters.loadFromSystemProperties();
    private final CloudFormationClient cloudFormationClient = CloudFormationClient.create();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
    private final SleeperInstanceContext instance = new SleeperInstanceContext(
            parameters, cloudFormationClient, s3Client, dynamoDB);
    private final IngestContext ingest = new IngestContext(instance);

    public static SleeperSystemTest getInstance() {
        return INSTANCE;
    }

    public void connectToInstance(SystemTestInstance testInstance) {
        instance.connectTo(testInstance.getIdentifier(), testInstance.getInstanceConfiguration());
        instance.reinitialise();
    }

    public InstanceProperties instanceProperties() {
        return instance.getInstanceProperties();
    }

    public TableProperties tableProperties() {
        return instance.getTableProperties();
    }

    public void ingestRecords(Path tempDir, Record... records) throws Exception {
        ingestRecords(tempDir, Stream.of(records));
    }

    public void ingestRecords(Path tempDir, Stream<Record> recordStream) throws Exception {
        ingest.factory(tempDir).ingestFromRecordIterator(tableProperties(), recordStream.iterator());
    }

    public List<Record> allRecordsInTable() throws StateStoreException, QueryException {
        InstanceProperties instanceProperties = instanceProperties();
        TableProperties tableProperties = tableProperties();
        StateStore stateStore = new StateStoreProvider(dynamoDB, instanceProperties).getStateStore(tableProperties);
        PartitionTree tree = new PartitionTree(tableProperties.getSchema(), stateStore.getAllPartitions());
        QueryExecutor queryExecutor = new QueryExecutor(ObjectFactory.noUserJars(), tableProperties(),
                stateStore, new Configuration(), Executors.newSingleThreadExecutor());
        queryExecutor.init(tree.getAllPartitions(), stateStore.getPartitionToActiveFilesMap());
        try (CloseableIterator<Record> recordIterator = queryExecutor.execute(
                new Query.Builder(tableProperties.get(TableProperty.TABLE_NAME), UUID.randomUUID().toString(),
                        List.of(tree.getRootPartition().getRegion())).build())) {
            return stream(recordIterator).collect(Collectors.toUnmodifiableList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }
}
