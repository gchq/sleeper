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
package sleeper.query.executor;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.impl.AgeOffIterator;
import sleeper.core.iterator.impl.SecurityFilteringIterator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.query.QueryException;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;

public class QueryExecutorIT {
    protected static final int DYNAMO_PORT = 8000;
    protected static AmazonDynamoDB dynamoDBClient;
    protected static ExecutorService executorService;

    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void initDynamoClient() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterClass
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
        executorService.shutdown();
    }

    @Test
    public void shouldReturnNothingWhenThereAreNoFiles() throws Exception {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema);
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        assertThat(results.hasNext()).isFalse();

        // When 2
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        assertThat(results.hasNext()).isFalse();

        // When 3
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 3
        assertThat(leafPartitionQueries).isEmpty();
    }

    @Test
    public void shouldReturnCorrectDataWhenOneRecordInOneFileInOnePartition() throws Exception {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema);
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        ingestData(instanceProperties, stateStore, schema, getRecords().iterator());
        List<String> files = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(1);
        assertThat(resultsAsList.get(0)).isEqualTo(getRecords().get(0));

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        assertThat(results.hasNext()).isFalse();

        // When 3
        region = new Region(rangeFactory.createRange(field, -10L, true, 1L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(1);
        assertThat(resultsAsList.get(0)).isEqualTo(getRecords().get(0));

        // When 4
        region = new Region(rangeFactory.createRange(field, 10L, true, 100L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 4
        assertThat(results.hasNext()).isFalse();

        // When 5
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 5
        assertThat(leafPartitionQueries).hasSize(1);
        LeafPartitionQuery expectedLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartitionQueries.get(0).getSubQueryId(), region, "root", rootPartition.getRegion(), files)
                .build();
        assertThat(leafPartitionQueries).containsExactly(expectedLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenMultipleIdenticalRecordsInOneFileInOnePartition() throws Exception {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema);
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        ingestData(instanceProperties, stateStore, schema, getMultipleIdenticalRecords().iterator());
        List<String> files = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getRecords().get(0));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        assertThat(results.hasNext()).isFalse();

        // When 3
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getRecords().get(0));
        }

        // When 4
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 4
        assertThat(leafPartitionQueries).hasSize(1);
        LeafPartitionQuery expectedLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartitionQueries.get(0).getSubQueryId(), region, "root", rootPartition.getRegion(), files)
                .build();
        assertThat(leafPartitionQueries).containsExactly(expectedLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenIdenticalRecordsInMultipleFilesInOnePartition()
            throws StateStoreException, InterruptedException,
            IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema);
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, schema, getRecords().iterator());
        }
        List<String> files = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getRecords().get(0));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        assertThat(results.hasNext()).isFalse();

        // When 3
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getRecords().get(0));
        }

        // When 4
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 4
        assertThat(leafPartitionQueries).hasSize(1);
        LeafPartitionQuery expectedLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartitionQueries.get(0).getSubQueryId(), region, "root", rootPartition.getRegion(), files)
                .build();
        assertThat(leafPartitionQueries).containsExactly(expectedLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenRecordsInMultipleFilesInOnePartition()
            throws StateStoreException, InterruptedException,
            IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema);
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, schema, getMultipleRecords().iterator());
        }
        List<String> files = stateStore.getActiveFiles().stream()
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getMultipleRecords().get(0));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 5L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getMultipleRecords().get(4));
        }

        // When 3
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        assertThat(results.hasNext()).isFalse();

        // When 4
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 4
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(100);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecords()));

        // When 5
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 5
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(90);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecords().stream()
                .filter(r -> ((long) r.get("key")) >= 1L && ((long) r.get("key")) < 10L).collect(Collectors.toList())));

        // When 6
        region = new Region(rangeFactory.createRange(field, 1L, false, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 6
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(80);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecords().stream()
                .filter(r -> ((long) r.get("key")) > 1L && ((long) r.get("key")) < 10L).collect(Collectors.toList())));

        // When 7
        region = new Region(rangeFactory.createRange(field, 1L, false, 10L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 7
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(90);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecords().stream()
                .filter(r -> ((long) r.get("key")) > 1L && ((long) r.get("key")) <= 10L).collect(Collectors.toList())));

        // When 8
        region = new Region(rangeFactory.createRange(field, -100000L, true, 123456789L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 8
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(100);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecords()));

        // When 9
        region = new Region(rangeFactory.createRange(field, 5L, true, 123456789L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 9
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(60);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecords().stream().filter(r -> ((long) r.get("key")) >= 5L).collect(Collectors.toList())));

        // When 10
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 10
        assertThat(leafPartitionQueries).hasSize(1);
        LeafPartitionQuery expectedLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartitionQueries.get(0).getSubQueryId(), region, "root", rootPartition.getRegion(), files)
                .build();
        assertThat(leafPartitionQueries).containsExactly(expectedLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenRecordsInMultipleFilesInMultiplePartitions()
            throws StateStoreException, InterruptedException,
            IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema, Collections.singletonList(5L));
        Partition leftPartition = stateStore.getLeafPartitions().stream()
                .filter(p -> ((long) p.getRegion().getRange("key").getMin() == Long.MIN_VALUE))
                .findFirst()
                .get();
        Partition rightPartition = stateStore.getLeafPartitions().stream()
                .filter(p -> ((long) p.getRegion().getRange("key").getMin() == 5L))
                .findFirst()
                .get();
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, schema, getMultipleRecords().iterator());
        }
        List<String> filesInLeftPartition = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(leftPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInRightPartition = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(rightPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getMultipleRecords().get(0));
        }

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 5L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getMultipleRecords().get(4));
        }

        // When 3
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        assertThat(results.hasNext()).isFalse();

        // When 4
        region = new Region(rangeFactory.createRange(field, -100000L, true, 123456789L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 4
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(100);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecords()));

        // When 5
        region = new Region(rangeFactory.createRange(field, 5L, true, 123456789L, true));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 5
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(60);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecords().stream().filter(r -> ((long) r.get("key")) >= 5L).collect(Collectors.toList())));

        // When 6
        region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 6
        assertThat(leafPartitionQueries).hasSize(2);
        LeafPartitionQuery leftLeafPartitionQuery;
        LeafPartitionQuery rightLeafPartitionQuery;
        if (leafPartitionQueries.get(0).getLeafPartitionId().equals(leftPartition.getId())) {
            leftLeafPartitionQuery = leafPartitionQueries.get(0);
            rightLeafPartitionQuery = leafPartitionQueries.get(1);
        } else {
            leftLeafPartitionQuery = leafPartitionQueries.get(1);
            rightLeafPartitionQuery = leafPartitionQueries.get(0);
        }
        LeafPartitionQuery expectedLeftLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leftLeafPartitionQuery.getSubQueryId(), region, leftPartition.getId(), leftPartition.getRegion(), filesInLeftPartition)
                .build();
        assertThat(leftLeafPartitionQuery).isEqualTo(expectedLeftLeafPartitionQuery);
        LeafPartitionQuery expectedRightLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", rightLeafPartitionQuery.getSubQueryId(), region, rightPartition.getId(), rightPartition.getRegion(), filesInRightPartition)
                .build();
        assertThat(rightLeafPartitionQuery).isEqualTo(expectedRightLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWithMultidimRowKey()
            throws StateStoreException, InterruptedException,
            IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = new Schema();
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        schema.setRowKeyFields(field1, field2);
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema, Collections.singletonList(5L));
        Partition leftPartition = stateStore.getLeafPartitions().stream()
                .filter(p -> ((long) p.getRegion().getRange("key1").getMin() == Long.MIN_VALUE))
                .findFirst()
                .get();
        Partition rightPartition = stateStore.getLeafPartitions().stream()
                .filter(p -> ((long) p.getRegion().getRange("key1").getMin() == 5L))
                .findFirst()
                .get();
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, schema, getMultipleRecordsMultidimRowKey().iterator());
        }
        List<String> filesInLeftPartition = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(leftPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInRightPartition = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(rightPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Range range1 = rangeFactory.createExactRange(field1, 1L);
        Range range2 = rangeFactory.createExactRange(field2, "1");
        Region region = new Region(Arrays.asList(range1, range2));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getMultipleRecordsMultidimRowKey().get(0));
        }

        // When 2
        range1 = rangeFactory.createExactRange(field1, 5L);
        range2 = rangeFactory.createExactRange(field2, "5");
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(10);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getMultipleRecordsMultidimRowKey().get(4));
        }

        // When 3
        range1 = rangeFactory.createExactRange(field1, 8L);
        range2 = rangeFactory.createExactRange(field2, "notthere");
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        assertThat(results.hasNext()).isFalse();

        // When 4
        range1 = rangeFactory.createRange(field1, -100000L, true, 123456789L, true);
        range2 = rangeFactory.createRange(field2, "0", true, "99999999999", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 4
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(100);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(getMultipleRecordsMultidimRowKey()));

        // When 5
        range1 = rangeFactory.createRange(field1, 2L, true, 5L, true);
        range2 = rangeFactory.createRange(field2, "3", true, "6", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 5
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(30);
        Set<Record> expectedResults = new HashSet<>(
                getMultipleRecordsMultidimRowKey().stream()
                        .filter(r -> ((long) r.get("key1")) >= 2L && ((long) r.get("key1")) <= 5L)
                        .filter(r -> ((String) r.get("key2")).compareTo("3") >= 0 && ((String) r.get("key2")).compareTo("6") <= 0)
                        .collect(Collectors.toList())
        );

        assertThat(new HashSet<>(resultsAsList)).isEqualTo(expectedResults);

        // When 6
        range1 = rangeFactory.createRange(field1, 2L, true, 500L, true);
        range2 = rangeFactory.createRange(field2, "3", true, "6", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 6
        assertThat(leafPartitionQueries).hasSize(2);
        LeafPartitionQuery leftLeafPartitionQuery;
        LeafPartitionQuery rightLeafPartitionQuery;
        if (leafPartitionQueries.get(0).getLeafPartitionId().equals(leftPartition.getId())) {
            leftLeafPartitionQuery = leafPartitionQueries.get(0);
            rightLeafPartitionQuery = leafPartitionQueries.get(1);
        } else {
            leftLeafPartitionQuery = leafPartitionQueries.get(1);
            rightLeafPartitionQuery = leafPartitionQueries.get(0);
        }
        LeafPartitionQuery expectedLeftLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", leftLeafPartitionQuery.getSubQueryId(), region, leftPartition.getId(), leftPartition.getRegion(), filesInLeftPartition)
                .build();
        assertThat(leftLeafPartitionQuery).isEqualTo(expectedLeftLeafPartitionQuery);
        LeafPartitionQuery expectedRightLeafPartitionQuery = new LeafPartitionQuery
                .Builder("myTable", "id", rightLeafPartitionQuery.getSubQueryId(), region, rightPartition.getId(), rightPartition.getRegion(), filesInRightPartition)
                .build();
        assertThat(rightLeafPartitionQuery).isEqualTo(expectedRightLeafPartitionQuery);
    }

    @Test
    public void shouldReturnCorrectDataWhenRecordsInMultipleFilesInMultiplePartitionsMultidimensionalKey()
            throws StateStoreException, InterruptedException, QueryException,
            IOException, IteratorException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        Field field1 = new Field("key1", new StringType());
        Field field2 = new Field("key2", new StringType());
        schema.setRowKeyFields(field1, field2);
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        //  Partitions:
        //  - Root partition covers the whole space
        //  - Root has 2 children: one is 1 and 3 below, the other is 2 and 4
        //  - There are 4 leaf partitions:
        //      null +-----------+-----------+
        //           |     3     |    4      |
        //           |           |           |
        //       "T" +-----------+-----------+
        //           |           |           |
        //           |     1     |    2      |
        //           |           |           |
        //           |           |           |
        //        "" +-----------+-----------+
        //           ""         "I"          null      (Dimension 1)
        StateStore stateStore = getStateStore(schema);
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        // Add 4 records - record i is in the center of partition i
        Record record1 = createRecordMultidimensionalKey("D", "J", 10L, 100L);
        Record record2 = createRecordMultidimensionalKey("K", "H", 1000L, 10000L);
        Record record3 = createRecordMultidimensionalKey("C", "X", 100000L, 1000000L);
        Record record4 = createRecordMultidimensionalKey("P", "Z", 10000000L, 100000000L);
        List<Record> records = Arrays.asList(record1, record2, record3, record4);
        ingestData(instanceProperties, stateStore, schema, records.iterator());
        // Split the root partition into 2: 1 and 3, and 2 and 4
        Range leftRange1 = new RangeFactory(schema).createRange(field1, "", "I");
        Range leftRange2 = new RangeFactory(schema).createRange(field2, "", null);
        Partition leftPartition = new Partition(rowKeyTypes,
                new Region(Arrays.asList(leftRange1, leftRange2)),
                "left", true, "root", new ArrayList<>(), -1);

        Range rightRange1 = new RangeFactory(schema).createRange(field1, "I", null);
        Range rightRange2 = new RangeFactory(schema).createRange(field2, "", null);
        Partition rightPartition = new Partition(rowKeyTypes,
                new Region(Arrays.asList(rightRange1, rightRange2)),
                "right", true, "root", new ArrayList<>(), -1);

        rootPartition.setLeafPartition(false);
        rootPartition.setChildPartitionIds(Arrays.asList("left", "right"));
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition,
                leftPartition, rightPartition);
        ingestData(instanceProperties, stateStore, schema, records.iterator());

        // 4 leaf partitions
        Range range11 = new RangeFactory(schema).createRange(field1, "", "I");
        Range range12 = new RangeFactory(schema).createRange(field2, "", "T");
        Partition partition1 = new Partition(rowKeyTypes,
                new Region(Arrays.asList(range11, range12)),
                "P1", true, "left", new ArrayList<>(), -1);

        Range range21 = new RangeFactory(schema).createRange(field1, "I", null);
        Range range22 = new RangeFactory(schema).createRange(field2, "", "T");
        Partition partition2 = new Partition(rowKeyTypes,
                new Region(Arrays.asList(range21, range22)),
                "P2", true, "right", new ArrayList<>(), -1);

        Range range31 = new RangeFactory(schema).createRange(field1, "", "I");
        Range range32 = new RangeFactory(schema).createRange(field2, "T", null);
        Partition partition3 = new Partition(rowKeyTypes,
                new Region(Arrays.asList(range31, range32)),
                "P3", true, "left", new ArrayList<>(), -1);

        Range range41 = new RangeFactory(schema).createRange(field1, "I", null);
        Range range42 = new RangeFactory(schema).createRange(field2, "T", null);
        Partition partition4 = new Partition(rowKeyTypes,
                new Region(Arrays.asList(range41, range42)),
                "P4", true, "right", new ArrayList<>(), -1);

        // Split the left partition into 1 and 3
        leftPartition.setLeafPartition(false);
        leftPartition.setChildPartitionIds(Arrays.asList("P1", "P3"));
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(leftPartition,
                partition1, partition3);
        // Split the right partition into 2 and 4
        rightPartition.setLeafPartition(false);
        rightPartition.setChildPartitionIds(Arrays.asList("P2", "P4"));
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(rightPartition,
                partition2, partition4);
        ingestData(instanceProperties, stateStore, schema, records.iterator());

        List<String> filesInLeafPartition1 = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(partition1.getId()) || f.getPartitionId().equals(leftPartition.getId()) || f.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition2 = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(partition2.getId()) || f.getPartitionId().equals(rightPartition.getId()) || f.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition3 = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(partition3.getId()) || f.getPartitionId().equals(leftPartition.getId()) || f.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        List<String> filesInLeafPartition4 = stateStore.getActiveFiles().stream()
                .filter(f -> f.getPartitionId().equals(partition4.getId()) || f.getPartitionId().equals(rightPartition.getId()) || f.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());

        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1 - query for entire space
        Range range1 = rangeFactory.createRange(field1, "", true, null, false);
        Range range2 = rangeFactory.createRange(field2, "", true, null, false);
        Region region = new Region(Arrays.asList(range1, range2));

        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(12); // 12 because the same data was added 3 times at different levels of the tree
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(records));

        // When 2 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", true, "H", true);
        range2 = rangeFactory.createRange(field2, "", true, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(3);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(Collections.singletonList(record1)));

        // When 3 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", true, "H", false);
        range2 = rangeFactory.createRange(field2, "", true, "S", false);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(3);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(Collections.singletonList(record1)));

        // When 4 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", false, "H", true);
        range2 = rangeFactory.createRange(field2, "", false, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 4
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(3);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(Collections.singletonList(record1)));

        // When 5 - query for range within partition 1
        range1 = rangeFactory.createRange(field1, "", false, "H", false);
        range2 = rangeFactory.createRange(field2, "", false, "S", false);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 5
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(3);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(Collections.singletonList(record1)));

        // When 6 - query for range within partitions 1 and 2
        range1 = rangeFactory.createRange(field1, "", true, "Z", true);
        range2 = rangeFactory.createRange(field2, "", true, "S", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 6
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(6);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(Arrays.asList(record1, record2)));

        // When 7 - query for range to the right of the data in partitions 2 and 4
        range1 = rangeFactory.createRange(field1, "T", true, "Z", true);
        range2 = rangeFactory.createRange(field2, "", true, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 7
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).isEmpty();

        // When 8 - query for a 1-dimensional range
        range1 = rangeFactory.createRange(field1, "J", true, "Z", true);
        region = new Region(range1);
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 8
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(6);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(new HashSet<>(Arrays.asList(record2, record4)));

        // When 9 - query for a range where the first dimension is constant
        range1 = rangeFactory.createExactRange(field1, "C");
        range2 = rangeFactory.createRange(field2, "", true, null, true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 9
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(3);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(Sets.newHashSet(record3));

        // When 10 - query for a range where the max equals record1 and max is not inclusive
        range1 = rangeFactory.createRange(field1, "", true, "D", false);
        range2 = rangeFactory.createRange(field2, "", true, "T", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 10
        assertThat(results.hasNext()).isFalse();

        // When 11 - query for a range where the max equals record1 and max is inclusive
        range1 = rangeFactory.createRange(field1, "", true, "D", true);
        range2 = rangeFactory.createRange(field2, "", true, "T", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 11
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(3);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(Sets.newHashSet(record1));

        // When 12 - query for a range where the boundaries cover all 4 records, min is inclusive, max is not inclusive
        // Record i is in range? 1 - yes; 2 - yes; 3 - yes; 4 - no
        range1 = rangeFactory.createRange(field1, "C", true, "P", false);
        range2 = rangeFactory.createRange(field2, "H", true, "Z", false);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 12
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(9);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(Sets.newHashSet(record1, record2, record3));

        // When 13 - query for a range where the boundaries cover all 4 records, min is inclusive, and max is inclusive
        // Record i is in range? 1 - yes; 2 - yes; 3 - yes; 4 - yes
        range1 = rangeFactory.createRange(field1, "C", true, "P", true);
        range2 = rangeFactory.createRange(field2, "H", true, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 13
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(12);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(Sets.newHashSet(record1, record2, record3, record4));

        // When 14 - query for a range where the boundaries cover all 4 records, min is not inclusive, and max is not inclusive
        // Record i is in range? 1 - yes; 2 - no; 3 - no; 4 - no
        range1 = rangeFactory.createRange(field1, "C", false, "P", false);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", false);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 14
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(3);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(Sets.newHashSet(record1));

        // When 15 - query for a range where the boundaries cover all 4 records, min is not inclusive, and max is inclusive
        // Record i is in range? 1 - yes; 2 - no; 3 - no; 4 - yes
        range1 = rangeFactory.createRange(field1, "C", false, "P", true);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 15
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        assertThat(resultsAsList).hasSize(6);
        assertThat(new HashSet<>(resultsAsList)).isEqualTo(Sets.newHashSet(record1, record4));

        // When 16
        range1 = rangeFactory.createRange(field1, "C", false, "P", true);
        range2 = rangeFactory.createRange(field2, "H", false, "Z", true);
        region = new Region(Arrays.asList(range1, range2));
        query = new Query.Builder("myTable", "id", region).build();
        List<LeafPartitionQuery> leafPartitionQueries = queryExecutor.splitIntoLeafPartitionQueries(query);

        // Then 16
        assertThat(leafPartitionQueries).hasSize(4);
        LeafPartitionQuery leafPartition1Query = leafPartitionQueries.stream()
                .filter(p -> p.getLeafPartitionId().equals("P1"))
                .findFirst()
                .get();
        LeafPartitionQuery leafPartition2Query = leafPartitionQueries.stream()
                .filter(p -> p.getLeafPartitionId().equals("P2"))
                .findFirst()
                .get();
        LeafPartitionQuery leafPartition3Query = leafPartitionQueries.stream()
                .filter(p -> p.getLeafPartitionId().equals("P3"))
                .findFirst()
                .get();
        LeafPartitionQuery leafPartition4Query = leafPartitionQueries.stream()
                .filter(p -> p.getLeafPartitionId().equals("P4"))
                .findFirst()
                .get();
        LeafPartitionQuery expectedLeafPartition1Query = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartition1Query.getSubQueryId(), region, partition1.getId(), partition1.getRegion(), filesInLeafPartition1)
                .build();
        assertThat(leafPartition1Query).isEqualTo(expectedLeafPartition1Query);
        LeafPartitionQuery expectedLeafPartition2Query = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartition2Query.getSubQueryId(), region, partition2.getId(), partition2.getRegion(), filesInLeafPartition2)
                .build();
        assertThat(leafPartition2Query).isEqualTo(expectedLeafPartition2Query);
        LeafPartitionQuery expectedLeafPartition3Query = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartition3Query.getSubQueryId(), region, partition3.getId(), partition3.getRegion(), filesInLeafPartition3)
                .build();
        assertThat(leafPartition3Query).isEqualTo(expectedLeafPartition3Query);
        LeafPartitionQuery expectedLeafPartition4Query = new LeafPartitionQuery
                .Builder("myTable", "id", leafPartition4Query.getSubQueryId(), region, partition4.getId(), partition4.getRegion(), filesInLeafPartition4)
                .build();
        assertThat(leafPartition4Query).isEqualTo(expectedLeafPartition4Query);
    }

    @Test
    public void shouldReturnDataCorrectlySorted()
            throws StateStoreException, InterruptedException,
            IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        schema.setSortKeyFields(new Field("value1", new LongType()));
        schema.setValueFields(new Field("value2", new LongType()));
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema, Collections.singletonList(5L));
        ingestData(instanceProperties, stateStore, schema, getMultipleRecordsForTestingSorting().iterator());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        List<Record> expectedResults = getMultipleRecordsForTestingSorting()
                .stream()
                .filter(r -> ((long) r.get("key")) == 1L)
                .sorted(Comparator.comparing(r -> ((Long) r.get("value1"))))
                .collect(Collectors.toList());
        assertThat(resultsAsList).isEqualTo(expectedResults);

        // When 2
        region = new Region(rangeFactory.createExactRange(field, 5L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        expectedResults = getMultipleRecordsForTestingSorting()
                .stream()
                .filter(r -> ((long) r.get("key")) == 5L)
                .sorted(Comparator.comparing(r -> ((Long) r.get("value1"))))
                .collect(Collectors.toList());
        assertThat(resultsAsList).isEqualTo(expectedResults);

        // When 3
        region = new Region(rangeFactory.createExactRange(field, 0L));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        assertThat(results.hasNext()).isFalse();
    }

    @Test
    public void shouldReturnCorrectDataWhenOneRecordInOneFileInOnePartitionAndCompactionIteratorApplied()
            throws StateStoreException, InterruptedException,
            IOException, IteratorException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("id", new StringType());
        schema.setRowKeyFields(field);
        schema.setValueFields(new Field("timestamp", new LongType()));
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
        tableProperties.set(ITERATOR_CONFIG, "timestamp,1000000");
        StateStore stateStore = getStateStore(schema);
        List<Record> records = getRecordsForAgeOffIteratorTest();
        ingestData(instanceProperties, stateStore, schema, records.iterator());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When 1
        Region region = new Region(rangeFactory.createExactRange(field, "1"));
        Query query = new Query.Builder("myTable", "id", region).build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then 1
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(1);
        assertThat(resultsAsList.get(0)).isEqualTo(records.get(0));

        // When 2
        region = new Region(rangeFactory.createExactRange(field, "0"));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 2
        assertThat(results.hasNext()).isFalse();

        // When 3
        region = new Region(rangeFactory.createExactRange(field, "2"));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 3
        assertThat(results.hasNext()).isFalse();

        // When 4
        region = new Region(rangeFactory.createExactRange(field, "3"));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 4
        assertThat(results.hasNext()).isFalse();

        // When 5
        region = new Region(rangeFactory.createExactRange(field, "4"));
        query = new Query.Builder("myTable", "id", region).build();
        results = queryExecutor.execute(query);

        // Then 5
        resultsAsList.clear();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(1);
        assertThat(resultsAsList.get(0)).isEqualTo(records.get(3));
    }

    @Test
    public void shouldReturnCorrectDataWhenQueryTimeIteratorApplied()
            throws InterruptedException, IteratorException,
            IOException, StateStoreException, ObjectFactoryException, QueryException {
        // Given
        Schema schema = getSecurityLabelSchema();
        Field field = schema.getRowKeyFields().get(0);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        StateStore stateStore = getStateStore(schema, Collections.singletonList(5L));
        for (int i = 0; i < 10; i++) {
            ingestData(instanceProperties, stateStore, schema,
                    getRecordsForQueryTimeIteratorTest(i % 2 == 0 ? "notsecret" : "secret").iterator());
        }
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, ""),
                tableProperties, stateStore, new Configuration(), executorService);
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("myTable", "id", region)
                .setQueryTimeIteratorClassName(SecurityFilteringIterator.class.getName())
                .setQueryTimeIteratorConfig("securityLabel,notsecret")
                .build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then
        List<Record> resultsAsList = new ArrayList<>();
        while (results.hasNext()) {
            resultsAsList.add(results.next());
        }
        results.close();
        assertThat(resultsAsList).hasSize(5);
        for (Record record : resultsAsList) {
            assertThat(record).isEqualTo(getRecordsForQueryTimeIteratorTest("notsecret").get(0));
        }
    }

    @Test
    public void shouldReturnOnlyRequestedValuesWhenSpecified() throws StateStoreException, InterruptedException, IteratorException, ObjectFactoryException, IOException, QueryException {
        // Given
        Schema schema = getLongKeySchema();
        Field field = schema.getRowKeyFields().get(0);
        StateStore stateStore = getStateStore(schema);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        ingestData(instanceProperties, stateStore, schema, getRecords().iterator());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, "/tmp"), tableProperties, stateStore,
                new Configuration(), Executors.newFixedThreadPool(1));
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("unused", "abc", region)
                .setQueryTimeIteratorClassName(null)
                .setQueryTimeIteratorConfig(null)
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(Lists.newArrayList("value2"))
                .build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then
        assertThat(results.hasNext()).isTrue();
        Record result = results.next();
        assertThat(results.hasNext()).isFalse();
        assertThat(result.getKeys()).doesNotContain("value1").contains("key", "value2");
    }

    @Test
    public void shouldIncludeFieldsRequiredByIteratorsEvenIfNotSpecifiedByTheUser() throws StateStoreException, InterruptedException, IteratorException, ObjectFactoryException, IOException, QueryException {
        // Given
        Schema schema = getSecurityLabelSchema();
        Field field = schema.getRowKeyFields().get(0);
        StateStore stateStore = getStateStore(schema);
        InstanceProperties instanceProperties = new InstanceProperties();
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(schema);
        ingestData(instanceProperties, stateStore, schema, getRecordsForQueryTimeIteratorTest("secret").iterator());
        QueryExecutor queryExecutor = new QueryExecutor(new ObjectFactory(instanceProperties, null, "/tmp"), tableProperties, stateStore,
                new Configuration(), Executors.newFixedThreadPool(1));
        queryExecutor.init();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder("unused", "abc", region)
                .setQueryTimeIteratorClassName(SecurityFilteringIterator.class.getName())
                .setQueryTimeIteratorConfig("securityLabel,secret")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(Lists.newArrayList("value"))
                .build();
        CloseableIterator<Record> results = queryExecutor.execute(query);

        // Then
        assertThat(results.hasNext()).isTrue();
        while (results.hasNext()) {
            Record result = results.next();
            assertThat(result.getKeys()).contains("key", "value", "securityLabel");
        }
    }

    protected Schema getLongKeySchema() {
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        return schema;
    }

    protected Schema getSecurityLabelSchema() {
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        schema.setValueFields(new Field("value", new LongType()), new Field("securityLabel", new StringType()));
        return schema;
    }

    protected void ingestData(InstanceProperties instanceProperties, StateStore stateStore, Schema schema, Iterator<Record> recordIterator)
            throws IOException, ObjectFactoryException, InterruptedException, IteratorException, StateStoreException {
        new IngestRecordsFromIterator(new ObjectFactory(instanceProperties, null, "/tmp"),
                recordIterator,
                folder.newFolder().getAbsolutePath(),
                100L,
                100L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "snappy",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120).write();
    }

    protected List<Record> getRecords() {
        List<Record> records = new ArrayList<>();
        Record record = new Record();
        record.put("key", 1L);
        record.put("value1", 10L);
        record.put("value2", 100L);
        records.add(record);
        return records;
    }

    protected List<Record> getMultipleIdenticalRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.addAll(getRecords());
        }
        return records;
    }

    protected List<Record> getMultipleRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Record record = new Record();
            record.put("key", (long) i);
            record.put("value1", i * 10L);
            record.put("value2", i * 100L);
            records.add(record);
        }
        return records;
    }

    protected List<Record> getMultipleRecordsMultidimRowKey() {
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Record record = new Record();
            record.put("key1", (long) i);
            record.put("key2", "" + i);
            record.put("value1", i * 10L);
            record.put("value2", i * 100L);
            records.add(record);
        }
        return records;
    }

    protected List<Record> getMultipleRecordsForTestingSorting() {
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            for (int j = 1000; j >= 900; j--) {
                Record record = new Record();
                record.put("key", (long) i);
                record.put("value1", (long) j);
                record.put("value2", i * 100L);
                records.add(record);
            }
        }
        return records;
    }

    protected List<Record> getRecordsForAgeOffIteratorTest() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("id", "1");
        record1.put("timestamp", System.currentTimeMillis());
        records.add(record1);
        Record record2 = new Record();
        record2.put("id", "2");
        record2.put("timestamp", System.currentTimeMillis() - 1_000_000_000L);
        records.add(record2);
        Record record3 = new Record();
        record3.put("id", "3");
        record3.put("timestamp", System.currentTimeMillis() - 2_000_000L);
        records.add(record3);
        Record record4 = new Record();
        record4.put("id", "4");
        record4.put("timestamp", System.currentTimeMillis());
        records.add(record4);
        return records;
    }

    protected List<Record> getRecordsForQueryTimeIteratorTest(String securityLabel) {
        List<Record> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Record record = new Record();
            record.put("key", (long) i);
            record.put("value", i * 10L);
            record.put("securityLabel", securityLabel);
            records.add(record);
        }
        return records;
    }

    protected StateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStore(schema, Collections.emptyList());
    }

    protected StateStore getStateStore(Schema schema,
                                       List<Object> splitPoints) throws StateStoreException {
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(UUID.randomUUID().toString(), schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise(new PartitionsFromSplitPoints(schema, splitPoints).construct());
        return dynamoStateStore;
    }

    private static Record createRecordMultidimensionalKey(String key1, String key2, long value1, long value2) {
        Record record = new Record();
        record.put("key1", key1);
        record.put("key2", key2);
        record.put("value1", value1);
        record.put("value2", value2);
        return record;
    }
}
