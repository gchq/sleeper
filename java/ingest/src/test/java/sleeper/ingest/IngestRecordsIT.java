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
package sleeper.ingest;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.iterator.impl.AdditionIterator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.record.CloneRecord;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.testutils.IngestRecordsTestDataHelper;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestRecordsIT {
    private static final int DYNAMO_PORT = 8000;

    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private final Field field = new Field("key", new LongType());
    private final IngestRecordsTestDataHelper dataHelper = new IngestRecordsTestDataHelper();
    private final Schema schema = dataHelper.schemaWithRowKeys(field);

    private static DynamoDBStateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    private static DynamoDBStateStore getStateStore(Schema schema, List<Partition> initialPartitions)
            throws StateStoreException {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        String tableNameStub = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(tableNameStub, schema, dynamoDBClient);
        DynamoDBStateStore stateStore = dynamoDBStateStoreCreator.create();
        stateStore.initialise(initialPartitions);
        return stateStore;
    }

    @Test
    public void shouldWriteRecordsCorrectly() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);
        String localDir = folder.newFolder().getAbsolutePath();

        // When
        IngestProperties properties = dataHelper.defaultPropertiesBuilder(stateStore, schema, localDir, folder.newFolder().getAbsolutePath()).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : dataHelper.getRecords()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(dataHelper.getRecords().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isOne();
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(3L);
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(2L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        //  - Read file and check it has correct records
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(fileInfo.getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(2);
        assertThat(readRecords.get(0)).isEqualTo(dataHelper.getRecords().get(0));
        assertThat(readRecords.get(1)).isEqualTo(dataHelper.getRecords().get(1));
        //  - Local files should have been deleted
        assertThat(Files.walk(Paths.get(localDir)).filter(Files::isRegularFile).count()).isZero();
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = fileInfo.getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        dataHelper.getRecords().forEach(r -> expectedSketch.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteNoRecordsSuccessfully() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        IngestProperties properties = dataHelper.defaultPropertiesBuilder(stateStore, schema, folder.newFolder().getAbsolutePath(), folder.newFolder().getAbsolutePath()).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isZero();
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).isEmpty();
    }

    @Test
    public void shouldApplyIterator() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        IngestProperties properties = dataHelper.defaultPropertiesBuilder(stateStore, schema, folder.newFolder().getAbsolutePath(), folder.newFolder().getAbsolutePath())
                .iteratorClassName(AdditionIterator.class.getName()).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : dataHelper.getRecordsForAggregationIteratorTest()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(2L);
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((byte[]) fileInfo.getMinRowKey().get(0)).containsExactly(new byte[]{1, 1});
        assertThat((byte[]) fileInfo.getMaxRowKey().get(0)).containsExactly(new byte[]{11, 2});
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(2L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        //  - Read file and check it has correct records
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(fileInfo.getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords.size()).isEqualTo(2L);

        Record expectedRecord1 = new Record();
        expectedRecord1.put("key", new byte[]{1, 1});
        expectedRecord1.put("sort", 2L);
        expectedRecord1.put("value", 7L);
        assertThat(readRecords.get(0)).isEqualTo(expectedRecord1);
        Record expectedRecord2 = new Record();
        expectedRecord2.put("key", new byte[]{11, 2});
        expectedRecord2.put("sort", 1L);
        expectedRecord2.put("value", 4L);
        assertThat(readRecords.get(1)).isEqualTo(expectedRecord2);

        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = activeFiles.get(0).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<ByteArray> expectedSketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", schema);
        List<Record> sortedRecords = new ArrayList<>(dataHelper.getRecordsForAggregationIteratorTest());
        sortedRecords.sort(Comparator.comparing(o -> ByteArray.wrap(((byte[]) o.get("key")))));
        CloseableIterator<Record> aggregatedRecords = additionIterator.apply(new WrappedIterator<>(sortedRecords.iterator()));
        while (aggregatedRecords.hasNext()) {
            expectedSketch.update(ByteArray.wrap((byte[]) aggregatedRecords.next().get("key")));
        }
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch.getQuantile(d));
        }
    }
}
