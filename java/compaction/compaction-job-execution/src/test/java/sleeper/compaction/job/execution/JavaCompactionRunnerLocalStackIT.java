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
package sleeper.compaction.job.execution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestBase;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestData;
import sleeper.compaction.tracker.job.DynamoDBCompactionJobTrackerCreator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.localstack.test.SleeperLocalStackClients;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdToInputFiles;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class JavaCompactionRunnerLocalStackIT extends CompactionRunnerTestBase {

    private static AmazonDynamoDB dynamoClient = SleeperLocalStackClients.DYNAMO_CLIENT;
    private static AmazonS3 s3Client = SleeperLocalStackClients.S3_CLIENT;
    private static S3AsyncClient s3AsyncClient = SleeperLocalStackClients.S3_ASYNC_CLIENT;
    private static Configuration configuration = SleeperLocalStackClients.HADOOP_CONF;

    @BeforeEach
    void setUp() {
        String dataBucket = "data-bucket-" + instanceProperties.get(ID);
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.set(DATA_BUCKET, dataBucket);
        instanceProperties.unset(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE);
        s3Client.createBucket(dataBucket);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoClient);
    }

    @Test
    public void shouldRunCompactionJob() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        createStateStore();
        PartitionTree tree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        update(stateStore).initialise(tree.getAllPartitions());

        List<Record> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddLongs();
        FileReference file1 = ingestRecordsGetFile(data1);
        FileReference file2 = ingestRecordsGetFile(data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        RecordsProcessed summary = compact(compactionJob, configuration);

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = CompactionRunnerTestData.combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
        assertThat(SketchesDeciles.from(readSketches(schema, compactionJob.getOutputFile())))
                .isEqualTo(SketchesDeciles.from(schema, expectedResults));
    }

    protected FileReference ingestRecordsGetFile(List<Record> records) throws Exception {
        return ingestRecordsGetFile(records, builder -> builder
                .hadoopConfiguration(configuration)
                .s3AsyncClient(s3AsyncClient));
    }

    private void createStateStore() {
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0");
        stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient, configuration)
                .getStateStore(tableProperties);
    }
}
