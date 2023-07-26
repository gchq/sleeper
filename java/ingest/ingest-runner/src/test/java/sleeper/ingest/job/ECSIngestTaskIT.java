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

package sleeper.ingest.job;

import org.junit.jupiter.api.Test;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStoreCreator;
import sleeper.ingest.task.IngestTask;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;

public class ECSIngestTaskIT extends IngestJobQueueConsumerTestBase {

    private void consumeAndVerify(Schema sleeperSchema,
                                  List<Record> expectedRecordList,
                                  int expectedNoOfFiles) throws Exception {
        String localDir = createTempDirectory(temporaryFolder, null).toString();
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(sleeperSchema);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(), instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        IngestTask runner = createTaskRunner(instanceProperties, localDir, "test-task");
        runner.run();

        // Verify the results
        ResultVerifier.verify(
                stateStore,
                sleeperSchema,
                key -> 0,
                expectedRecordList,
                Collections.singletonMap(0, expectedNoOfFiles),
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                createTempDirectory(temporaryFolder, null).toString());
    }

    private IngestTask createTaskRunner(InstanceProperties instanceProperties,
                                        String localDir,
                                        String taskId) {
        return ECSIngestTask.createIngestTask(
                ObjectFactory.noUserJars(), instanceProperties, localDir, taskId,
                AWS_EXTERNAL_RESOURCE.getS3Client(), AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                AWS_EXTERNAL_RESOURCE.getSqsClient(), AWS_EXTERNAL_RESOURCE.getCloudWatchClient(),
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient(), AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
    }

    @Test
    public void shouldIngestParquetFilesPutOnTheQueue() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 2);
        List<Record> doubledRecords = Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                .flatMap(List::stream)
                .sorted(new RecordComparator(recordListAndSchema.sleeperSchema))
                .collect(Collectors.toList());
        IngestJob ingestJob = createJobWithTableAndFiles("id", TEST_TABLE_NAME, files);
        AWS_EXTERNAL_RESOURCE.getSqsClient()
                .sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(ingestJob));
        consumeAndVerify(recordListAndSchema.sleeperSchema, doubledRecords, 1);
    }

    @Test
    public void shouldContinueReadingFromQueueWhileMoreMessagesExist() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        int noOfJobs = 10;
        int noOfFilesPerJob = 4;
        List<IngestJob> ingestJobs = IntStream.range(0, noOfJobs)
                .mapToObj(jobNo -> {
                    try {
                        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "job-" + jobNo, noOfFilesPerJob);
                        return IngestJob.builder()
                                .tableName(TEST_TABLE_NAME).id(UUID.randomUUID().toString()).files(files)
                                .build();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
        List<Record> expectedRecords = Stream.of(Collections.nCopies(noOfJobs * noOfFilesPerJob, recordListAndSchema.recordList))
                .flatMap(List::stream)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        ingestJobs.forEach(ingestJob ->
                AWS_EXTERNAL_RESOURCE.getSqsClient()
                        .sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(ingestJob)));
        consumeAndVerify(recordListAndSchema.sleeperSchema, expectedRecords, noOfJobs);
    }
}
