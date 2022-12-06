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

package sleeper.ingest.job;

import org.junit.Test;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusStore;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusStoreCreator;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class IngestJobQueueConsumerRunnerIT extends IngestJobQueueConsumerTestBase {
    private static final String TEST_TASK_ID = "test-task";

    @Test
    public void shouldRecordIngestTaskFinishedWhenIngestCompleteWithNoJobRuns() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        IngestTaskStatusStore taskStatusStore = DynamoDBIngestTaskStatusStore.from(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                instanceProperties);

        // When
        IngestJobQueueConsumerRunner jobRunner = new IngestJobQueueConsumerRunner(
                ObjectFactory.noUserJars(),
                instanceProperties,
                "/mnt/scratch",
                AWS_EXTERNAL_RESOURCE.getSqsClient(),
                AWS_EXTERNAL_RESOURCE.getCloudWatchClient(),
                AWS_EXTERNAL_RESOURCE.getS3Client(),
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        jobRunner.run(TEST_TASK_ID);

        // Then
        assertThat(taskStatusStore.getTask(TEST_TASK_ID))
                .extracting(IngestTaskStatus::isFinished, IngestTaskStatus::getJobRuns)
                .containsExactly(true, 0);
    }

    @Test
    public void shouldRecordIngestTaskFinishedWhenIngestCompleteWithSingleJobRun() throws Exception {
        // Given
        Schema schema = sendSingleJobToQueue();
        InstanceProperties instanceProperties = getInstanceProperties();
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        IngestTaskStatusStore taskStatusStore = DynamoDBIngestTaskStatusStore.from(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                instanceProperties);

        // When
        IngestJobQueueConsumerRunner runner = createIngestJobQueueConsumerRunner(schema, instanceProperties);
        runner.run(TEST_TASK_ID);

        // Then
        assertThat(taskStatusStore.getTask(TEST_TASK_ID))
                .extracting(IngestTaskStatus::isFinished, IngestTaskStatus::getJobRuns)
                .containsExactly(true, 1);
    }

    @Test
    public void shouldRecordIngestTaskFinishedWhenIngestCompleteWithFourJobRuns() throws Exception {
        // Given
        Schema schema = sendFourJobsToQueue();
        InstanceProperties instanceProperties = getInstanceProperties();
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        IngestTaskStatusStore taskStatusStore = DynamoDBIngestTaskStatusStore.from(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                instanceProperties);

        IngestJobQueueConsumerRunner runner = createIngestJobQueueConsumerRunner(schema, instanceProperties);
        runner.run(TEST_TASK_ID);

        // Then
        assertThat(taskStatusStore.getTask(TEST_TASK_ID))
                .extracting(IngestTaskStatus::isFinished, IngestTaskStatus::getJobRuns)
                .containsExactly(true, 4);
    }

    private IngestJobQueueConsumerRunner createIngestJobQueueConsumerRunner(Schema schema, InstanceProperties instanceProperties) throws Exception {

        TableProperties tableProperties = createTable(schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(AWS_EXTERNAL_RESOURCE.getS3Client(), instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(), new InstanceProperties());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();

        // When
        String localDir = temporaryFolder.newFolder().getAbsolutePath();
        return new IngestJobQueueConsumerRunner(
                ObjectFactory.noUserJars(),
                instanceProperties,
                localDir,
                AWS_EXTERNAL_RESOURCE.getSqsClient(),
                AWS_EXTERNAL_RESOURCE.getCloudWatchClient(),
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                tablePropertiesProvider,
                stateStoreProvider,
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient(),
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
    }

    private Schema sendSingleJobToQueue() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));

        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 2);
        IngestJob ingestJob = IngestJob.builder()
                .tableName(TEST_TABLE_NAME).id("id").files(files)
                .build();
        AWS_EXTERNAL_RESOURCE.getSqsClient()
                .sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(ingestJob));
        return recordListAndSchema.sleeperSchema;
    }

    private Schema sendFourJobsToQueue() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));

        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 4);
        List<IngestJob> ingestJob = Arrays.asList(IngestJob.builder()
                        .tableName(TEST_TABLE_NAME).id("id").files(files)
                        .build(),
                IngestJob.builder()
                        .tableName(TEST_TABLE_NAME).id("id").files(files)
                        .build(),
                IngestJob.builder()
                        .tableName(TEST_TABLE_NAME).id("id").files(files)
                        .build(),
                IngestJob.builder()
                        .tableName(TEST_TABLE_NAME).id("id").files(files)
                        .build());
        ingestJob.forEach(job -> AWS_EXTERNAL_RESOURCE.getSqsClient()
                .sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(job)));
        return recordListAndSchema.sleeperSchema;
    }
}
