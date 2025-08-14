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
package sleeper.systemtest.datageneration;

import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.clients.util.command.CommandUtils;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.util.command.Command.command;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DATA_ENGINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class DockerImageTestCompactionTask extends LocalStackTestBase {

    String dockerImage = "compaction-job-execution:test";
    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = createSchemaWithKey("key", new LongType());
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    @TempDir
    Path tempDir;
    StateStore stateStore;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.setEnum(DEFAULT_DATA_ENGINE, DataEngine.DATAFUSION);
        instanceProperties.set(COMPACTION_TRACKER_ENABLED, "false");
        instanceProperties.set(COMPACTION_TASK_WAIT_TIME_IN_SECONDS, "0");
        instanceProperties.set(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, "0");
        instanceProperties.set(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, "0");
        instanceProperties.set(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, "1");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
        update(stateStore).initialise(tableProperties);
    }

    @Test
    void shouldRunCompaction() throws Exception {
        // Given
        List<Row> rows = List.of(
                new Row(Map.of("key", 10L)),
                new Row(Map.of("key", 20L)));
        FileReference file = addFileAtRoot("test", rows);
        CompactionJob job = sendCompactionJobAtRoot("test-job", List.of(file));
        update(stateStore).assignJobId("test-job", List.of(file));

        // When
        CommandUtils.runCommandLogOutputWithPty(command(
                "docker", "run", "--rm", "-it",
                "--env", "AWS_ENDPOINT_URL=" + localStackContainer.getEndpoint(),
                dockerImage, instanceProperties.get(CONFIG_BUCKET)));

        // Then
        assertThat(readOutputFile(job)).containsExactlyElementsOf(rows);
    }

    private FileReference addFileAtRoot(String name, List<Row> rows) {
        FileReference reference = fileFactory().rootFile(name, rows.size());
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(reference.getFilename());
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, tableProperties, hadoopConf)) {
            for (Row row : rows) {
                writer.write(row);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        update(stateStore).addFile(reference);
        return reference;
    }

    private CompactionJob sendCompactionJobAtRoot(String jobId, List<FileReference> files) {
        CompactionJobFactory factory = new CompactionJobFactory(instanceProperties, tableProperties);
        CompactionJob job = factory.createCompactionJob(jobId, files, "root");
        CompactionJobSerDe serDe = new CompactionJobSerDe();
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .messageBody(serDe.toJson(job))
                .build());
        return job;
    }

    private List<Row> readOutputFile(CompactionJob job) {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(job.getOutputFile());
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).withConf(hadoopConf).build())) {
            List<Row> rows = new ArrayList<>();
            reader.forEachRemaining(rows::add);
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(instanceProperties, tableProperties, stateStore);
    }

}
