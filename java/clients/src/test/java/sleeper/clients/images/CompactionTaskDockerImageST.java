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
package sleeper.clients.images;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.row.Row;
import sleeper.core.statestore.FileReference;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DATA_ENGINE;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class CompactionTaskDockerImageST extends DockerImageTestBase {

    @BeforeEach
    void setUp() {
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.setEnum(DEFAULT_DATA_ENGINE, DataEngine.DATAFUSION);
        instanceProperties.set(COMPACTION_TRACKER_ENABLED, "false");
        instanceProperties.set(COMPACTION_TASK_WAIT_TIME_IN_SECONDS, "0");
        instanceProperties.set(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, "0");
        instanceProperties.set(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, "0");
        instanceProperties.set(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, "1");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        update(stateStore).initialise(tableProperties);
    }

    @Test
    void shouldRunDataFusionCompactionWithDockerImage() throws Exception {
        // Given
        List<Row> rows = List.of(
                new Row(Map.of("key", 10L)),
                new Row(Map.of("key", 20L)));
        FileReference file = addFileAtRoot("test", rows);
        CompactionJob job = sendCompactionJobAtRoot("test-job", List.of(file));
        update(stateStore).assignJobId("test-job", List.of(file));

        // When
        runTask("compaction-job-execution:test", instanceProperties.get(CONFIG_BUCKET));

        // Then
        assertThat(readOutputFile(job)).containsExactlyElementsOf(rows);
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
        return readFile(job.getOutputFile());
    }

}
