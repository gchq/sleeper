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
import sleeper.ingest.task.IngestTaskFinishedStatus;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;

import java.time.Instant;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class IngestJobQueueConsumerRunnerTest {

    private final IngestJobQueueConsumer queueConsumer = mock(IngestJobQueueConsumer.class);
    private final IngestTaskStatusStore statusStore = mock(IngestTaskStatusStore.class);

    @Test
    public void shouldRunAndReportTaskWithNoJobs() throws Exception {
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-12-07T12:37:00.123Z");
        Instant finishTime = Instant.parse("2022-12-07T12:38:00.123Z");
        IngestJobQueueConsumerRunner runner = new IngestJobQueueConsumerRunner(queueConsumer, taskId, statusStore,
                () -> startTime, () -> finishTime);
        runner.run();

        verify(statusStore).taskStarted(IngestTaskStatus.builder().taskId(taskId).startTime(startTime).build());
        verify(statusStore).taskFinished(IngestTaskStatus.builder().taskId(taskId).startTime(startTime)
                .finishedStatus(IngestTaskFinishedStatus.builder().finish(startTime, finishTime).build())
                .build());
    }
}
