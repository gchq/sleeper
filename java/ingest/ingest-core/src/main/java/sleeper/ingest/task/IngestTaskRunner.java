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

package sleeper.ingest.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.iterator.IteratorException;
import sleeper.ingest.IngestResult;
import sleeper.ingest.job.IngestJobHandler;
import sleeper.ingest.job.IngestJobSource;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.time.Instant;
import java.util.function.Supplier;

public class IngestTaskRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestTaskRunner.class);

    private final IngestJobSource jobSource;
    private final String taskId;
    private final IngestTaskStatusStore statusStore;
    private final Supplier<Instant> getTimeNow;
    private final IngestJobHandler runJobCallback;

    public IngestTaskRunner(
            IngestJobSource jobSource, String taskId, IngestTaskStatusStore statusStore,
            IngestJobHandler runJobCallback, Supplier<Instant> getTimeNow) {
        this.getTimeNow = getTimeNow;
        this.jobSource = jobSource;
        this.taskId = taskId;
        this.statusStore = statusStore;
        this.runJobCallback = runJobCallback;
    }

    public IngestTaskRunner(
            IngestJobSource jobSource, String taskId,
            IngestTaskStatusStore statusStore, IngestJobHandler runJobCallback) {
        this(jobSource, taskId, statusStore, runJobCallback, Instant::now);
    }

    public void run() throws IOException, IteratorException, StateStoreException {
        Instant startTaskTime = getTimeNow.get();
        IngestTaskStatus.Builder taskStatusBuilder = IngestTaskStatus.builder().taskId(taskId).startTime(startTaskTime);
        statusStore.taskStarted(taskStatusBuilder.build());
        LOGGER.info("IngestTask started at = {}", startTaskTime);

        IngestTaskFinishedStatus.Builder taskFinishedStatusBuilder = IngestTaskFinishedStatus.builder();
        jobSource.consumeJobs(job -> {
            Instant startTime = getTimeNow.get();
            IngestResult result = runJobCallback.ingest(job);
            Instant finishTime = getTimeNow.get();
            taskFinishedStatusBuilder.addIngestResult(result, startTime, finishTime);
            return result;
        });

        Instant finishTaskTime = getTimeNow.get();
        taskStatusBuilder.finishedStatus(taskFinishedStatusBuilder
                .finish(startTaskTime, finishTaskTime).build());
        statusStore.taskFinished(taskStatusBuilder.build());
        LOGGER.info("IngestTask finished at = {}", finishTaskTime);
    }
}
