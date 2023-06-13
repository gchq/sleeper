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

package sleeper.ingest.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.IteratorException;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.IngestResult;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobHandler;
import sleeper.ingest.job.IngestJobSource;
import sleeper.ingest.job.status.IngestJobFinishedData;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.ingest.job.status.IngestJobStartedData.startOfRun;

public class IngestTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestTask.class);

    private final IngestJobSource jobSource;
    private final String taskId;
    private final IngestTaskStatusStore taskStatusStore;
    private final IngestJobStatusStore jobStatusStore;
    private final Supplier<Instant> getTimeNow;
    private final IngestJobHandler runJobCallback;

    public IngestTask(
            IngestJobSource jobSource, String taskId,
            IngestTaskStatusStore taskStatusStore,
            IngestJobStatusStore jobStatusStore,
            IngestJobHandler runJobCallback, Supplier<Instant> getTimeNow) {
        this.getTimeNow = getTimeNow;
        this.jobSource = jobSource;
        this.taskId = taskId;
        this.taskStatusStore = taskStatusStore;
        this.jobStatusStore = jobStatusStore;
        this.runJobCallback = runJobCallback;
    }

    public IngestTask(
            IngestJobSource jobSource, String taskId,
            IngestTaskStatusStore taskStatusStore, IngestJobStatusStore jobStatusStore,
            IngestJobHandler runJobCallback) {
        this(jobSource, taskId, taskStatusStore, jobStatusStore, runJobCallback, Instant::now);
    }

    public void run() throws IOException, IteratorException, StateStoreException {
        Instant startTaskTime = getTimeNow.get();
        IngestTaskStatus.Builder taskStatusBuilder = IngestTaskStatus.builder().taskId(taskId).startTime(startTaskTime);
        taskStatusStore.taskStarted(taskStatusBuilder.build());
        LOGGER.info("IngestTask started at = {}", startTaskTime);

        IngestTaskFinishedStatus.Builder taskFinishedStatusBuilder = IngestTaskFinishedStatus.builder();
        try {
            jobSource.consumeJobs(job -> runJob(job, taskFinishedStatusBuilder));
        } finally {
            Instant finishTaskTime = getTimeNow.get();
            taskStatusBuilder.finished(finishTaskTime, taskFinishedStatusBuilder);
            taskStatusStore.taskFinished(taskStatusBuilder.build());
            LOGGER.info("IngestTask finished at = {}", finishTaskTime);
        }
    }

    private IngestResult runJob(IngestJob job, IngestTaskFinishedStatus.Builder taskBuilder)
            throws IteratorException, StateStoreException, IOException {

        Instant startTime = getTimeNow.get();
        jobStatusStore.jobStarted(startOfRun(taskId, job, startTime));

        IngestResult result = IngestResult.noFiles();
        try {
            result = runJobCallback.ingest(job);
        } finally {
            Instant finishTime = getTimeNow.get();
            RecordsProcessedSummary summary = new RecordsProcessedSummary(result.asRecordsProcessed(), startTime, finishTime);
            jobStatusStore.jobFinished(IngestJobFinishedData.from(taskId, job, summary));
            taskBuilder.addJobSummary(summary);
        }

        return result;
    }
}
