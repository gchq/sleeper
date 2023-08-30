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
package sleeper.systemtest.drivers.compaction;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.configuration.properties.instance.SystemDefinedInstanceProperty;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.util.InvokeSystemTestLambda;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;

public class InvokeCompactionTaskCreationUntilAllJobsStarted {
    static final int POLL_INTERVAL_MILLIS = 10000;
    static final int MAX_POLLS = 5;
    private final PollWithRetries pollWithRetries;
    private final String tableName;
    private final CompactionJobStatusStore statusStore;
    private final InvokeSystemTestLambda.Client lambdaClient;
    private final SystemDefinedInstanceProperty lambdaProperty;

    private InvokeCompactionTaskCreationUntilAllJobsStarted(
            String tableName, CompactionJobStatusStore statusStore, InvokeSystemTestLambda.Client lambdaClient,
            SystemDefinedInstanceProperty lambdaProperty, PollWithRetries pollWithRetries) {
        this.tableName = tableName;
        this.statusStore = statusStore;
        this.lambdaClient = lambdaClient;
        this.lambdaProperty = lambdaProperty;
        this.pollWithRetries = pollWithRetries;
    }

    public static InvokeCompactionTaskCreationUntilAllJobsStarted forSplitting(
            String tableName, CompactionJobStatusStore statusStore, InvokeSystemTestLambda.Client lambdaClient, PollWithRetries pollWithRetries) {
        return new InvokeCompactionTaskCreationUntilAllJobsStarted(tableName, statusStore, lambdaClient,
                SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION,
                pollWithRetries);
    }

    public void pollUntilFinished() throws InterruptedException {
        pollWithRetries.pollUntil("all compaction jobs have started", () -> {
            lambdaClient.invokeLambda(lambdaProperty);
            return statusStore.getAllJobs(tableName).stream().allMatch(CompactionJobStatus::isStarted);
        });
    }
}
