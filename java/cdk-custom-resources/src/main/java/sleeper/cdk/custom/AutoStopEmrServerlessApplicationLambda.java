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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;
import software.amazon.awssdk.services.emrserverless.model.JobRunSummary;

import sleeper.core.util.PollWithRetries;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Delete an EMR Serverless application.
 */
public class AutoStopEmrServerlessApplicationLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoStopEmrServerlessApplicationLambda.class);

    private final PollWithRetries poll;
    private final EmrServerlessClient emrServerlessClient;

    public AutoStopEmrServerlessApplicationLambda() {
        this(EmrServerlessClient.create(), PollWithRetries
                .intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(15)));
    }

    public AutoStopEmrServerlessApplicationLambda(EmrServerlessClient emrServerlessClient, PollWithRetries poll) {
        this.emrServerlessClient = emrServerlessClient;
        this.poll = poll;
    }

    /**
     * Handles an event triggered by CloudFormation.
     *
     * @param event   the event to handle
     * @param context the context
     */
    public void handleEvent(
            CloudFormationCustomResourceEvent event, Context context) throws InterruptedException {

        Map<String, Object> resourceProperties = event.getResourceProperties();
        String applicationId = (String) resourceProperties.get("applicationId");

        switch (event.getRequestType()) {
            case "Create":
                break;
            case "Update":
                break;
            case "Delete":
                stopApplication(applicationId);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void stopApplication(String applicationId) throws InterruptedException {

        LOGGER.info("Terminating {} running application: ", applicationId);

        List<JobRunSummary> jobRuns = emrServerlessClient.listJobRuns(request -> request.applicationId(applicationId)
                .states(JobRunState.RUNNING, JobRunState.SCHEDULED, JobRunState.PENDING, JobRunState.SUBMITTED))
                .jobRuns();

        LOGGER.info("Waiting for application jobs to terminate");
        jobRuns.forEach(jobRun -> emrServerlessClient.cancelJobRun(request -> request
                .applicationId(applicationId).jobRunId(jobRun.id())));

        if (!jobRuns.isEmpty()) {
            poll.pollUntil("all EMR Serverless jobs finished", () -> allJobsFinished(applicationId));
        }

        if (!isApplicationStopped(applicationId)) {
            emrServerlessClient.stopApplication(request -> request.applicationId(applicationId));

            LOGGER.info("Waiting for applications to stop");
            poll.pollUntil("all EMR Serverless applications stopped", () -> isApplicationStopped(applicationId));
        }
    }

    private boolean allJobsFinished(String applicationId) {
        List<JobRunSummary> unfinishedJobRuns = emrServerlessClient
                .listJobRuns(request -> request.applicationId(applicationId)
                        .states(JobRunState.RUNNING, JobRunState.SCHEDULED, JobRunState.PENDING, JobRunState.SUBMITTED, JobRunState.CANCELLING))
                .jobRuns();

        if (unfinishedJobRuns.isEmpty()) {
            return true;
        } else {
            LOGGER.info("{} jobs are still unfinished for application {}", unfinishedJobRuns.size(), applicationId);
            return false;
        }
    }

    private boolean isApplicationStopped(String applicationId) {

        ApplicationState currentState = emrServerlessClient.getApplication(request -> request.applicationId(applicationId)).application().state();

        Set<ApplicationState> runningApplication = EnumSet.of(ApplicationState.STARTING, ApplicationState.STARTED, ApplicationState.STOPPING);

        if (runningApplication.contains(currentState)) {
            return false;
        }
        return true;

    }

}
