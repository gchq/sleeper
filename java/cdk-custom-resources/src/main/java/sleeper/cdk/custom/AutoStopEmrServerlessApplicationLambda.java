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
import software.amazon.awssdk.services.emrserverless.model.ApplicationSummary;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;
import software.amazon.awssdk.services.emrserverless.model.JobRunSummary;

import sleeper.core.util.PollWithRetries;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Delete an EMR Serverless application.
 */
public class AutoStopEmrServerlessApplicationLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoStopEmrServerlessApplicationLambda.class);

    private final PollWithRetries poll = PollWithRetries
            .intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(15));
    private final EmrServerlessClient emrServerlessClient;
    private String applicationPrefix;

    public AutoStopEmrServerlessApplicationLambda() {
        this(EmrServerlessClient.create());
    }

    public AutoStopEmrServerlessApplicationLambda(EmrServerlessClient emrServerlessClient) {
        this.emrServerlessClient = emrServerlessClient;
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
        this.applicationPrefix = "sleeper-" + (String) resourceProperties.get("instanceId");

        switch (event.getRequestType()) {
            case "Create":
                break;
            case "Update":
                break;
            case "Delete":
                stopEmrServerlessApplication(emrServerlessClient);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void stopEmrServerlessApplication(EmrServerlessClient emrServerless) throws InterruptedException {
        this.run();
    }

    private void run() throws InterruptedException {
        List<ApplicationSummary> applications = listActiveApplications();
        if (applications.isEmpty()) {
            LOGGER.info("No running applications to terminate");
        } else {
            LOGGER.info("Terminating {} running applications", applications.size());
            stopApplications(applications);
            LOGGER.info("Waiting for applications to terminate");
            poll.pollUntil("all EMR Serverless applications terminated", this::allApplicationsTerminated);
        }
    }

    private void stopApplications(List<ApplicationSummary> applications) throws InterruptedException {
        for (ApplicationSummary application : applications) {
            List<JobRunSummary> jobRuns = emrServerlessClient.listJobRuns(request -> request.applicationId(application.id())
                    .states(JobRunState.RUNNING, JobRunState.SCHEDULED, JobRunState.PENDING, JobRunState.SUBMITTED))
                    .jobRuns();

            jobRuns.forEach(jobRun -> emrServerlessClient.cancelJobRun(request -> request
                    .applicationId(application.id()).jobRunId(jobRun.id())));

            if (!jobRuns.isEmpty()) {
                poll.pollUntil("all EMR Serverless jobs finished", () -> allJobsFinished(application.id()));
            }

            emrServerlessClient.stopApplication(request -> request.applicationId(application.id()));
        }
    }

    private boolean allApplicationsTerminated() {
        long applicationsStillRunning = listActiveApplications().size();
        LOGGER.info("{} apps are still terminating for instance", applicationsStillRunning);
        return applicationsStillRunning == 0;
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

    private List<ApplicationSummary> listActiveApplications() {
        return emrServerlessClient.listApplications(request -> request.states(
                ApplicationState.STARTING, ApplicationState.STARTED, ApplicationState.STOPPING))
                .applications().stream()
                .filter(summary -> summary.name().startsWith(applicationPrefix))
                .collect(Collectors.toUnmodifiableList());
    }

}
