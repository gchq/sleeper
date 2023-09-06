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

package sleeper.clients.teardown;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;
import software.amazon.awssdk.services.emrserverless.model.ApplicationSummary;
import software.amazon.awssdk.services.emrserverless.model.CancelJobRunRequest;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;
import software.amazon.awssdk.services.emrserverless.model.ListJobRunsRequest;
import software.amazon.awssdk.services.emrserverless.model.ListJobRunsResponse;
import software.amazon.awssdk.services.emrserverless.model.StopApplicationRequest;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.clients.util.EmrServerlessUtils.listActiveApplications;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;

public class TerminateEMRServerlessApplications {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(TerminateEMRServerlessApplications.class);
    private static final long POLL_INTERVAL_MILLIS = 30000;
    private static final int MAX_POLLS = 30;

    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS,
            MAX_POLLS);

    private final EmrServerlessClient emrServerlessClient;
    private final String applicationPrefix;


    public TerminateEMRServerlessApplications(EmrServerlessClient emrServerlessClient,
            InstanceProperties properties) {
        this.emrServerlessClient = emrServerlessClient;
        this.applicationPrefix = "sleeper-" + properties.get(ID) + "-";
    }

    public void run() throws InterruptedException {
        List<ApplicationSummary> applications = listActiveApplications(emrServerlessClient)
                .applications();
        List<String> applicationIds = applications.stream()
                .filter(application -> application.name().startsWith(applicationPrefix))
                .map(ApplicationSummary::id).collect(Collectors.toList());
        if (applicationIds.isEmpty()) {
            LOGGER.info("No running applications to terminate");
        } else {
            LOGGER.info("Terminating {} running applications", applicationIds.size());
            stopApplications(applicationIds);
            LOGGER.info("Waiting for applications to terminate");
            pollUntilTerminated();
        }
    }

    private void stopApplications(List<String> applications) {
        List<JobRunState> runningStates = List.of(JobRunState.RUNNING, JobRunState.SCHEDULED, JobRunState.PENDING, JobRunState.SUBMITTED);
        applications.stream().forEach(application -> {
            ListJobRunsResponse jobRunResponse = emrServerlessClient
                    .listJobRuns(ListJobRunsRequest.builder().applicationId(application).build());

            if (jobRunResponse.hasJobRuns()) {
                jobRunResponse.jobRuns().stream().filter(job -> runningStates.contains(job.state()))
                .forEach(jobRun -> {
                    emrServerlessClient.cancelJobRun((CancelJobRunRequest.builder()
                            .applicationId(application).jobRunId(jobRun.id()).build()));
                });
            }
            try {
                pollUntilCancelled(application);
            } catch (InterruptedException e) {
                LOGGER.error("An error has occurred whilst waiting for jobs to cancel", e);
            }
            emrServerlessClient.stopApplication(StopApplicationRequest.builder().applicationId(application).build());
        });
    }

    private void pollUntilTerminated() throws InterruptedException {
        poll.pollUntil("all EMR Serverless applications terminated", this::allApplicationsTerminated);
    }

    private void pollUntilCancelled(String applicationId) throws InterruptedException {
        poll.pollUntil("all EMR Serverless jobs cancelled", () -> allJobsCancelled(applicationId));
    }

    private boolean allApplicationsTerminated() {
        List<ApplicationSummary> applications = listActiveApplications(emrServerlessClient).applications();
        long applicationsStillRunning = applications.stream()
                .filter(application -> application.name().startsWith(applicationPrefix))
                .filter(application -> application.state().equals(ApplicationState.STOPPED)).count();
        LOGGER.info("{} apps are still terminating for instance", applicationsStillRunning);
        return applicationsStillRunning == 0;
    }

    private boolean allJobsCancelled(String applicationId) {
        ListJobRunsResponse jobRunResponse = emrServerlessClient
                    .listJobRuns(ListJobRunsRequest.builder().applicationId(applicationId).build());

        long terminatedJobs = jobRunResponse.jobRuns().stream().filter(jobRun -> jobRun.state().equals(JobRunState.CANCELLED)).count();
        long runningJobs = jobRunResponse.jobRuns().size() - terminatedJobs;
        LOGGER.info("{} jobs are still cancelling for application {}", runningJobs, applicationId);
        return runningJobs == 0;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: <instance id>");
            return;
        }

        String instanceId = args[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        InstanceProperties properties = new InstanceProperties();
        properties.loadFromS3GivenInstanceId(s3Client, instanceId);

        if (properties.getList(OPTIONAL_STACKS).contains("EmrServerlessBulkImportStack")) {
            EmrServerlessClient emrServerlessClient = EmrServerlessClient.create();
            TerminateEMRServerlessApplications terminateApplications = new TerminateEMRServerlessApplications(
                    emrServerlessClient, properties);
            terminateApplications.run();
            emrServerlessClient.close();
        }

        s3Client.shutdown();
    }
}
