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

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;

import sleeper.core.util.PollWithRetries;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.aResponseWithJobRunWithState;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.aResponseWithNoJobRuns;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.aResponseWithStartedApplication;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.aResponseWithStoppedApplication;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.aResponseWithStoppingApplication;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.aResponseWithTerminatedApplication;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.anyRequestedForEmrServerless;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.cancelJobRunRequest;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.cancelJobRunRequested;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.getApplicationRequest;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.getApplicationRequested;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.listRunningJobsForApplicationRequest;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.listRunningJobsForApplicationRequested;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.listRunningOrCancellingJobsForApplicationRequest;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.listRunningOrCancellingJobsForApplicationRequested;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.stopApplicationRequest;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.stopApplicationRequested;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.wiremockEmrServerlessClient;

@WireMockTest
public class AutoStopEmrServerlessApplicationLambdaIT {

    private AutoStopEmrServerlessApplicationLambda lambda;
    private String applicationId = "test-app-id";
    private String jobRunId = "test-job-run";

    @Test
    void shouldTimeOutWhenDeleting(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // Given
        stubFor(listRunningJobsForApplicationRequest(applicationId)
                .inScenario("ShouldTimeOut")
                .willReturn(aResponseWithStartedApplication(applicationId))
                .willSetStateTo(STARTED));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldTimeOut")
                .willReturn(aResponseWithStartedApplication(applicationId))
                .whenScenarioStateIs(STARTED));
        stubFor(stopApplicationRequest(applicationId)
                .inScenario("ShouldTimeOut")
                .willReturn(aResponse().withStatus(200))
                .whenScenarioStateIs(STARTED)
                .willSetStateTo(ApplicationState.STOPPING.toString()));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldTimeOut")
                .willReturn(aResponseWithStoppingApplication(applicationId))
                .whenScenarioStateIs(ApplicationState.STOPPING.toString()));

        // Then
        assertThatThrownBy(() -> lambda.handleEvent(applicationEvent(applicationId, "Delete"), null))
                .isInstanceOf(PollWithRetries.CheckFailedException.class);
    }

    @Test
    void shouldTrackStoppingApplication(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.immediateRetries(5));

        // Given
        stubFor(listRunningJobsForApplicationRequest(applicationId)
                .inScenario("ShouldTrackStopping")
                .willReturn(aResponseWithNoJobRuns())
                .willSetStateTo(STARTED));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldTrackStopping")
                .willReturn(aResponseWithStartedApplication(applicationId))
                .whenScenarioStateIs(STARTED));
        stubFor(stopApplicationRequest(applicationId)
                .inScenario("ShouldTrackStopping")
                .willReturn(aResponse().withStatus(200))
                .whenScenarioStateIs(STARTED)
                .willSetStateTo(ApplicationState.STOPPING.toString()));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldTrackStopping")
                .willReturn(aResponseWithStoppingApplication(applicationId))
                .whenScenarioStateIs(ApplicationState.STOPPING.toString())
                .willSetStateTo(ApplicationState.STOPPED.toString()));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldTrackStopping")
                .willReturn(aResponseWithStoppedApplication(applicationId))
                .whenScenarioStateIs(ApplicationState.STOPPED.toString()));

        // Then
        lambda.handleEvent(applicationEvent(applicationId, "Delete"), null);
        // Then
        verify(5, anyRequestedForEmrServerless());
        verify(1, stopApplicationRequested(applicationId));
        verify(3, getApplicationRequested(applicationId));

    }

    @Test
    void shouldStopEMRServerlessWhenApplicationIsStartedWithRunningJob(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // Given
        stubFor(listRunningJobsForApplicationRequest(applicationId)
                .inScenario("ShouldStopWithJobs")
                .willReturn(aResponseWithJobRunWithState(applicationId, jobRunId, JobRunState.RUNNING))
                .willSetStateTo(STARTED));
        stubFor(cancelJobRunRequest(applicationId, jobRunId)
                .inScenario("ShouldStopWithJobs")
                .willReturn(ResponseDefinitionBuilder.okForEmptyJson())
                .whenScenarioStateIs(STARTED)
                .willSetStateTo(JobRunState.CANCELLING.toString()));
        stubFor(listRunningOrCancellingJobsForApplicationRequest(applicationId)
                .inScenario("ShouldStopWithJobs")
                .willReturn(aResponseWithNoJobRuns())
                .whenScenarioStateIs(JobRunState.CANCELLING.toString())
                .willSetStateTo(JobRunState.CANCELLED.toString()));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldStopWithJobs")
                .willReturn(aResponseWithStartedApplication(applicationId))
                .whenScenarioStateIs(JobRunState.CANCELLED.toString()));
        stubFor(stopApplicationRequest(applicationId)
                .inScenario("ShouldStopWithJobs")
                .willReturn(aResponse().withStatus(200))
                .whenScenarioStateIs(JobRunState.CANCELLED.toString())
                .willSetStateTo(ApplicationState.STOPPED.toString()));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldStopWithJobs")
                .willReturn(aResponseWithTerminatedApplication(applicationId))
                .whenScenarioStateIs(ApplicationState.STOPPED.toString()));

        // When
        lambda.handleEvent(applicationEvent(applicationId, "Delete"), null);

        // Then
        verify(6, anyRequestedForEmrServerless());
        verify(1, listRunningJobsForApplicationRequested(applicationId));
        verify(1, cancelJobRunRequested(applicationId, jobRunId));
        verify(1, listRunningOrCancellingJobsForApplicationRequested(applicationId));
        verify(1, stopApplicationRequested(applicationId));
        verify(2, getApplicationRequested(applicationId));
    }

    @Test
    void shouldStopEMRServerlessWhenApplicationIsStartedWithNoRunningJobs(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // Given
        stubFor(listRunningJobsForApplicationRequest(applicationId)
                .inScenario("ShouldStopNoJobs")
                .willReturn(aResponseWithNoJobRuns())
                .willSetStateTo(STARTED));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldStopNoJobs")
                .willReturn(aResponseWithStartedApplication(applicationId))
                .whenScenarioStateIs(STARTED));
        stubFor(stopApplicationRequest(applicationId)
                .inScenario("ShouldStopNoJobs")
                .willReturn(aResponse().withStatus(200))
                .whenScenarioStateIs(STARTED)
                .willSetStateTo(ApplicationState.STOPPED.toString()));
        stubFor(getApplicationRequest(applicationId)
                .inScenario("ShouldStopNoJobs")
                .willReturn(aResponseWithTerminatedApplication(applicationId))
                .whenScenarioStateIs(ApplicationState.STOPPED.toString()));

        // When
        lambda.handleEvent(applicationEvent(applicationId, "Delete"), null);

        // Then
        verify(4, anyRequestedForEmrServerless());
        verify(1, listRunningJobsForApplicationRequested(applicationId));
        verify(1, stopApplicationRequested(applicationId));
        verify(2, getApplicationRequested(applicationId));
    }

    @Test
    @DisplayName("Test an invalid event")
    void shouldRaiseExceptionOnInvalidEvent(WireMockRuntimeInfo runtimeInfo) {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // When / Then
        assertThatThrownBy(
                () -> lambda.handleEvent(
                        applicationEvent(applicationId, "TagResource"), null))
                .isInstanceOf(IllegalArgumentException.class);
        verify(0, anyRequestedForEmrServerless());
    }

    private CloudFormationCustomResourceEvent applicationEvent(
            String applicationId, String event) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType(event)
                .withResourceProperties(Map.of("applicationId", applicationId))
                .build();
    }

    private AutoStopEmrServerlessApplicationLambda lambda(WireMockRuntimeInfo runtimeInfo, PollWithRetries poll) {
        return new AutoStopEmrServerlessApplicationLambda(wiremockEmrServerlessClient(runtimeInfo), poll);
    }
}
