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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.aResponseWithJobRunWithState;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.aResponseWithNoJobRuns;
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
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

@WireMockTest
public class AutoStopEmrServerlessApplicationLambdaIT {

    private AutoStopEmrServerlessApplicationLambda lambda;
    private final InstanceProperties properties = createTestInstanceProperties();

    @BeforeEach
    void setUp() {
        properties.set(ID, "test-app-id");
    }

    @Test
    void shouldStopEMRServerlessWhenApplicationIsStartedWithRunningJob(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // Given
        stubFor(listRunningJobsForApplicationRequest().inScenario("StopJob")
                .willReturn(aResponseWithJobRunWithState("test-job-run", JobRunState.RUNNING))
                .whenScenarioStateIs(STARTED));
        stubFor(cancelJobRunRequest("test-job-run").inScenario("StopJob")
                .willReturn(ResponseDefinitionBuilder.okForEmptyJson())
                .whenScenarioStateIs(STARTED).willSetStateTo("JobStopped"));
        stubFor(listRunningOrCancellingJobsForApplicationRequest().inScenario("StopJob")
                .willReturn(aResponseWithNoJobRuns())
                .whenScenarioStateIs("JobStopped"));
        stubFor(stopApplicationRequest().inScenario("StopJob")
                .willReturn(aResponse().withStatus(200))
                .whenScenarioStateIs("JobStopped").willSetStateTo("AppStopped"));
        stubFor(getApplicationRequest().inScenario("StopJob")
                .willReturn(aResponseWithTerminatedApplication())
                .whenScenarioStateIs("AppStopped"));

        // When
        lambda.handleEvent(eventHandlerForApplication(properties.get(ID), "Delete"), null);

        // Then
        verify(5, anyRequestedForEmrServerless());
        verify(1, listRunningJobsForApplicationRequested());
        verify(1, cancelJobRunRequested("test-job-run"));
        verify(1, listRunningOrCancellingJobsForApplicationRequested());
        verify(1, stopApplicationRequested());
        verify(1, getApplicationRequested());
    }

    @Test
    void shouldStopEMRServerlessWhenApplicationIsStartedWithNoRunningJobs(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // Given
        stubFor(listRunningJobsForApplicationRequest()
                .willReturn(aResponseWithNoJobRuns()));
        stubFor(stopApplicationRequest().inScenario("StopApplication")
                .willReturn(aResponse().withStatus(200))
                .whenScenarioStateIs(STARTED).willSetStateTo("ApplicationStopped"));
        stubFor(getApplicationRequest().inScenario("StopApplication")
                .willReturn(aResponseWithTerminatedApplication())
                .whenScenarioStateIs("ApplicationStopped"));

        // When
        lambda.handleEvent(eventHandlerForApplication(properties.get(ID), "Delete"), null);

        // Then
        verify(3, anyRequestedForEmrServerless());
        verify(1, listRunningJobsForApplicationRequested());
        verify(1, stopApplicationRequested());
        verify(1, getApplicationRequested());
    }

    @Test
    @DisplayName("Test unsupported operation")
    void shouldRaiseExceptionOnUnsupportedOperation(WireMockRuntimeInfo runtimeInfo) {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // When / Then
        verify(0, anyRequestedForEmrServerless());
        assertThatThrownBy(
                () -> lambda.handleEvent(
                        eventHandlerForApplication(properties.get(ID), "TagResource"), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private CloudFormationCustomResourceEvent eventHandlerForApplication(
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
