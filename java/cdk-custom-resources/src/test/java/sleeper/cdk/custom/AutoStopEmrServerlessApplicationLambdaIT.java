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
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.MATCHING_CREATE_APPLICATION_OPERATION;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.MATCHING_LIST_APPLICATIONS_OPERATION;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.MATCHING_STOP_APPLICATION_OPERATION;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.MATCHING_UPDATE_APPLICATION_OPERATION;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.OPERATION_HEADER;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.anyRequestedForEmrServerless;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.createApplicationRequestedFor;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.stopApplicationRequestedFor;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.updateApplicationRequestedFor;
import static sleeper.cdk.custom.WiremockEmrServerlessTestHelper.wiremockEmrServerlessClient;

@WireMockTest
public class AutoStopEmrServerlessApplicationLambdaIT {

    private AutoStopEmrServerlessApplicationLambda lambda;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        lambda = lambda(runtimeInfo);
    }

    @Test
    @DisplayName("Stop EMR Serverless application")
    void shouldStopEmrServerlessApplication() throws InterruptedException {

        // Given
        String applicationId = UUID.randomUUID().toString();
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_LIST_APPLICATIONS_OPERATION)
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody("{\"nextToken\":null,\"applications\":[\"ApplicationList\"{\"id\":\"test-application\"}]}")));
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_STOP_APPLICATION_OPERATION)
                        .willReturn(aResponse().withStatus(200)));

        // When
        lambda.handleEvent(eventHandlerForApplication(applicationId, "StopApplication"), null);

        // Then
        verify(2, anyRequestedForEmrServerless());
        verify(1, stopApplicationRequestedFor("test-application"));
    }

    @Test
    @DisplayName("Create EMR Serverless Application")
    void shouldTakeNoActionOnCreateEmrServlessApplication() throws InterruptedException {

        // Given
        String applicationName = UUID.randomUUID().toString();
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_CREATE_APPLICATION_OPERATION)
                        .willReturn(aResponse().withStatus(200)));

        // When
        lambda.handleEvent(eventHandlerForApplication(applicationName, "CreateApplication"), null);

        // Then
        verify(0, createApplicationRequestedFor(applicationName));
    }

    @Test
    @DisplayName("Update EMR Serverless Application")
    void shouldTakeNoActionOnUpdateEmrApplication() throws InterruptedException {

        // Given
        String applicationId = UUID.randomUUID().toString();
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_UPDATE_APPLICATION_OPERATION)
                        .willReturn(aResponse().withStatus(200)));

        // When
        lambda.handleEvent(eventHandlerForApplication(applicationId, "Update"), null);

        // Then
        verify(0, updateApplicationRequestedFor(applicationId));
    }

    @Test
    @DisplayName("Test unsupported operation")
    void shouldRaiseExceptionOnUnsupportedOperation() {

        // Given
        String applicationId = UUID.randomUUID().toString();

        // When / Then
        verify(0, anyRequestedForEmrServerless());
        assertThatThrownBy(
                () -> lambda.handleEvent(
                        eventHandlerForApplication(applicationId, "TagResource"), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private CloudFormationCustomResourceEvent eventHandlerForApplication(
            String applicationId, String event) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType(event)
                .withResourceProperties(Map.of("applicationId", applicationId))
                .build();
    }

    private AutoStopEmrServerlessApplicationLambda lambda(WireMockRuntimeInfo runtimeInfo) {
        return new AutoStopEmrServerlessApplicationLambda(wiremockEmrServerlessClient(runtimeInfo));
    }
}
