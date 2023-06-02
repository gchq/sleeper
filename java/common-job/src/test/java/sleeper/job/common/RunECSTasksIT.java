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
package sleeper.job.common;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.InvalidParameterException;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.job.common.WiremockTestHelper.wiremockEcsClient;

@WireMockTest
class RunECSTasksIT {

    private static final String OPERATION_HEADER = "X-Amz-Target";
    private static final StringValuePattern MATCHING_RUN_TASK_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.RunTask");

    @Nested
    @DisplayName("Run tasks")
    class RunTasks {

        @BeforeEach
        void setUp() {
            stubResponseStatus(200);
        }

        @Test
        void shouldRunOneTask(WireMockRuntimeInfo runtimeInfo) {

            RunECSTasks.runTasksOrThrow(wiremockEcsClient(runtimeInfo),
                    new RunTaskRequest()
                            .withCluster("test-cluster"),
                    1);

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldRunTwoTasks(WireMockRuntimeInfo runtimeInfo) {

            RunECSTasks.runTasksOrThrow(wiremockEcsClient(runtimeInfo),
                    new RunTaskRequest()
                            .withCluster("test-cluster"),
                    2);

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 2));
        }

        @Test
        void shouldRunTwoBatches(WireMockRuntimeInfo runtimeInfo) {

            RunECSTasks.runTasksOrThrow(wiremockEcsClient(runtimeInfo),
                    new RunTaskRequest()
                            .withCluster("test-cluster"),
                    20);

            verify(2, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldRunTwoBatchesAndTwoMoreTasks(WireMockRuntimeInfo runtimeInfo) {

            RunECSTasks.runTasksOrThrow(wiremockEcsClient(runtimeInfo),
                    new RunTaskRequest()
                            .withCluster("test-cluster"),
                    22);

            verify(3, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 2));
        }

        @Test
        void shouldRunMoreTasksThanCanBeCreatedInOneRequest(WireMockRuntimeInfo runtimeInfo) {

            RunECSTasks.runTasksOrThrow(wiremockEcsClient(runtimeInfo),
                    new RunTaskRequest()
                            .withCluster("test-cluster"),
                    11);

            verify(2, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldAbortAfterOneRequest(WireMockRuntimeInfo runtimeInfo) {

            RunECSTasks.runTasksOrThrow(wiremockEcsClient(runtimeInfo),
                    new RunTaskRequest()
                            .withCluster("test-cluster"),
                    11, () -> true);

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }
    }

    @Nested
    @DisplayName("Stop on failure running tasks")
    class StopOnFailure {

        @Test
        void shouldNotMakeASecondRequestWhenFirstOneFailsOnServerAfterRetries(WireMockRuntimeInfo runtimeInfo) {
            stubResponseStatus(500);

            RunECSTasks.runTasks(wiremockEcsClient(runtimeInfo),
                    new RunTaskRequest()
                            .withCluster("test-cluster"),
                    11);

            verify(4, anyRequest());
            verify(4, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldNotMakeASecondRequestWhenFirstOneFailsWithInvalidParameter(WireMockRuntimeInfo runtimeInfo) {
            stubInvalidParameter();

            RunECSTasks.runTasks(wiremockEcsClient(runtimeInfo),
                    new RunTaskRequest()
                            .withCluster("test-cluster"),
                    11);

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldThrowInvalidParameterException(WireMockRuntimeInfo runtimeInfo) {
            stubInvalidParameter();

            AmazonECS ecsClient = wiremockEcsClient(runtimeInfo);
            RunTaskRequest request = new RunTaskRequest()
                    .withCluster("test-cluster");

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request, 11))
                    .isInstanceOf(InvalidParameterException.class);
        }

        @Test
        void shouldThrowAmazonClientException(WireMockRuntimeInfo runtimeInfo) {
            stubResponseStatus(500);

            AmazonECS ecsClient = wiremockEcsClient(runtimeInfo);
            RunTaskRequest request = new RunTaskRequest()
                    .withCluster("test-cluster");

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request, 11))
                    .isInstanceOf(AmazonClientException.class);
        }

        @Test
        void shouldThrowECSFailureExceptionIfResponseHasFailures(WireMockRuntimeInfo runtimeInfo) {
            stubResponseWithFailures();
            AmazonECS ecsClient = wiremockEcsClient(runtimeInfo);
            RunTaskRequest request = new RunTaskRequest()
                    .withCluster("test-cluster");

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request, 1))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldExitEarlyIfResponseHasFailures(WireMockRuntimeInfo runtimeInfo) {
            stubResponseWithFailures();
            AmazonECS ecsClient = wiremockEcsClient(runtimeInfo);
            RunTaskRequest request = new RunTaskRequest()
                    .withCluster("test-cluster");

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request, 20))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }
    }

    private static void stubResponseStatus(int status) {
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_RUN_TASK_OPERATION)
                .willReturn(aResponse().withStatus(status)));
    }

    private static void stubResponseWithFailures() {
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_RUN_TASK_OPERATION)
                .willReturn(aResponse().withStatus(200)
                        .withBody("{" +
                                "\"failures\":[{" +
                                "\"arn\":\"test-arn\"," +
                                "\"reason\":\"test-reason\"," +
                                "\"detail\":\"test-detail\"" +
                                "}]}")));
    }

    private static void stubInvalidParameter() {
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_RUN_TASK_OPERATION)
                .willReturn(aResponse().withStatus(400)
                        .withHeader("x-amzn-ErrorType", "InvalidParameterException")));
    }

    private static RequestPatternBuilder anyRequest() {
        return postRequestedFor(urlEqualTo("/"));
    }

    private static RequestPatternBuilder runTasksRequestedFor(String clusterName, int count) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_RUN_TASK_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName))
                        .and(matchingJsonPath("$.count", equalTo("" + count))));
    }
}
