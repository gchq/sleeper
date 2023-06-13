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

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.AmazonECSException;
import com.amazonaws.services.ecs.model.InvalidParameterException;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.util.PollWithRetries;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.job.common.WiremockTestHelper.wiremockEcsClient;

@WireMockTest
class RunECSTasksIT {

    private static final String OPERATION_HEADER = "X-Amz-Target";
    private static final StringValuePattern MATCHING_RUN_TASK_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.RunTask");

    private AmazonECS ecsClient;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        ecsClient = wiremockEcsClient(runtimeInfo);
    }

    @Nested
    @DisplayName("Run tasks")
    class RunTasks {

        @BeforeEach
        void setUp() {
            stubResponseStatus(200);
        }

        @Test
        void shouldRunOneTask() {
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(new RunTaskRequest().withCluster("test-cluster"))
                    .numberOfTasksToCreate(1));

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldRunTwoTasks() {
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(new RunTaskRequest().withCluster("test-cluster"))
                    .numberOfTasksToCreate(2));

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 2));
        }

        @Test
        void shouldRunTwoBatches() {
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(new RunTaskRequest().withCluster("test-cluster"))
                    .numberOfTasksToCreate(20));

            verify(2, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldRunTwoBatchesAndTwoMoreTasks() {
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(new RunTaskRequest().withCluster("test-cluster"))
                    .numberOfTasksToCreate(22));

            verify(3, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 2));
        }

        @Test
        void shouldRunMoreTasksThanCanBeCreatedInOneRequest() {
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(new RunTaskRequest().withCluster("test-cluster"))
                    .numberOfTasksToCreate(11));

            verify(2, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldAbortAfterOneRequest() {
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(new RunTaskRequest().withCluster("test-cluster"))
                    .numberOfTasksToCreate(11)
                    .checkAbort(() -> true));

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldConsumeResults() {
            stubResponseStatus(200);
            List<RunTaskResult> results = new ArrayList<>();

            runTasksOrThrow(builder -> builder
                    .runTaskRequest(new RunTaskRequest().withCluster("test-cluster"))
                    .numberOfTasksToCreate(1).resultConsumer(results::add));
            assertThat(results)
                    .containsExactly(new RunTaskResult().withTasks().withFailures());
        }
    }

    @Nested
    @DisplayName("Stop on failure running tasks")
    class StopOnFailure {

        @Test
        void shouldNotMakeASecondRequestWhenFirstOneFailsOnServerAfterRetries() {
            stubResponseStatus(500);

            runTasks(builder -> builder
                    .runTaskRequest(new RunTaskRequest().withCluster("test-cluster"))
                    .numberOfTasksToCreate(11));

            verify(4, anyRequest());
            verify(4, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldNotMakeASecondRequestWhenFirstOneFailsWithInvalidParameter() {
            stubInvalidParameter();

            RunECSTasks.runTasks(ecsClient,
                    new RunTaskRequest().withCluster("test-cluster"), 11);

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldThrowInvalidParameterException() {
            stubInvalidParameter();

            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request, 11))
                    .isInstanceOf(InvalidParameterException.class);
        }

        @Test
        void shouldThrowAmazonClientException() {
            stubResponseStatus(500);

            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request, 11))
                    .isInstanceOf(AmazonECSException.class)
                    .hasFieldOrPropertyWithValue("statusCode", 500);
        }

        @Test
        void shouldThrowECSFailureExceptionIfResponseHasFailures() {
            stubResponseWithFailures();
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request, 1))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldExitEarlyIfResultHasFailures() {
            stubResponseWithFailures();
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request, 20))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldNotConsumeResultsWithFailures() {
            stubResponseWithFailures();
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");
            List<RunTaskResult> results = new ArrayList<>();

            assertThatThrownBy(() -> RunECSTasks.runTasksOrThrow(ecsClient, request,
                    20, results::add))
                    .isInstanceOf(ECSFailureException.class);
            assertThat(results).isEmpty();
        }

        // TODO don't throw ECSFailureException from runTasks without orThrow
    }

    @Nested
    @DisplayName("Retry running tasks")
    class RetryTasks {

        @Test
        void shouldRetryWhenCapacityIsUnavailableThenSuccessfullyRunTask() {
            // Given
            stubFor(runTaskWillReturnCapacityUnavailable()
                    .inScenario("retry capacity")
                    .whenScenarioStateIs(Scenario.STARTED)
                    .willSetStateTo("request-2"));
            stubFor(runTaskWillReturn(aResponse().withStatus(200))
                    .inScenario("retry capacity")
                    .whenScenarioStateIs("request-2"));
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            // When
            builderWithDefaults().runTaskRequest(request).numberOfTasksToCreate(10)
                    .retryWhenNoCapacity(PollWithRetries.intervalAndMaxPolls(0, 2))
                    .build().runTasksOrThrow();

            // Then
            verify(2, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldRetryWhenCapacityIsUnavailableThenFailForOtherReason() {
            // Given
            stubFor(runTaskWillReturnCapacityUnavailable()
                    .inScenario("retry capacity")
                    .whenScenarioStateIs(Scenario.STARTED)
                    .willSetStateTo("request-2"));
            stubFor(runTaskWillReturn(aResponse().withStatus(200)
                    .withBody("{" +
                            "\"failures\":[{" +
                            "\"arn\":\"test-arn\"," +
                            "\"reason\":\"test-reason\"," +
                            "\"detail\":\"test-detail\"" +
                            "}]}"))
                    .inScenario("retry capacity")
                    .whenScenarioStateIs("request-2"));
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            // When/Then
            RunECSTasks run = builderWithDefaults().runTaskRequest(request).numberOfTasksToCreate(10)
                    .retryWhenNoCapacity(PollWithRetries.intervalAndMaxPolls(0, 2)).build();
            assertThatThrownBy(run::runTasksOrThrow)
                    .isInstanceOf(ECSFailureException.class)
                    .hasMessageContaining("test-reason");
            verify(2, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
        }
    }

    private void runTasks(Consumer<RunECSTasks.Builder> configuration) {
        RunECSTasks.Builder builder = builderWithDefaults();
        configuration.accept(builder);
        builder.build().runTasks();
    }

    private void runTasksOrThrow(Consumer<RunECSTasks.Builder> configuration) {
        RunECSTasks.Builder builder = builderWithDefaults();
        configuration.accept(builder);
        builder.build().runTasksOrThrow();
    }

    private RunECSTasks.Builder builderWithDefaults() {
        return RunECSTasks.builder().ecsClient(ecsClient)
                .retryWhenNoCapacity(PollWithRetries.intervalAndMaxPolls(0, 1))
                .sleepForSustainedRatePerSecond(rate -> {
                });
    }

    private static void stubResponseStatus(int status) {
        stubFor(runTaskWillReturn(aResponse().withStatus(status)));
    }

    private static void stubResponseWithFailures() {
        stubFor(runTaskWillReturn(aResponse().withStatus(200)
                .withBody("{" +
                        "\"failures\":[{" +
                        "\"arn\":\"test-arn\"," +
                        "\"reason\":\"test-reason\"," +
                        "\"detail\":\"test-detail\"" +
                        "}]}")));
    }

    private static MappingBuilder runTaskWillReturnCapacityUnavailable() {
        return runTaskWillReturn(aResponse().withStatus(200)
                .withBody("{" +
                        "\"failures\":[{" +
                        "\"reason\":\"Capacity is unavailable at this time. Please try again later or in a different availability zone\"" +
                        "}]}"));
    }

    private static MappingBuilder runTaskWillReturn(ResponseDefinitionBuilder response) {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_RUN_TASK_OPERATION)
                .willReturn(response);
    }

    private static void stubInvalidParameter() {
        stubFor(runTaskWillReturn(aResponse().withStatus(400)
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
