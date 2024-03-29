/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.task.common;

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
import static sleeper.task.common.WiremockTestHelper.wiremockEcsClient;

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
            runTasksOrThrow(new RunTaskRequest().withCluster("test-cluster"), 1);

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldRunTwoTasks() {
            runTasksOrThrow(new RunTaskRequest().withCluster("test-cluster"), 2);

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 2));
        }

        @Test
        void shouldRunTwoBatches() {
            runTasksOrThrow(new RunTaskRequest().withCluster("test-cluster"), 20);

            verify(2, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldRunTwoBatchesAndTwoMoreTasks() {
            runTasksOrThrow(new RunTaskRequest().withCluster("test-cluster"), 22);

            verify(3, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 2));
        }

        @Test
        void shouldRunMoreTasksThanCanBeCreatedInOneRequest() {
            runTasksOrThrow(new RunTaskRequest().withCluster("test-cluster"), 11);

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

            runTasks(new RunTaskRequest().withCluster("test-cluster"), 11);

            verify(4, anyRequest());
            verify(4, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldNotMakeASecondRequestWhenFirstOneFailsWithInvalidParameter() {
            stubInvalidParameter();

            runTasks(new RunTaskRequest().withCluster("test-cluster"), 11);

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldThrowInvalidParameterException() {
            stubInvalidParameter();

            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            assertThatThrownBy(() -> runTasksOrThrow(request, 11))
                    .isInstanceOf(InvalidParameterException.class);
        }

        @Test
        void shouldThrowAmazonClientException() {
            stubResponseStatus(500);

            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            assertThatThrownBy(() -> runTasksOrThrow(request, 11))
                    .isInstanceOf(AmazonECSException.class)
                    .hasFieldOrPropertyWithValue("statusCode", 500);
        }

        @Test
        void shouldThrowECSFailureExceptionIfResponseHasFailures() {
            stubFor(runTaskWillReturnFatalFailure());
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            assertThatThrownBy(() -> runTasksOrThrow(request, 1))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldExitEarlyIfResultHasFailures() {
            stubFor(runTaskWillReturnFatalFailure());
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            assertThatThrownBy(() -> runTasksOrThrow(request, 20))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldConsumeResultsWithFailures() {
            stubFor(runTaskWillReturnFatalFailure());
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");
            List<RunTaskResult> results = new ArrayList<>();

            Consumer<RunECSTasks.Builder> configuration = builder -> builder
                    .runTaskRequest(request)
                    .numberOfTasksToCreate(20)
                    .resultConsumer(results::add);
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(ECSFailureException.class);
            assertThat(results).hasSize(1);
        }

        @Test
        void shouldEndOnFailureIfNotThrowing() {
            stubFor(runTaskWillReturnFatalFailure());
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            runTasks(request, 20);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }
    }

    @Nested
    @DisplayName("Retry running tasks when capacity is unavailable")
    class RetryTasksWaitingForCapacity {

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
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(request).numberOfTasksToCreate(10)
                    .retryWhenNoCapacity(PollWithRetries.immediateRetries(1)));

            // Then
            verify(2, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldRetryWhenCapacityIsUnavailableThenFailForOtherReason() {
            // Given
            stubFor(runTaskWillReturnCapacityUnavailable()
                    .inScenario("retry capacity")
                    .whenScenarioStateIs(Scenario.STARTED)
                    .willSetStateTo("request-2"));
            stubFor(runTaskWillReturnFatalFailure()
                    .inScenario("retry capacity")
                    .whenScenarioStateIs("request-2"));
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            // When/Then
            Consumer<RunECSTasks.Builder> configuration = builder -> builder
                    .runTaskRequest(request).numberOfTasksToCreate(10)
                    .retryWhenNoCapacity(PollWithRetries.immediateRetries(1));
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(ECSFailureException.class)
                    .hasMessageContaining("test-reason");
            verify(2, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldConsumeSuccessfulTaskRequestsWhenSomeRequestsFailedDueToUnavailableCapacity() {
            // Given
            stubFor(runTaskWillReturnCapacityUnavailable());
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");
            List<RunTaskResult> results = new ArrayList<>();

            // When
            runTasks(builder -> builder
                    .runTaskRequest(request).numberOfTasksToCreate(10)
                    .retryWhenNoCapacity(PollWithRetries.noRetries())
                    .resultConsumer(results::add));

            // Then
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            assertThat(results).hasSize(1);
        }

        @Test
        void shouldFailWhenTerminalFailureAndCapacityUnavailableHappenedInSameRequest() {
            // Given
            stubFor(runTaskWillReturnCapacityUnavailableAndOtherFailure());
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");
            List<RunTaskResult> results = new ArrayList<>();

            // When
            Consumer<RunECSTasks.Builder> configuration = builder -> builder
                    .runTaskRequest(request).numberOfTasksToCreate(10)
                    .retryWhenNoCapacity(PollWithRetries.noRetries())
                    .resultConsumer(results::add);
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(ECSFailureException.class);

            // Then
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            assertThat(results).hasSize(1);
        }

        @Test
        void shouldAbortBetweenCapacityUnavailableRequests() {
            // Given
            stubFor(runTaskWillReturnCapacityUnavailable());
            RunTaskRequest request = new RunTaskRequest().withCluster("test-cluster");

            // When
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(request).numberOfTasksToCreate(10)
                    .checkAbort(() -> true));

            // Then
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldRetryAFullBatchWhenCapacityUnavailableAndAFullBatchIsLeftToCreate() {
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
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(request).numberOfTasksToCreate(19)
                    .retryWhenNoCapacity(PollWithRetries.immediateRetries(1)));

            // Then
            verify(2, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
        }
    }

    private void runTasks(RunTaskRequest request, int numberOfTasksToCreate) {
        runTasks(builder -> builder
                .runTaskRequest(request)
                .numberOfTasksToCreate(numberOfTasksToCreate));
    }

    private void runTasks(Consumer<RunECSTasks.Builder> configuration) {
        RunECSTasks.runTasks(builder -> {
            setDefaults(builder);
            configuration.accept(builder);
        });
    }

    private void runTasksOrThrow(RunTaskRequest request, int numberOfTasksToCreate) {
        runTasksOrThrow(builder -> builder
                .runTaskRequest(request)
                .numberOfTasksToCreate(numberOfTasksToCreate));
    }

    private void runTasksOrThrow(Consumer<RunECSTasks.Builder> configuration) {
        RunECSTasks.runTasksOrThrow(builder -> {
            setDefaults(builder);
            configuration.accept(builder);
        });
    }

    private void setDefaults(RunECSTasks.Builder builder) {
        builder.ecsClient(ecsClient)
                .retryWhenNoCapacity(PollWithRetries.noRetries())
                .sleepForSustainedRatePerSecond(rate -> {
                });
    }

    private static void stubResponseStatus(int status) {
        stubFor(runTaskWillReturn(aResponse().withStatus(status)));
    }

    private static MappingBuilder runTaskWillReturnFatalFailure() {
        return runTaskWillReturn(aResponse().withStatus(200)
                .withBody("{" +
                        "\"failures\":[{" +
                        "\"arn\":\"test-arn\"," +
                        "\"reason\":\"test-reason\"," +
                        "\"detail\":\"test-detail\"" +
                        "}]}"));
    }

    private static MappingBuilder runTaskWillReturnCapacityUnavailable() {
        return runTaskWillReturn(aResponse().withStatus(200)
                .withBody("{" +
                        "\"failures\":[{" +
                        "\"reason\":\"Capacity is unavailable at this time. Please try again later or in a different availability zone\"" +
                        "}]}"));
    }

    private static MappingBuilder runTaskWillReturnCapacityUnavailableAndOtherFailure() {
        return runTaskWillReturn(aResponse().withStatus(200)
                .withBody("{\"failures\":[" +
                        "{" +
                        "\"reason\":\"Capacity is unavailable at this time. Please try again later or in a different availability zone\"" +
                        "},{" +
                        "\"arn\":\"test-arn\"," +
                        "\"reason\":\"test-reason\"," +
                        "\"detail\":\"test-detail\"" +
                        "}" +
                        "]}"));
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
