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
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.EcsException;
import software.amazon.awssdk.services.ecs.model.InvalidParameterException;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;

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
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.task.common.WiremockTestHelper.wiremockEcsClient;

@WireMockTest
class RunECSTasksIT {

    private static final String OPERATION_HEADER = "X-Amz-Target";
    private static final StringValuePattern MATCHING_RUN_TASK_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.RunTask");

    private EcsClient ecsClient;

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
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(1));

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldRunTwoTasks() {
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(2));

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 2));
        }

        @Test
        void shouldRunTwoBatches() {
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(20));

            verify(2, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldRunTwoBatchesAndTwoMoreTasks() {
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(22));

            verify(3, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 2));
        }

        @Test
        void shouldRunMoreTasksThanCanBeCreatedInOneRequest() {
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(11));

            verify(2, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldAbortAfterOneRequest() {
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(11)
                    .checkAbort(() -> true));

            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldConsumeResults() {
            stubResponseStatus(200);
            List<RunTaskResponse> results = new ArrayList<>();

            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(1).responseConsumer(results::add));
            assertThat(results)
                    .extracting(RunTaskResponse::tasks, RunTaskResponse::failures)
                    .containsExactly(tuple(List.of(), List.of()));
        }

        @Test
        void shouldRunNoTasks() {
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(0));

            verify(0, anyRequest());
        }
    }

    @Nested
    @DisplayName("Stop on failure running tasks")
    class StopOnFailure {

        @Test
        void shouldNotMakeASecondRequestWhenFirstOneFailsOnServerAfterRetries() {
            // Given
            stubResponseStatus(500);

            // When
            runTasks(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(11));

            // Then
            verify(4, anyRequest());
            verify(4, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldNotMakeASecondRequestWhenFirstOneFailsWithInvalidParameter() {
            // Given
            stubInvalidParameter();

            // When
            runTasks(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(11));

            // Then
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldThrowInvalidParameterException() {
            // Given
            stubInvalidParameter();
            Consumer<RunECSTasks.Builder> configuration = runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(11);

            // When / Then
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(InvalidParameterException.class);
        }

        @Test
        void shouldThrowAmazonClientException() {
            // Given
            stubResponseStatus(500);
            Consumer<RunECSTasks.Builder> configuration = runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(11);

            // When / Then
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(EcsException.class)
                    .hasFieldOrPropertyWithValue("statusCode", 500);
        }

        @Test
        void shouldThrowECSFailureExceptionIfResponseHasFailures() {
            // Given
            stubFor(runTaskWillReturnFatalFailure());
            Consumer<RunECSTasks.Builder> configuration = runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(1);

            // When / Then
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 1));
        }

        @Test
        void shouldExitEarlyIfResultHasFailures() {
            // Given
            stubFor(runTaskWillReturnFatalFailure());
            Consumer<RunECSTasks.Builder> configuration = runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(20);

            // When / Then
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
        }

        @Test
        void shouldConsumeResultsWithFailures() {
            // Given
            stubFor(runTaskWillReturnFatalFailure());
            List<RunTaskResponse> responses = new ArrayList<>();
            Consumer<RunECSTasks.Builder> configuration = runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(20)
                    .responseConsumer(responses::add);

            // When / Then
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(ECSFailureException.class);
            assertThat(responses).hasSize(1);
        }

        @Test
        void shouldEndOnFailureIfNotThrowing() {
            // Given
            stubFor(runTaskWillReturnFatalFailure());

            // When
            runTasks(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(11));

            // Then
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

            // When
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(10)
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

            // When/Then
            Consumer<RunECSTasks.Builder> configuration = runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(10)
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
            List<RunTaskResponse> responses = new ArrayList<>();

            // When
            runTasks(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(10)
                    .retryWhenNoCapacity(PollWithRetries.noRetries())
                    .responseConsumer(responses::add));

            // Then
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            assertThat(responses).hasSize(1);
        }

        @Test
        void shouldFailWhenTerminalFailureAndCapacityUnavailableHappenedInSameRequest() {
            // Given
            stubFor(runTaskWillReturnCapacityUnavailableAndOtherFailure());
            List<RunTaskResponse> responses = new ArrayList<>();
            Consumer<RunECSTasks.Builder> configuration = runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(10)
                    .retryWhenNoCapacity(PollWithRetries.noRetries())
                    .responseConsumer(responses::add);

            // When / Then
            assertThatThrownBy(() -> runTasksOrThrow(configuration))
                    .isInstanceOf(ECSFailureException.class);
            verify(1, anyRequest());
            verify(1, runTasksRequestedFor("test-cluster", 10));
            assertThat(responses).hasSize(1);
        }

        @Test
        void shouldAbortBetweenCapacityUnavailableRequests() {
            // Given
            stubFor(runTaskWillReturnCapacityUnavailable());

            // When
            runTasksOrThrow(runTasks -> runTasks
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(10)
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

            // When
            runTasksOrThrow(builder -> builder
                    .runTaskRequest(request -> request.cluster("test-cluster"))
                    .numberOfTasksToCreate(19)
                    .retryWhenNoCapacity(PollWithRetries.immediateRetries(1)));

            // Then
            verify(2, anyRequest());
            verify(2, runTasksRequestedFor("test-cluster", 10));
        }
    }

    private void runTasks(Consumer<RunECSTasks.Builder> configuration) {
        RunECSTasks.runTasks(builder -> {
            setDefaults(builder);
            configuration.accept(builder);
        });
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
