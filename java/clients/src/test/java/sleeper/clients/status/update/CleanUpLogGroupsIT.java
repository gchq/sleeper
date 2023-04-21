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
package sleeper.clients.status.update;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static com.amazonaws.services.s3.Headers.CONTENT_TYPE;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockCloudFormationClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockLogsClient;

class CleanUpLogGroupsIT {

    @Nested
    @DisplayName("Delete empty groups not matching a deployed CloudFormation stack")
    @WireMockTest
    class DeleteEmptyLogGroups {

        @Test
        void shouldDeleteAnEmptyLogGroupWhenNoStacksArePresent(WireMockRuntimeInfo runtimeInfo) {
            // Given
            stubFor(listActiveStacksRequest().willReturn(aResponse().withStatus(200)));
            stubFor(describeLogGroupsRequest().willReturn(aResponse().withStatus(200)
                    .withBody("{\"logGroups\": [{" +
                            "\"logGroupName\": \"test-log-group\"," +
                            "\"storedBytes\": 0," +
                            "\"retentionInDays\": 1" +
                            "}]}")));
            stubFor(deleteLogGroupRequestWithName("test-log-group").willReturn(aResponse().withStatus(200)));

            // When
            cleanUpLogGroups(runtimeInfo);

            // Then
            verify(1, deleteLogGroupRequestedWithName("test-log-group"));
        }

        @Test
        void shouldNotDeleteAnEmptyLogGroupWhenItsNameContainsAStackName(WireMockRuntimeInfo runtimeInfo) {
            // Given
            stubFor(listActiveStacksRequest().willReturn(aResponseWithStackName("test-stack")));
            stubFor(describeLogGroupsRequest().willReturn(aResponse().withStatus(200)
                    .withBody("{\"logGroups\": [{" +
                            "\"logGroupName\": \"test-stack-log-group\"," +
                            "\"storedBytes\": 0," +
                            "\"retentionInDays\": 1" +
                            "}]}")));

            // When
            cleanUpLogGroups(runtimeInfo);

            // Then
            verify(0, deleteLogGroupRequested());
        }

        @Test
        void shouldDeleteAnEmptyLogGroupWhenItsNameContainsANestedStackName(WireMockRuntimeInfo runtimeInfo) {
            // Given
            stubFor(listActiveStacksRequest().willReturn(aResponseWithNestedStackName("test-stack")));
            stubFor(describeLogGroupsRequest().willReturn(aResponse().withStatus(200)
                    .withBody("{\"logGroups\": [{" +
                            "\"logGroupName\": \"test-log-group\"," +
                            "\"storedBytes\": 0," +
                            "\"retentionInDays\": 1" +
                            "}]}")));
            stubFor(deleteLogGroupRequestWithName("test-log-group").willReturn(aResponse().withStatus(200)));

            // When
            cleanUpLogGroups(runtimeInfo);

            // Then
            verify(1, deleteLogGroupRequestedWithName("test-log-group"));
        }

        @Test
        void shouldNotDeleteALogGroupThatIsNotEmpty(WireMockRuntimeInfo runtimeInfo) {
            // Given
            stubFor(listActiveStacksRequest().willReturn(aResponse().withStatus(200)));
            stubFor(describeLogGroupsRequest().willReturn(aResponse().withStatus(200)
                    .withBody("{\"logGroups\": [{" +
                            "\"logGroupName\": \"test-log-group\"," +
                            "\"storedBytes\": 1," +
                            "\"retentionInDays\": 1" +
                            "}]}")));

            // When
            cleanUpLogGroups(runtimeInfo);

            // Then
            verify(0, deleteLogGroupRequested());
        }
    }

    @Nested
    @DisplayName("Delete old groups with no retention period not matching a deployed CloudFormation stack")
    @WireMockTest
    class DeleteOldGroupsWithNoRetentionPeriod {

        @Test
        void shouldDeleteALogGroupWithoutARetentionPeriodWhenOlderThanAMonthAndNotEmpty(WireMockRuntimeInfo runtimeInfo) {
            // Given
            Instant queryTime = Instant.parse("2023-04-12T11:26:00Z");
            Instant createdTime = Instant.parse("2023-01-12T11:26:00Z");
            stubFor(listActiveStacksRequest().willReturn(aResponse().withStatus(200)));
            stubFor(describeLogGroupsRequest().willReturn(aResponse().withStatus(200)
                    .withBody("{\"logGroups\": [{" +
                            "\"logGroupName\": \"test-log-group\"," +
                            "\"storedBytes\": 1," +
                            "\"creationTime\": " + createdTime.toEpochMilli() +
                            "}]}")));
            stubFor(deleteLogGroupRequestWithName("test-log-group").willReturn(aResponse().withStatus(200)));

            // When
            cleanUpLogGroups(runtimeInfo, queryTime);

            // Then
            verify(1, deleteLogGroupRequestedWithName("test-log-group"));
        }

        @Test
        void shouldNotDeleteALogGroupWithoutARetentionPeriodWhenYoungerThanAMonthAndNotEmpty(WireMockRuntimeInfo runtimeInfo) {
            // Given
            Instant queryTime = Instant.parse("2023-04-12T11:26:00Z");
            Instant createdTime = Instant.parse("2023-04-11T11:26:00Z");
            stubFor(listActiveStacksRequest().willReturn(aResponse().withStatus(200)));
            stubFor(describeLogGroupsRequest().willReturn(aResponse().withStatus(200)
                    .withBody("{\"logGroups\": [{" +
                            "\"logGroupName\": \"test-log-group\"," +
                            "\"storedBytes\": 1," +
                            "\"creationTime\": " + createdTime.toEpochMilli() +
                            "}]}")));

            // When
            cleanUpLogGroups(runtimeInfo, queryTime);

            // Then
            verify(0, deleteLogGroupRequested());
        }

        @Test
        void shouldNotDeleteALogGroupWithoutARetentionPeriodWhenOlderThanAMonthAndContainsStackName(WireMockRuntimeInfo runtimeInfo) {
            // Given
            Instant queryTime = Instant.parse("2023-04-12T11:26:00Z");
            Instant createdTime = Instant.parse("2023-01-12T11:26:00Z");
            stubFor(listActiveStacksRequest().willReturn(aResponseWithStackName("test-stack")));
            stubFor(describeLogGroupsRequest().willReturn(aResponse().withStatus(200)
                    .withBody("{\"logGroups\": [{" +
                            "\"logGroupName\": \"test-stack-log-group\"," +
                            "\"storedBytes\": 1," +
                            "\"creationTime\": " + createdTime.toEpochMilli() +
                            "}]}")));

            // When
            cleanUpLogGroups(runtimeInfo, queryTime);

            // Then
            verify(0, deleteLogGroupRequested());
        }
    }

    @Nested
    @DisplayName("Delete multiple log groups")
    @WireMockTest
    class DeleteMultipleGroups {

        @Test
        void shouldDeleteEmptyLogGroupAndOldLogGroupWithNoRetentionPeriod(WireMockRuntimeInfo runtimeInfo) {
            // Given
            Instant queryTime = Instant.parse("2023-04-12T11:26:00Z");
            Instant createdTime = Instant.parse("2023-01-12T11:26:00Z");
            stubFor(listActiveStacksRequest().willReturn(aResponse().withStatus(200)));
            stubFor(describeLogGroupsRequest().willReturn(aResponse().withStatus(200)
                    .withBody("{\"logGroups\": [{" +
                            "\"logGroupName\": \"empty-log-group\"," +
                            "\"storedBytes\": 0," +
                            "\"retentionInDays\": 1" +
                            "},{" +
                            "\"logGroupName\": \"old-log-group\"," +
                            "\"storedBytes\": 1," +
                            "\"creationTime\": " + createdTime.toEpochMilli() +
                            "}]}")));
            stubFor(deleteLogGroupRequestWithName(equalTo("empty-log-group").or(equalTo("old-log-group")))
                    .willReturn(aResponse().withStatus(200)));

            // When
            cleanUpLogGroups(runtimeInfo, queryTime);

            // Then
            verify(1, deleteLogGroupRequestedWithName("empty-log-group"));
            verify(1, deleteLogGroupRequestedWithName("old-log-group"));
        }
    }

    private static void cleanUpLogGroups(WireMockRuntimeInfo runtimeInfo) {
        cleanUpLogGroups(runtimeInfo, Instant.now());
    }

    private static void cleanUpLogGroups(WireMockRuntimeInfo runtimeInfo, Instant queryTime) {
        CleanUpLogGroups.run(wiremockLogsClient(runtimeInfo), wiremockCloudFormationClient(runtimeInfo),
                queryTime, () -> {
                });
    }

    private static MappingBuilder listActiveStacksRequest() {
        return post("/")
                .withHeader(CONTENT_TYPE, equalTo("application/x-www-form-urlencoded; charset=UTF-8"))
                .withRequestBody(containing("Action=ListStacks")
                        .and(containing("StackStatusFilter.member.1=CREATE_COMPLETE" +
                                "&StackStatusFilter.member.2=UPDATE_COMPLETE")));
    }

    private static ResponseDefinitionBuilder aResponseWithStackName(String stackName) {
        return aResponse().withStatus(200)
                .withBody("<ListStacksResponse xmlns=\"http://cloudformation.amazonaws.com/doc/2010-05-15/\"><ListStacksResult><StackSummaries><member>" +
                        "<StackName>" + stackName + "</StackName>" +
                        "</member></StackSummaries></ListStacksResult></ListStacksResponse>");
    }

    private static ResponseDefinitionBuilder aResponseWithNestedStackName(String stackName) {
        return aResponse().withStatus(200)
                .withBody("<ListStacksResponse xmlns=\"http://cloudformation.amazonaws.com/doc/2010-05-15/\"><ListStacksResult><StackSummaries><member>" +
                        "<StackName>" + stackName + "</StackName>" +
                        "<ParentId>some-parent-stack</ParentId>" +
                        "</member></StackSummaries></ListStacksResult></ListStacksResponse>");
    }

    private static MappingBuilder describeLogGroupsRequest() {
        return post("/")
                .withHeader(CONTENT_TYPE, equalTo("application/x-amz-json-1.1"))
                .withHeader("X-Amz-Target", matching("^Logs_\\d+\\.DescribeLogGroups$"));
    }

    private static MappingBuilder deleteLogGroupRequestWithName(String logGroupName) {
        return deleteLogGroupRequestWithName(equalTo(logGroupName));
    }

    private static MappingBuilder deleteLogGroupRequestWithName(StringValuePattern pattern) {
        return post("/")
                .withHeader(CONTENT_TYPE, equalTo("application/x-amz-json-1.1"))
                .withHeader("X-Amz-Target", matching("^Logs_\\d+\\.DeleteLogGroup$"))
                .withRequestBody(matchingJsonPath("$.logGroupName", pattern));
    }

    private static RequestPatternBuilder deleteLogGroupRequestedWithName(String logGroupName) {
        return deleteLogGroupRequested()
                .withRequestBody(equalToJson("{\"logGroupName\": \"" + logGroupName + "\"}"));
    }

    private static RequestPatternBuilder deleteLogGroupRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(CONTENT_TYPE, equalTo("application/x-amz-json-1.1"))
                .withHeader("X-Amz-Target", matching("^Logs_\\d+\\.DeleteLogGroup$"));
    }
}
