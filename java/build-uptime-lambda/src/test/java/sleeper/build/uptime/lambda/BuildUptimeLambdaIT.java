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
package sleeper.build.uptime.lambda;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.ec2.Ec2Client;

import sleeper.localstack.test.LocalStackTestBase;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.build.uptime.lambda.WiremockTestHelper.wiremockClient;

@WireMockTest
public class BuildUptimeLambdaIT extends LocalStackTestBase {

    private final BuildUptimeEventSerDe serDe = new BuildUptimeEventSerDe();
    private BuildUptimeLambda lambda;
    private final Queue<Instant> times = new LinkedList<>();

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        lambda = new BuildUptimeLambda(
                wiremockClient(runtimeInfo, Ec2Client.builder()),
                wiremockClient(runtimeInfo, CloudWatchEventsClient.builder()),
                s3Client, times::poll);
        stubFor(post("/").willReturn(aResponse().withStatus(200)));
    }

    @Test
    void shouldStartEc2s() {
        // When
        handle(BuildUptimeEvent.start().ec2Ids("A", "B").build());

        // Then
        verify(1, startRequestedForEc2Ids("A", "B"));
        verify(1, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldStopEc2s() {
        // When
        handle(BuildUptimeEvent.stop().ec2Ids("A", "B").build());

        // Then
        verify(1, stopRequestedForEc2Ids("A", "B"));
        verify(1, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldEnableCloudWatchRules() {
        // When
        handle(BuildUptimeEvent.start().rules("A", "B").build());

        // Then
        verify(1, enableRequestedForRuleName("A"));
        verify(1, enableRequestedForRuleName("B"));
        verify(2, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldDisableCloudWatchRules() {
        // When
        handle(BuildUptimeEvent.stop().rules("A", "B").build());

        // Then
        verify(1, disableRequestedForRuleName("A"));
        verify(1, disableRequestedForRuleName("B"));
        verify(2, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldFailWithUnrecognisedOperation() {
        // When / Then
        assertThatThrownBy(() -> handle(BuildUptimeEvent.operation("test").build()))
                .isInstanceOf(IllegalArgumentException.class);
        verify(0, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldDoNothingWhenConditionNotMet() {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        times.add(Instant.parse("2024-10-02T15:02:00Z"));

        // When
        handle(BuildUptimeEvent.stop()
                .ec2Ids("A", "B")
                .ifTestFinishedFromToday()
                .testBucket(bucketName)
                .build());

        // Then
        verify(0, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldPerformOperationWhenConditionIsMet() {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        putObject(bucketName, "summary.json", "{" +
                "\"executions\": [{" +
                "\"startTime\": \"2024-10-02T03:00:00Z\"" +
                "}]}");
        times.add(Instant.parse("2024-10-02T15:02:00Z"));

        // When
        handle(BuildUptimeEvent.stop()
                .ec2Ids("nightly-test-ec2")
                .ifTestFinishedFromToday()
                .testBucket(bucketName)
                .build());

        // Then
        verify(1, stopRequestedForEc2Ids("nightly-test-ec2"));
        verify(1, postRequestedFor(urlEqualTo("/")));
    }

    void handle(BuildUptimeEvent event) {
        InputStream inputStream = new ByteArrayInputStream(serDe.toJson(event).getBytes());
        lambda.handleRequest(inputStream, null, null);
    }

    private RequestPatternBuilder startRequestedForEc2Ids(String... ec2Ids) {
        return postRequestedFor(urlEqualTo("/"))
                .withRequestBody(matching("^Action=StartInstances&Version=[0-9\\-]+" + buildInstanceIdParams(ec2Ids) + "$"));
    }

    private RequestPatternBuilder stopRequestedForEc2Ids(String... ec2Ids) {
        return postRequestedFor(urlEqualTo("/"))
                .withRequestBody(matching("^Action=StopInstances&Version=[0-9\\-]+" + buildInstanceIdParams(ec2Ids) + "$"));
    }

    private RequestPatternBuilder enableRequestedForRuleName(String ruleName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("AWSEvents.EnableRule"))
                .withRequestBody(equalTo("{\"Name\":\"" + ruleName + "\"}"));
    }

    private RequestPatternBuilder disableRequestedForRuleName(String ruleName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("AWSEvents.DisableRule"))
                .withRequestBody(equalTo("{\"Name\":\"" + ruleName + "\"}"));
    }

    private String buildInstanceIdParams(String... instanceIds) {
        return IntStream.range(0, instanceIds.length)
                .mapToObj(i -> "&InstanceId." + (i + 1) + "=" + instanceIds[i])
                .collect(joining());
    }
}
