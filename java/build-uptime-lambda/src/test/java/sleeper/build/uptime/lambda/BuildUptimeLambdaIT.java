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
package sleeper.build.uptime.lambda;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.build.uptime.lambda.WiremockTestHelper.wiremockEc2Client;

@WireMockTest
public class BuildUptimeLambdaIT {

    private final BuildUptimeEventSerDe serDe = new BuildUptimeEventSerDe();
    private BuildUptimeLambda lambda;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        lambda = new BuildUptimeLambda(wiremockEc2Client(runtimeInfo));
        stubFor(post("/").willReturn(aResponse().withStatus(200)));
    }

    @Test
    void shouldStartEc2() {
        // When
        handle(BuildUptimeEvent.startEc2sById("A", "B"));

        // Then
        verify(1, startRequestedForEc2Ids("A", "B"));
        verify(1, postRequestedFor(urlEqualTo("/")));
    }

    @Test
    void shouldFailWithUnrecognisedOperation() {
        // When / Then
        assertThatThrownBy(() -> handle(BuildUptimeEvent.operation("test")))
                .isInstanceOf(IllegalArgumentException.class);
        verify(0, postRequestedFor(urlEqualTo("/")));
    }

    void handle(BuildUptimeEvent event) {
        InputStream inputStream = new ByteArrayInputStream(serDe.toJson(event).getBytes());
        lambda.handleRequest(inputStream, null, null);
    }

    private RequestPatternBuilder startRequestedForEc2Ids(String... ec2Ids) {
        String instanceIds = IntStream.range(0, ec2Ids.length)
                .mapToObj(i -> "&InstanceId." + (i + 1) + "=" + ec2Ids[i])
                .collect(joining());
        return postRequestedFor(urlEqualTo("/"))
                .withRequestBody(matching("^Action=StartInstances&Version=[0-9\\-]+" + instanceIds + "$"));
    }
}
