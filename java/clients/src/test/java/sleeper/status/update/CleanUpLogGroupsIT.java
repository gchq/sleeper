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
package sleeper.status.update;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static com.amazonaws.services.s3.Headers.CONTENT_TYPE;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientWiremockTestHelper.wiremockCloudFormationClient;
import static sleeper.ClientWiremockTestHelper.wiremockLogsClient;

@WireMockTest
class CleanUpLogGroupsIT {

    @Test
    void shouldDeleteAnEmptyLogGroupWhenNotInAStack(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(listActiveStacksRequest()
                .willReturn(aResponse().withStatus(200)));
        stubFor(describeLogGroupsRequest()
                .willReturn(aResponse().withStatus(200)));

        // When / Then
        assertThat(streamLogGroupNamesToDelete(runtimeInfo)).containsExactly();
    }

    private static Stream<String> streamLogGroupNamesToDelete(WireMockRuntimeInfo runtimeInfo) {
        return CleanUpLogGroups.streamLogGroupNamesToDelete(
                wiremockLogsClient(runtimeInfo),
                wiremockCloudFormationClient(runtimeInfo));
    }

    private static MappingBuilder listActiveStacksRequest() {
        return post("/")
                .withHeader(CONTENT_TYPE, equalTo("application/x-www-form-urlencoded; charset=UTF-8"))
                .withRequestBody(containing("Action=ListStacks")
                        .and(containing("StackStatusFilter.member.1=CREATE_COMPLETE" +
                                "&StackStatusFilter.member.2=UPDATE_COMPLETE")));
    }

    private static MappingBuilder describeLogGroupsRequest() {
        return post("/")
                .withHeader(CONTENT_TYPE, equalTo("application/x-amz-json-1.1"))
                .withHeader("X-Amz-Target", matching("Logs_\\d+\\.DescribeLogGroups"));
    }
}
