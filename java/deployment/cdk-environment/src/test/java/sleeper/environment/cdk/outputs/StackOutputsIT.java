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

package sleeper.environment.cdk.outputs;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.environment.cdk.testutil.EnvironmentWiremockTestHelper.wiremockCloudFormationClient;
import static software.amazon.awssdk.http.Header.CONTENT_TYPE;

@WireMockTest
public class StackOutputsIT {

    @Test
    void shouldLoadOutputsFromCloudFormation(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeStacksRequestWithStackName("test-stack")
                .willReturn(aResponseWithStackNameAndOutputs("test-stack", "<Outputs>" +
                        "<member><OutputKey>test-output</OutputKey><OutputValue>test-value</OutputValue></member>" +
                        "</Outputs>")));

        assertThat(loadStackOutputs(runtimeInfo, "test-stack"))
                .isEqualTo(StackOutputs.fromMap(Map.of(
                        "test-stack", Map.of("test-output", "test-value"))));
    }

    @Test
    void shouldLoadOutputsForMultipleStacks(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeStacksRequestWithStackName("stack-1")
                .willReturn(aResponseWithStackNameAndOutputs("stack-1", "<Outputs>" +
                        "<member><OutputKey>some-output</OutputKey><OutputValue>stack-1-value</OutputValue></member>" +
                        "</Outputs>")));
        stubFor(describeStacksRequestWithStackName("stack-2")
                .willReturn(aResponseWithStackNameAndOutputs("stack-2", "<Outputs>" +
                        "<member><OutputKey>some-output</OutputKey><OutputValue>stack-2-value</OutputValue></member>" +
                        "</Outputs>")));

        assertThat(loadStackOutputs(runtimeInfo, "stack-1", "stack-2"))
                .isEqualTo(StackOutputs.fromMap(Map.of(
                        "stack-1", Map.of("some-output", "stack-1-value"),
                        "stack-2", Map.of("some-output", "stack-2-value"))));
    }

    @Test
    void shouldIgnoreStackWhichDoesNotExist(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeStacksRequestWithStackName("stack-1")
                .willReturn(aResponseWithStackNameAndOutputs("stack-1", "<Outputs>" +
                        "<member><OutputKey>some-output</OutputKey><OutputValue>stack-1-value</OutputValue></member>" +
                        "</Outputs>")));
        stubFor(describeStacksRequestWithStackName("stack-2")
                .willReturn(aResponseStatingStackDoesNotExist("stack-2")));

        assertThat(loadStackOutputs(runtimeInfo, "stack-1", "stack-2"))
                .isEqualTo(StackOutputs.fromMap(Map.of(
                        "stack-1", Map.of("some-output", "stack-1-value"))));
    }

    @Test
    void shouldFailWhenNotAuthed(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeStacksRequestWithStackName("test-stack")
                .willReturn(aResponseStatingTokenNotValid()));

        assertThatThrownBy(() -> loadStackOutputs(runtimeInfo, "test-stack"))
                .isInstanceOf(CloudFormationException.class);
    }

    private static StackOutputs loadStackOutputs(WireMockRuntimeInfo runtimeInfo, String... stackNames) {
        return StackOutputs.load(wiremockCloudFormationClient(runtimeInfo), List.of(stackNames));
    }

    private static MappingBuilder describeStacksRequestWithStackName(String stackName) {
        return post("/")
                .withHeader(CONTENT_TYPE, equalTo("application/x-www-form-urlencoded; charset=UTF-8"))
                .withRequestBody(containing("Action=DescribeStacks")
                        .and(containing("StackName=" + stackName)));
    }

    private static ResponseDefinitionBuilder aResponseWithStackNameAndOutputs(String stackName, String outputs) {
        return aResponse().withStatus(200)
                .withBody("<DescribeStacksResponse xmlns=\"http://cloudformation.amazonaws.com/doc/2010-05-15/\"><DescribeStacksResult><Stacks><member>" +
                        "<StackName>" + stackName + "</StackName>" +
                        outputs +
                        "</member></Stacks></DescribeStacksResult></DescribeStacksResponse>");
    }

    private static ResponseDefinitionBuilder aResponseStatingStackDoesNotExist(String stackName) {
        return aResponse().withStatus(400)
                .withBody("<ErrorResponse xmlns=\"http://cloudformation.amazonaws.com/doc/2010-05-15/\"><Error>" +
                        "<Type>Sender</Type>" +
                        "<Code>ValidationError</Code>" +
                        "<Message>Stack with id " + stackName + " does not exist</Message>" +
                        "</Error></ErrorResponse>");
    }

    private static ResponseDefinitionBuilder aResponseStatingTokenNotValid() {
        return aResponse().withStatus(403)
                .withBody("<ErrorResponse xmlns=\"http://cloudformation.amazonaws.com/doc/2010-05-15/\"><Error>" +
                        "<Type>Sender</Type>" +
                        "<Code>InvalidClientTokenId</Code>" +
                        "<Message>The security token included in the request is invalid.</Message>" +
                        "</Error></ErrorResponse>");
    }
}
